from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from google.cloud import storage
import requests
import json
import os
import pandas as pd

COINS = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "XRPUSDT", "SOLUSDT",
    "DOGEUSDT", "AVAXUSDT", "LINKUSDT", "ADAUSDT", "LTCUSDT"
]

BINANCE_URL = "https://api.binance.com/api/v3/klines"
LOCAL_TEMP_PATH = "/tmp/staging"
GCS_BUCKET = "crypto-trading-signal-pipeline"
GCS_STAGING_PATH = "staging/ohlcv"
GCS_BRONZE_PATH = "bronze/ohlcv"
CREDENTIALS_PATH = "/opt/airflow/config/gcp-credentials.json"

COLUMNS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_volume", "num_trades",
    "taker_buy_base", "taker_buy_quote", "ignore"
]

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}


def get_last_timestamp(bucket, symbol):
    """
    Cari open_time terbesar dari semua file Parquet
    milik symbol tertentu di GCS Bronze.
    Kalau tidak ada file sama sekali, return None.
    """
    blobs = list(bucket.list_blobs(prefix=f"{GCS_BRONZE_PATH}/"))
    symbol_blobs = [b for b in blobs if symbol in b.name and b.name.endswith(".parquet")]

    if not symbol_blobs:
        return None

    max_open_time = 0

    for blob in symbol_blobs:
        local_path = f"{LOCAL_TEMP_PATH}/_check_{symbol}.parquet"
        blob.download_to_filename(local_path)
        df = pd.read_parquet(local_path)
        os.remove(local_path)

        if "open_time" in df.columns and not df.empty:
            max_open_time = max(max_open_time, df["open_time"].max())

    return max_open_time if max_open_time > 0 else None


def fetch_incremental(**context):
    """
    Fetch OHLCV dari last checkpoint di Bronze sampai sekarang.
    Kalau Bronze kosong, fetch 2 hari terakhir sebagai safety net.
    Upload raw JSON ke GCS Staging.
    """
    os.makedirs(LOCAL_TEMP_PATH, exist_ok=True)

    client = storage.Client.from_service_account_json(CREDENTIALS_PATH)
    bucket = client.bucket(GCS_BUCKET)

    end_ms = int(datetime.utcnow().timestamp() * 1000)
    timestamp_str = datetime.utcnow().strftime("%Y%m%d_%H%M")

    for symbol in COINS:
        # Cari last timestamp di Bronze
        last_open_time = get_last_timestamp(bucket, symbol)

        if last_open_time:
            # Lanjut dari candle berikutnya setelah data terakhir
            start_ms = int(last_open_time) + 3600000
            print(f"{symbol}: resuming from {datetime.utcfromtimestamp(start_ms/1000)}")
        else:
            # Fallback: ambil 2 hari terakhir kalau Bronze kosong
            start_ms = int((datetime.utcnow() - timedelta(days=2)).timestamp() * 1000)
            print(f"{symbol}: no existing data, fetching last 2 days")

        if start_ms >= end_ms:
            print(f"{symbol}: already up to date, skipping")
            continue

        # Fetch semua candle dari start_ms sampai end_ms
        all_data = []
        current_start = start_ms

        while current_start < end_ms:
            params = {
                "symbol": symbol,
                "interval": "1h",
                "startTime": current_start,
                "endTime": end_ms,
                "limit": 1000
            }
            response = requests.get(BINANCE_URL, params=params)
            batch = response.json()

            if not batch:
                break

            all_data.extend(batch)
            current_start = batch[-1][0] + 3600000

        if not all_data:
            print(f"{symbol}: no new data to fetch")
            continue

        # Simpan ke local temp
        local_filename = f"{symbol}_incremental_{timestamp_str}.json"
        local_path = f"{LOCAL_TEMP_PATH}/{local_filename}"

        with open(local_path, "w") as f:
            json.dump(all_data, f)

        # Upload JSON ke GCS Staging
        gcs_path = f"{GCS_STAGING_PATH}/{local_filename}"
        blob = bucket.blob(gcs_path)
        blob.upload_from_filename(local_path)
        print(f"{symbol}: {len(all_data)} candles uploaded to staging")

        # Hapus local temp
        os.remove(local_path)


def upload_to_bronze(**context):
    """
    Baca raw JSON dari GCS Staging,
    convert ke Parquet, upload ke GCS Bronze,
    lalu hapus file staging.
    """
    client = storage.Client.from_service_account_json(CREDENTIALS_PATH)
    bucket = client.bucket(GCS_BUCKET)

    os.makedirs(LOCAL_TEMP_PATH, exist_ok=True)

    blobs = list(bucket.list_blobs(prefix=GCS_STAGING_PATH + "/"))

    if not blobs:
        print("No files found in staging. Skipping.")
        return

    for blob in blobs:
        filename = blob.name.split("/")[-1]
        if not filename.endswith(".json"):
            continue

        # Download dari GCS Staging ke local temp
        local_json_path = f"{LOCAL_TEMP_PATH}/{filename}"
        blob.download_to_filename(local_json_path)
        print(f"Downloaded {filename} from staging")

        # Convert JSON -> Parquet
        with open(local_json_path, "r") as f:
            data = json.load(f)

        df = pd.DataFrame(data, columns=COLUMNS)

        parquet_filename = filename.replace(".json", ".parquet")
        local_parquet_path = f"{LOCAL_TEMP_PATH}/{parquet_filename}"
        df.to_parquet(local_parquet_path, index=False)

        # Upload Parquet ke GCS Bronze
        gcs_bronze_path = f"{GCS_BRONZE_PATH}/{parquet_filename}"
        bronze_blob = bucket.blob(gcs_bronze_path)
        bronze_blob.upload_from_filename(local_parquet_path)
        print(f"Uploaded {parquet_filename} to gs://{GCS_BUCKET}/{gcs_bronze_path}")

        # Hapus file di GCS Staging
        blob.delete()
        print(f"Deleted staging file: {blob.name}")

        # Hapus local temp
        os.remove(local_json_path)
        os.remove(local_parquet_path)
        print(f"Cleaned up local temp: {filename}, {parquet_filename}")


with DAG(
    dag_id="dag_incremental",
    default_args=default_args,
    description="Incremental fetch OHLCV from Binance API with gap filling",
    schedule_interval="@hourly",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["binance", "incremental"],
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_incremental",
        python_callable=fetch_incremental,
    )

    upload_task = PythonOperator(
        task_id="upload_to_bronze",
        python_callable=upload_to_bronze,
    )

    fetch_task >> upload_task