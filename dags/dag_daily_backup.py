from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from google.cloud import storage
import requests
import pandas as pd
import os

COINS = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "XRPUSDT", "SOLUSDT",
    "DOGEUSDT", "AVAXUSDT", "LINKUSDT", "ADAUSDT", "LTCUSDT"
]

BINANCE_URL = "https://api.binance.com/api/v3/klines"
LOCAL_TEMP_PATH = "/tmp/daily_backup"
GCS_BUCKET = "crypto-trading-signal-pipeline"
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


def fetch_and_upload_daily_backup(**context):
    """
    Fetch OHLCV 24 jam terakhir dari Binance API,
    langsung convert ke Parquet dan upload ke GCS Bronze.
    Berfungsi sebagai safety net jika dag_incremental error.
    Dedup akan dilakukan di Silver layer (dbt).
    """
    os.makedirs(LOCAL_TEMP_PATH, exist_ok=True)

    client = storage.Client.from_service_account_json(CREDENTIALS_PATH)
    bucket = client.bucket(GCS_BUCKET)

    execution_time = context["execution_date"]
    end_ms = int(execution_time.timestamp() * 1000)
    start_ms = int((execution_time - timedelta(hours=24)).timestamp() * 1000)
    timestamp_str = execution_time.strftime("%Y%m%d_%H%M")

    for symbol in COINS:
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
            print(f"{symbol}: no data returned, skipping")
            continue

        # Convert langsung ke Parquet
        df = pd.DataFrame(all_data, columns=COLUMNS)
        parquet_filename = f"{symbol}_daily_backup_{timestamp_str}.parquet"
        local_path = f"{LOCAL_TEMP_PATH}/{parquet_filename}"
        df.to_parquet(local_path, index=False)

        # Upload langsung ke Bronze
        gcs_path = f"{GCS_BRONZE_PATH}/{parquet_filename}"
        blob = bucket.blob(gcs_path)
        blob.upload_from_filename(local_path)
        print(f"{symbol}: uploaded {parquet_filename} to Bronze ({len(all_data)} candles)")

        # Hapus local temp
        os.remove(local_path)


with DAG(
    dag_id="dag_daily_backup",
    default_args=default_args,
    description="Daily backup OHLCV 24 jam terakhir sebagai safety net",
    schedule_interval="@daily",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["binance", "backup"],
) as dag:

    fetch_upload_task = PythonOperator(
        task_id="fetch_and_upload_to_bronze",
        python_callable=fetch_and_upload_daily_backup,
    )