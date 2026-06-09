from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import os

COINS = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "XRPUSDT", "SOLUSDT",
    "DOGEUSDT", "AVAXUSDT", "LINKUSDT", "ADAUSDT", "LTCUSDT"
]

BINANCE_URL = "https://api.binance.com/api/v3/klines"
STAGING_PATH = "/tmp/staging"
LIMIT_PER_REQUEST = 1000

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

def fetch_backfill(**context):
    os.makedirs(STAGING_PATH, exist_ok=True)

    # Ambil parameter dari DAG run config
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=365)

    start_ms = int(start_time.timestamp() * 1000)
    end_ms = int(end_time.timestamp() * 1000)

    for symbol in COINS:
        all_data = []
        current_start = start_ms

        while current_start < end_ms:
            params = {
                "symbol": symbol,
                "interval": "1h",
                "startTime": current_start,
                "endTime": end_ms,
                "limit": LIMIT_PER_REQUEST
            }
            response = requests.get(BINANCE_URL, params=params)
            batch = response.json()

            if not batch:
                break

            all_data.extend(batch)
            # Geser start ke candle terakhir + 1 jam
            current_start = batch[-1][0] + 3600000

            print(f"{symbol}: fetched {len(all_data)} candles so far")

        timestamp_str = datetime.utcnow().strftime("%Y%m%d_%H%M")
        filename = f"{STAGING_PATH}/{symbol}backfill{timestamp_str}.json"
        with open(filename, "w") as f:
            json.dump(all_data, f)

        print(f"{symbol}: total {len(all_data)} candles saved")

with DAG(
    dag_id="dag_backfill",
    default_args=default_args,
    description="One-time backfill OHLCV 1 year from Binance API",
    schedule_interval=None,
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["binance", "backfill"],
) as dag:

    backfill_task = PythonOperator(
        task_id="fetch_backfill",
        python_callable=fetch_backfill,
    )