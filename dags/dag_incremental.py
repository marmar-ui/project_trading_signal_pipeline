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

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

def fetch_incremental(**context):
    os.makedirs(STAGING_PATH, exist_ok=True)
    
    execution_time = context["execution_date"]
    start_time = int((execution_time - timedelta(hours=1)).timestamp() * 1000)
    end_time = int(execution_time.timestamp() * 1000)
    timestamp_str = execution_time.strftime("%Y%m%d_%H%M")

    for symbol in COINS:
        params = {
            "symbol": symbol,
            "interval": "1h",
            "startTime": start_time,
            "endTime": end_time,
            "limit": 1
        }
        response = requests.get(BINANCE_URL, params=params)
        data = response.json()

        filename = f"{STAGING_PATH}/{symbol}_{timestamp_str}.json"
        with open(filename, "w") as f:
            json.dump(data, f)

        print(f"Fetched {symbol}: {data}")

with DAG(
    dag_id="dag_incremental",
    default_args=default_args,
    description="Incremental fetch OHLCV from Binance API (1 candle per hour)",
    schedule_interval="@hourly",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["binance", "incremental"],
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_incremental",
        python_callable=fetch_incremental,
    )