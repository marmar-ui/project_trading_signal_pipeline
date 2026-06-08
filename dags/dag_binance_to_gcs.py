from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import os

# Coins to track
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

def fetch_binance_data(**context):
    os.makedirs(STAGING_PATH, exist_ok=True)
    execution_time = context["execution_date"].strftime("%Y%m%d_%H%M")
    
    for symbol in COINS:
        params = {
            "symbol": symbol,
            "interval": "1h",
            "limit": 1
        }
        response = requests.get(BINANCE_URL, params=params)
        data = response.json()
        
        filename = f"{STAGING_PATH}/{symbol}_{execution_time}.json"
        with open(filename, "w") as f:
            json.dump(data, f)
        
        print(f"Fetched {symbol}: {data}")

with DAG(
    dag_id="dag_binance_to_gcs",
    default_args=default_args,
    description="Fetch OHLCV data from Binance API",
    schedule_interval="@hourly",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["binance", "ingestion"],
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_binance_data",
        python_callable=fetch_binance_data,
    )