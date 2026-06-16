# Crypto Trading Signal Pipeline

Project ini dibangun untuk mengetahui coin mana yang paling efektif menggunakan indikator Golden Cross, serta pada kondisi Z-Score berapa sinyal tersebut menghasilkan akurasi tertinggi. Pipeline ini mengolah data OHLCV dari Binance secara otomatis melalui arsitektur medallion (Bronze → Silver → Gold) dan divisualisasikan dalam dashboard interaktif untuk mendukung pengambilan keputusan trading berbasis data.

---

## Architecture

```
        Binance API (OHLCV, 1h interval)
                       |
                       ▼
              Apache Airflow (Orchestration)
              GitHub Codespaces (Docker)
                       |
                       ▼
            Google Cloud Storage (GCS)
          Bronze Layer — Parquet, asia-southeast2
                       |
                       ▼
          BigQuery External Table (Bronze)
                       |
                       ▼
                 dbt (Transformation)
                Silver → Gold (Medallion)
                       |
                       ▼
              Looker Studio Dashboard
```

---

## Data Source

- **Binance API** — OHLCV candlestick data, 1-hour interval
- **Coins:** BTCUSDT, ETHUSDT, BNBUSDT, XRPUSDT, SOLUSDT, DOGEUSDT, AVAXUSDT, LINKUSDT, ADAUSDT, LTCUSDT

---

## Pipeline Components

### Airflow DAGs (`dags/`)

| DAG | Fungsi |
|-----|--------|
| `dag_backfill.py` | Backfill data historis |
| `dag_incremental.py` | Fetch data 1 jam terakhir (scheduled hourly) |
| `dag_daily_backup.py` | Backup data 24 jam terakhir ke GCS |

### dbt Models (`crypto_trading/models/`)

> Bronze layer disimpan di GCS sebagai Parquet dan di-load ke BigQuery sebagai external table. dbt transformation dimulai dari Silver.

#### Silver Layer
- `fact_ohlcv` — OHLCV dengan type casting dan deduplication

#### Gold Layer
- `gold_trading_signal` — tabel final dengan sinyal trading

---

## Gold Model Schema

| Kolom | Deskripsi |
|-------|-----------|
| `open_time`, `close_time` | Timestamp candle |
| `symbol` | Nama coin |
| `open`, `high`, `low`, `close`, `volume` | OHLCV |
| `ma7` | Moving Average 7 jam |
| `ma20` | Moving Average 20 jam |
| `z_score` | Z-Score harga terhadap MA20 |
| `golden_cross` | TRUE jika MA7 cross above MA20 |
| `death_cross` | TRUE jika MA7 cross below MA20 |
| `signal` | BUY / SELL / HOLD |
| `price_after_7h` | Harga close 7 jam setelah sinyal |
| `price_after_24h` | Harga close 24 jam setelah sinyal |
| `price_after_168h` | Harga close 168 jam setelah sinyal |
| `return_pct_7h` | Return % setelah 7 jam |
| `return_pct_24h` | Return % setelah 24 jam |
| `return_pct_168h` | Return % setelah 168 jam (7 hari) |
| `is_profit_7h/24h/168h` | Boolean profit flag |

---

## Research Questions

### RQ 1: Coin mana yang Golden Cross paling efektif?

Berdasarkan analisis winrate BUY signal (24h) terhadap 10 coin selama periode Juni 2025 - Juni 2026:

- Efektivitas Golden Cross per coin bersifat **time-dependent** — coin dengan winrate tertinggi dapat berubah tergantung kondisi market pada periode tertentu
- Secara keseluruhan, mayoritas coin mendekati coin-flip (48-55%)
- **Insight:** Golden Cross alone tidak cukup sebagai standalone signal — perlu dikombinasikan dengan indikator lain untuk meningkatkan akurasi

### RQ 2: Pada Z-Score berapa Golden Cross paling akurat?

Berdasarkan analisis win_rate_24h per z_score bucket selama periode Juni 2025 - Juni 2026:

- Akurasi Golden Cross juga bersifat **time-dependent** — hasil per bucket Z-Score dapat berbeda tergantung periode analisis
- Secara umum, Golden Cross yang terjadi saat Z-Score negatif (harga oversold) cenderung menghasilkan win rate lebih tinggi dibanding saat Z-Score positif (harga overbought)
- **Insight:** Kombinasi Golden Cross + Z-Score rendah adalah kondisi entry yang lebih optimal, namun tetap perlu divalidasi dengan data yang lebih panjang

---

## Dashboard

📊 [Crypto Trading Signal Dashboard (Looker Studio)](https://datastudio.google.com/u/0/reporting/6019b0d8-1af2-4486-8f9d-b3b5e3d4084f/page/P3K1F)

**Fitur dashboard:**
- KPI: Avg Close Price, Total BUY/SELL Signals, Win Rate BUY 24h
- Filter: Symbol dropdown, Date range
- Price & Moving Average chart (Close, MA7, MA20)
- Harga Wajar (MA20) trend
- Win Rate per Z-Score table
- Signal Performance per Coin table

---

## Tech Stack

| Layer | Tools |
|-------|-------|
| Orchestration | Apache Airflow 2.x |
| Infrastructure | GitHub Codespaces, Docker |
| Storage | Google Cloud Storage (Parquet) |
| Data Warehouse | BigQuery (asia-southeast2) |
| Transformation | dbt Core |
| Visualization | Looker Studio |
| Language | Python, SQL |

---

## Setup

### Prerequisites
- GitHub Codespaces
- GCP Project dengan BigQuery & GCS enabled
- Binance API key

### Environment Variables (Codespaces Secrets)
```
GCP_PROJECT_ID
GCS_BUCKET_NAME
BINANCE_API_KEY
BINANCE_API_SECRET
GOOGLE_APPLICATION_CREDENTIALS
```

### Run Pipeline
```bash
# Start Airflow
docker-compose up -d

# Trigger backfill
airflow dags trigger dag_backfill

# Run dbt
cd crypto_trading
dbt run
dbt test
```

---

## Project Structure

```
project_trading_signal_pipeline/
├── dags/
│   ├── dag_backfill.py
│   ├── dag_incremental.py
│   └── dag_daily_backup.py
├── crypto_trading/
│   ├── models/
│   │   ├── silver/
│   │   │   ├── fact_ohlcv.sql
│   │   │   └── sources.yml
│   │   └── gold/
│   │       └── gold_trading_signal.sql
│   └── dbt_project.yml
├── docker-compose.yml
└── README.md
```

---

## Author

**Eka Fajar Kharisma**  
