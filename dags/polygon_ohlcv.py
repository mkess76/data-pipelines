from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import time
import psycopg2
from psycopg2.extras import execute_values

POLYGON_API_KEY = "VqDMSwWfyZvcGQV51pxkGjs4NzM1r0PD"
TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA"]

TIMESCALE_CONN = {
    "host": "timescaledb.data.svc.cluster.local",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "rancherlab"
}

def fetch_ticker(ticker, date_str, retries=3):
    """Fetch a single ticker with retry on rate limit."""
    for attempt in range(retries):
        resp = requests.get(
            f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{date_str}/{date_str}",
            params={"apiKey": POLYGON_API_KEY, "adjusted": "true"},
            timeout=30
        )
        if resp.status_code == 429:
            wait = 60 * (attempt + 1)
            print(f"Rate limited on {ticker}, waiting {wait}s (attempt {attempt + 1}/{retries})")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json()
    raise Exception(f"Rate limited on {ticker} after {retries} retries")

def fetch_and_store_ohlcv(**context):
    execution_date = context.get("data_interval_end") or context.get("logical_date") or datetime.utcnow()
    trade_date = execution_date - timedelta(days=2)
    date_str = trade_date.strftime("%Y-%m-%d")

    conn = psycopg2.connect(**TIMESCALE_CONN, sslmode="require")
    cur = conn.cursor()

    rows = []
    for i, ticker in enumerate(TICKERS):
        if i > 0:
            time.sleep(20)  # Polygon free tier: 5 calls/min

        data = fetch_ticker(ticker, date_str)

        if data.get("resultsCount", 0) == 0:
            print(f"No data for {ticker} on {date_str}")
            continue

        for result in data["results"]:
            rows.append((
                datetime.utcfromtimestamp(result["t"] / 1000),
                ticker,
                result["o"],
                result["h"],
                result["l"],
                result["c"],
                result["v"]
            ))

    if rows:
        execute_values(cur, """
            INSERT INTO stock_prices (time, ticker, open, high, low, close, volume)
            VALUES %s
            ON CONFLICT (time, ticker) DO UPDATE SET
                open   = EXCLUDED.open,
                high   = EXCLUDED.high,
                low    = EXCLUDED.low,
                close  = EXCLUDED.close,
                volume = EXCLUDED.volume
        """, rows)
        conn.commit()
        print(f"Inserted {len(rows)} rows for {date_str}")
    else:
        print(f"No rows to insert for {date_str}")

    cur.close()
    conn.close()

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=10)
}

with DAG(
    "polygon_ohlcv_ingest",
    start_date=datetime(2025, 1, 1),
    schedule="0 22 * * 1-5",
    catchup=False,
    default_args=default_args,
    tags=["stocks", "ingestion"]
) as dag:
    PythonOperator(
        task_id="fetch_and_store_ohlcv",
        python_callable=fetch_and_store_ohlcv
    )
