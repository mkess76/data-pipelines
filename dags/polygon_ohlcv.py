from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import psycopg2
from psycopg2.extras import execute_values

POLYGON_API_KEY = "VqDMSwWfyZvcGQV51pxkGjs4NzM1r0PD"
TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA"]

TIMESCALE_CONN = {
    "host": "timescaledb.data.svc.cluster.local",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "postgres"
}

def fetch_and_store_ohlcv(**context):
    execution_date = context["execution_date"]
    date_str = execution_date.strftime("%Y-%m-%d")

    conn = psycopg2.connect(**TIMESCALE_CONN)
    cur = conn.cursor()

    rows = []
    for ticker in TICKERS:
        url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{date_str}/{date_str}"
        params = {"apiKey": POLYGON_API_KEY, "adjusted": "true"}
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

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

    cur.close()
    conn.close()

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5)
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
