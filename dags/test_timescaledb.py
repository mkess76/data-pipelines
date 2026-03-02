from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2

def write_test_row():
    conn = psycopg2.connect(
        host="timescaledb.data.svc.cluster.local",
        port=5432,
        dbname="postgres",
        user="postgres",
        password="rancherlab"
    )
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS test_runs (
            id SERIAL PRIMARY KEY,
            run_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            message TEXT
        );
    """)
    cur.execute("INSERT INTO test_runs (message) VALUES (%s)", ("Day 8 gate test",))
    conn.commit()
    cur.close()
    conn.close()
    print("Row written to TimescaleDB successfully")

with DAG(
    dag_id="test_timescaledb_write",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test"],
) as dag:
    task = PythonOperator(
        task_id="write_row",
        python_callable=write_test_row,
    )
