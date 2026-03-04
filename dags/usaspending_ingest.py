from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import psycopg2
from psycopg2.extras import execute_values

POSTGRES_CONN = {
    "host": "postgresql.data.svc.cluster.local",
    "port": 5432,
    "dbname": "contracting",
    "user": "rancher",
    "password": "rancherlab"
}

def fetch_and_store_awards(**context):
    execution_date = context["execution_date"]
    date_str = execution_date.strftime("%Y-%m-%d")
    prev_date_str = (execution_date - timedelta(days=1)).strftime("%Y-%m-%d")

    url = "https://api.usaspending.gov/api/v2/search/spending_by_award/"
    payload = {
        "filters": {
            "time_period": [{"start_date": prev_date_str, "end_date": date_str}],
            "award_type_codes": ["A", "B", "C", "D"]
        },
        "fields": [
            "Award ID", "Recipient Name", "recipient_uei",
            "Awarding Agency", "awarding_sub_agency",
            "Award Amount", "Award Type",
            "Start Date", "End Date",
            "Place of Performance State Code",
            "NAICS Code", "NAICS Description"
        ],
        "page": 1,
        "limit": 100,
        "sort": "Award Amount",
        "order": "desc"
    }

    conn = psycopg2.connect(**POSTGRES_CONN)
    cur = conn.cursor()
    rows = []
    page = 1
    total_inserted = 0

    while True:
        payload["page"] = page
        resp = requests.post(url, json=payload, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        results = data.get("results", [])

        if not results:
            break

        for r in results:
            rows.append((
                r.get("Award ID"),
                r.get("Start Date"),
                r.get("Awarding Agency"),
                None,
                r.get("Recipient Name"),
                r.get("recipient_uei"),
                r.get("NAICS Code"),
                r.get("NAICS Description"),
                r.get("Award Amount"),
                r.get("Start Date"),
                r.get("End Date"),
                r.get("Place of Performance State Code"),
                r.get("Award Type")
            ))

        if rows:
            execute_values(cur, """
                INSERT INTO raw.contract_awards (
                    award_id, award_date, agency_name, agency_id,
                    vendor_name, vendor_uei, naics_code, naics_description,
                    award_amount, period_of_perf_start, period_of_perf_end,
                    place_of_performance_state, award_type
                ) VALUES %s
                ON CONFLICT (award_id) DO UPDATE SET
                    award_amount = EXCLUDED.award_amount,
                    loaded_at = NOW()
            """, rows)
            conn.commit()
            total_inserted += len(rows)
            rows = []

        if not data.get("page_metadata", {}).get("hasNext", False):
            break
        page += 1

    print(f"Total awards inserted: {total_inserted}")
    cur.close()
    conn.close()

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=10)
}

with DAG(
    "usaspending_awards_ingest",
    start_date=datetime(2025, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    default_args=default_args,
    tags=["contracting", "ingestion"]
) as dag:
    PythonOperator(
        task_id="fetch_and_store_awards",
        python_callable=fetch_and_store_awards
    )
