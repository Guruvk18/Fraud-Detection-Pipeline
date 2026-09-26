from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowStartPythonJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime, timedelta
import requests
import pandas as pd

# ─── GCP CONFIG ───────────────────────────────────────────────
GCP_PROJECT_ID = "# here your GCP project id"
GCS_BUCKET     = "# here your GCS bucket name"
GCP_REGION     = "us-central1"
FASTAPI_URL    = "# here your FastAPI service URL"
# ──────────────────────────────────────────────────────────────

default_args = {
    "owner":            "junaid",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}


def score_transactions():
    df  = pd.read_csv("/opt/airflow/data/transactions.csv")
    url = f"{FASTAPI_URL}/predict/batch"
    transactions = df[["txn_id", "account_id", "customer_id", "amount",
                        "txn_type", "merchant", "city", "country", "hour"]].to_dict(orient="records")
    response = requests.post(url, json=transactions)
    response.raise_for_status()
    results = response.json()
    fraud_count = sum(1 for r in results if r["is_fraud"])
    print(f"Scored {len(results)} transactions | Fraud detected: {fraud_count}")


with DAG(
    dag_id="fraud_detection_pipeline",
    default_args=default_args,
    description="Real-Time Fraud Detection Pipeline — Transactions → Pub/Sub → Dataflow → Deep Learning → BigQuery",
    schedule_interval="*/15 * * * *",  # runs every 15 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["fraud", "deep-learning", "gcp", "tensorflow"],
) as dag:

    # ── Task 1: Start Dataflow streaming pipeline ─────────────
    start_dataflow = DataflowStartPythonJobOperator(
        task_id="start_dataflow_stream",
        py_file=f"gs://{GCS_BUCKET}/scripts/stream_pipeline.py",
        job_name="fraud-stream-pipeline",
        project_id=GCP_PROJECT_ID,
        location=GCP_REGION,
        options={
            "project":          GCP_PROJECT_ID,
            "region":           GCP_REGION,
            "temp_location":    f"gs://{GCS_BUCKET}/temp",
            "staging_location": f"gs://{GCS_BUCKET}/staging",
            "streaming":        True,
        },
    )

    # ── Task 2: Score transactions via FastAPI ────────────────
    score_txns = PythonOperator(
        task_id="score_transactions_fastapi",
        python_callable=score_transactions,
    )

    # ── Task 3: Create fraud summary view in BigQuery ─────────
    create_fraud_view = BigQueryInsertJobOperator(
        task_id="create_fraud_summary_view",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE VIEW `{GCP_PROJECT_ID}.fraud_results.vw_fraud_summary` AS
                    SELECT
                        country,
                        risk_level,
                        COUNT(*)                                         AS total_transactions,
                        SUM(CAST(is_fraud AS INT64))                     AS fraud_count,
                        ROUND(AVG(fraud_score), 4)                       AS avg_fraud_score,
                        ROUND(SUM(CAST(is_fraud AS INT64)) * 100.0
                            / COUNT(*), 2)                               AS fraud_rate_pct
                    FROM `{GCP_PROJECT_ID}.fraud_results.predictions`
                    GROUP BY country, risk_level
                    ORDER BY fraud_rate_pct DESC
                """,
                "useLegacySql": False,
            }
        },
    )

    # ── Task 4: Alert on critical fraud ──────────────────────
    create_alert_view = BigQueryInsertJobOperator(
        task_id="create_critical_fraud_alerts",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE VIEW `{GCP_PROJECT_ID}.fraud_results.vw_critical_alerts` AS
                    SELECT
                        txn_id,
                        account_id,
                        customer_id,
                        amount,
                        country,
                        fraud_score,
                        predicted_at
                    FROM `{GCP_PROJECT_ID}.fraud_results.predictions`
                    WHERE risk_level = 'critical'
                    ORDER BY predicted_at DESC
                """,
                "useLegacySql": False,
            }
        },
    )

    # ── DAG Flow ──────────────────────────────────────────────
    start_dataflow >> score_txns >> create_fraud_view >> create_alert_view
