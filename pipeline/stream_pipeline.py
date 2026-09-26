import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from google.cloud import pubsub_v1
import json
import csv
import os

# ─── GCP CONFIG ───────────────────────────────────────────────
GCP_PROJECT_ID      = "# here your GCP project id"
PUBSUB_TOPIC        = "projects/# here your GCP project id/topics/fraud-transactions"
GCS_BUCKET          = "# here your GCS bucket name"
BQ_DATASET          = "fraud_raw"
BQ_TABLE            = "transactions"
GCP_REGION          = "us-central1"
SERVICE_ACCOUNT_KEY = "# here your GCP service account json path"
# ──────────────────────────────────────────────────────────────

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_KEY

BQ_SCHEMA = {
    "fields": [
        {"name": "txn_id",      "type": "STRING"},
        {"name": "account_id",  "type": "STRING"},
        {"name": "customer_id", "type": "STRING"},
        {"name": "amount",      "type": "FLOAT"},
        {"name": "txn_type",    "type": "STRING"},
        {"name": "merchant",    "type": "STRING"},
        {"name": "city",        "type": "STRING"},
        {"name": "country",     "type": "STRING"},
        {"name": "hour",        "type": "INTEGER"},
        {"name": "is_fraud",    "type": "INTEGER"},
    ]
}


# ── Pub/Sub Publisher ─────────────────────────────────────────
def publish_transactions(csv_path: str):
    publisher  = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(GCP_PROJECT_ID, "fraud-transactions")

    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            message = json.dumps(row).encode("utf-8")
            publisher.publish(topic_path, message)
            print(f"Published: {row['txn_id']} | Amount: {row['amount']} | Fraud: {row['is_fraud']}")

    print("All transactions published to Pub/Sub")


# ── Dataflow Pipeline ─────────────────────────────────────────
class ParseTransaction(beam.DoFn):
    def process(self, element):
        record = json.loads(element.decode("utf-8"))
        yield {
            "txn_id":      record.get("txn_id"),
            "account_id":  record.get("account_id"),
            "customer_id": record.get("customer_id"),
            "amount":      float(record.get("amount", 0)),
            "txn_type":    record.get("txn_type"),
            "merchant":    record.get("merchant"),
            "city":        record.get("city"),
            "country":     record.get("country"),
            "hour":        int(record.get("hour", 0)),
            "is_fraud":    int(record.get("is_fraud", 0)),
        }


def run_dataflow():
    options = PipelineOptions(
        project=GCP_PROJECT_ID,
        region=GCP_REGION,
        temp_location=f"gs://{GCS_BUCKET}/temp",
        staging_location=f"gs://{GCS_BUCKET}/staging",
        runner="DataflowRunner",
        job_name="fraud-stream-pipeline",
    )
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        events = (
            p
            | "Read Pub/Sub"       >> beam.io.ReadFromPubSub(topic=PUBSUB_TOPIC)
            | "Parse Transactions" >> beam.ParDo(ParseTransaction())
        )

        events | "Write GCS" >> beam.io.WriteToText(
            f"gs://{GCS_BUCKET}/raw/transactions", file_name_suffix=".json"
        )

        events | "Write BigQuery" >> beam.io.WriteToBigQuery(
            table=f"{GCP_PROJECT_ID}:{BQ_DATASET}.{BQ_TABLE}",
            schema=BQ_SCHEMA,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        )


if __name__ == "__main__":
    publish_transactions("../data/transactions.csv")
    run_dataflow()
