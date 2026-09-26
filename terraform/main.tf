terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ── Variables ─────────────────────────────────────────────────
variable "project_id" {
  description = "# here your GCP project id"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "us-central1"
}

variable "bucket_name" {
  description = "# here your GCS bucket name"
  type        = string
}

# ── GCS Bucket ────────────────────────────────────────────────
resource "google_storage_bucket" "fraud_bucket" {
  name          = var.bucket_name
  location      = "US"
  force_destroy = true

  lifecycle_rule {
    condition { age = 30 }
    action    { type = "Delete" }
  }
}

# ── Pub/Sub Topic + Subscription ─────────────────────────────
resource "google_pubsub_topic" "fraud_topic" {
  name = "fraud-transactions"
}

resource "google_pubsub_subscription" "fraud_sub" {
  name  = "fraud-transactions-sub"
  topic = google_pubsub_topic.fraud_topic.name
  ack_deadline_seconds = 30
}

# ── BigQuery Datasets ─────────────────────────────────────────
resource "google_bigquery_dataset" "fraud_raw" {
  dataset_id = "fraud_raw"
  location   = "US"
}

resource "google_bigquery_dataset" "fraud_results" {
  dataset_id = "fraud_results"
  location   = "US"
}

# ── BigQuery Tables ───────────────────────────────────────────
resource "google_bigquery_table" "transactions_raw" {
  dataset_id          = google_bigquery_dataset.fraud_raw.dataset_id
  table_id            = "transactions"
  deletion_protection = false

  schema = jsonencode([
    { name = "txn_id",      type = "STRING"  },
    { name = "account_id",  type = "STRING"  },
    { name = "customer_id", type = "STRING"  },
    { name = "amount",      type = "FLOAT"   },
    { name = "txn_type",    type = "STRING"  },
    { name = "merchant",    type = "STRING"  },
    { name = "city",        type = "STRING"  },
    { name = "country",     type = "STRING"  },
    { name = "hour",        type = "INTEGER" },
    { name = "is_fraud",    type = "INTEGER" },
  ])
}

resource "google_bigquery_table" "fraud_predictions" {
  dataset_id          = google_bigquery_dataset.fraud_results.dataset_id
  table_id            = "predictions"
  deletion_protection = false

  schema = jsonencode([
    { name = "txn_id",       type = "STRING"    },
    { name = "account_id",   type = "STRING"    },
    { name = "customer_id",  type = "STRING"    },
    { name = "amount",       type = "FLOAT"     },
    { name = "txn_type",     type = "STRING"    },
    { name = "merchant",     type = "STRING"    },
    { name = "city",         type = "STRING"    },
    { name = "country",      type = "STRING"    },
    { name = "hour",         type = "INTEGER"   },
    { name = "is_fraud",     type = "BOOL"      },
    { name = "fraud_score",  type = "FLOAT"     },
    { name = "risk_level",   type = "STRING"    },
    { name = "predicted_at", type = "TIMESTAMP" },
  ])
}

# ── Outputs ───────────────────────────────────────────────────
output "bucket_name" {
  value = google_storage_bucket.fraud_bucket.name
}

output "pubsub_topic" {
  value = google_pubsub_topic.fraud_topic.name
}
