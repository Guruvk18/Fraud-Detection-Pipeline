from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from google.cloud import bigquery
import tensorflow as tf
import numpy as np
import joblib
import os
from datetime import datetime

# ─── GCP CONFIG ───────────────────────────────────────────────
GCP_PROJECT_ID      = "# here your GCP project id"
BQ_DATASET          = "fraud_results"
BQ_TABLE            = "predictions"
SERVICE_ACCOUNT_KEY = "# here your GCP service account json path"
MODEL_PATH          = "../model/fraud_model"
SCALER_PATH         = "../model/scaler.pkl"
LE_TXN_PATH         = "../model/le_txn.pkl"
LE_COUNTRY_PATH     = "../model/le_country.pkl"
FRAUD_THRESHOLD     = 0.5
# ──────────────────────────────────────────────────────────────

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_KEY

app     = FastAPI(title="Real-Time Fraud Detection API", version="1.0.0")
model   = tf.keras.models.load_model(MODEL_PATH)
scaler  = joblib.load(SCALER_PATH)
le_txn  = joblib.load(LE_TXN_PATH)
le_country = joblib.load(LE_COUNTRY_PATH)


class TransactionRequest(BaseModel):
    txn_id:      str
    account_id:  str
    customer_id: str
    amount:      float
    txn_type:    str
    merchant:    str
    city:        str
    country:     str
    hour:        int


class FraudResponse(BaseModel):
    txn_id:        str
    is_fraud:      bool
    fraud_score:   float
    risk_level:    str
    predicted_at:  str


def get_risk_level(score: float) -> str:
    if score >= 0.8:   return "critical"
    elif score >= 0.6: return "high"
    elif score >= 0.4: return "medium"
    else:              return "low"


def save_to_bigquery(txn: TransactionRequest, is_fraud: bool, score: float, risk: str):
    client   = bigquery.Client(project=GCP_PROJECT_ID)
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
    rows = [{
        "txn_id":       txn.txn_id,
        "account_id":   txn.account_id,
        "customer_id":  txn.customer_id,
        "amount":       txn.amount,
        "txn_type":     txn.txn_type,
        "merchant":     txn.merchant,
        "city":         txn.city,
        "country":      txn.country,
        "hour":         txn.hour,
        "is_fraud":     is_fraud,
        "fraud_score":  score,
        "risk_level":   risk,
        "predicted_at": datetime.utcnow().isoformat(),
    }]
    client.insert_rows_json(table_id, rows)


@app.get("/health")
def health():
    return {"status": "ok", "model": "fraud-detection-deep-learning"}


@app.post("/predict", response_model=FraudResponse)
def predict(txn: TransactionRequest):
    try:
        txn_type_enc = le_txn.transform([txn.txn_type])[0]
        country_enc  = le_country.transform([txn.country])[0]

        features = np.array([[txn.amount, txn.hour, txn_type_enc, country_enc]])
        features = scaler.transform(features)

        score    = float(model.predict(features)[0][0])
        is_fraud = score >= FRAUD_THRESHOLD
        risk     = get_risk_level(score)

        save_to_bigquery(txn, is_fraud, round(score, 4), risk)

        return FraudResponse(
            txn_id=txn.txn_id,
            is_fraud=is_fraud,
            fraud_score=round(score, 4),
            risk_level=risk,
            predicted_at=datetime.utcnow().isoformat(),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/predict/batch")
def predict_batch(transactions: list[TransactionRequest]):
    return [predict(t) for t in transactions]
