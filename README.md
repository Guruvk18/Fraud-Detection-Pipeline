<div align="center">

<img src="https://capsule-render.vercel.app/api?type=waving&color=0:0a0a0f,50:1a1a2e,100:F2388F&height=200&section=header&text=Real-Time%20Fraud%20Detection%20Pipeline&fontSize=30&fontColor=e2e8f0&fontAlignY=35&desc=TensorFlow%20%2B%20FastAPI%20%2B%20GCP%20%2B%20Terraform&descAlignY=55&descSize=18&descColor=94a3b8"/>

<img src="https://readme-typing-svg.demolab.com?font=Fira+Code&size=20&duration=3000&pause=1000&color=F2388F&center=true&vCenter=true&width=700&lines=Transactions+%E2%86%92+Pub%2FSub+%E2%86%92+Dataflow+%E2%86%92+Deep+Learning;Real-Time+Fraud+Scoring+via+FastAPI;Airflow+DAG+%2B+BigQuery+Alerts;TensorFlow+%7C+GCP+%7C+Terraform+IaC" alt="Typing SVG" />

</div>

---

A real-time fraud detection pipeline built on **Google Cloud Platform**.
Financial transactions are streamed through Pub/Sub and Dataflow into BigQuery,
scored by a **TensorFlow deep learning model** served via **FastAPI**,
orchestrated by **Airflow DAGs** running every 15 minutes, with critical fraud
alerts surfaced in Looker Studio. All infrastructure provisioned with **Terraform**.

---

### Architecture

```
transactions.csv
        ↓
Pub/Sub (fraud-transactions topic)
        ↓
Dataflow (Apache Beam)
    ↙           ↘
GCS (raw)    BigQuery (fraud_raw.transactions)
                  ↓
        FastAPI (TensorFlow Model Scoring)
                  ↓
        BigQuery (fraud_results.predictions)
                  ↓
        Airflow DAG (every 15 mins)
                  ↓
        BigQuery Views
        ├── vw_fraud_summary      (by country + risk level)
        └── vw_critical_alerts    (critical fraud only)
                  ↓
        Looker Studio Dashboard
```

---

### Project Structure

```
fraud_detection_pipeline/
├── data/
│   └── transactions.csv              # sample transaction data
├── model/
│   └── train.py                      # TensorFlow deep learning model
├── api/
│   └── main.py                       # FastAPI real-time scoring
├── pipeline/
│   └── stream_pipeline.py            # Pub/Sub publisher + Dataflow
├── dags/
│   └── fraud_dag.py                  # Airflow DAG (every 15 mins)
├── terraform/
│   └── main.tf                       # GCP infra provisioning
└── requirements.txt
```

---

### Pipeline Stages

| Stage | Tool | What it does |
|-------|------|-------------|
| Ingestion | Pub/Sub | streams real-time transaction events |
| Stream Processing | Dataflow (Apache Beam) | routes to GCS + BigQuery |
| Model Training | TensorFlow + Keras | trains deep learning fraud classifier |
| Model Serving | FastAPI + Pydantic | real-time fraud scoring REST API |
| Orchestration | Airflow (every 15 mins) | schedules scoring + alert generation |
| Data Warehouse | BigQuery | stores raw + predictions + views |
| Alerts | BigQuery Views | critical fraud alerts surfaced |
| IaC | Terraform | provisions all GCP infrastructure |
| Visualization | Looker Studio | fraud dashboard by country + risk |

---

### Risk Levels

| Score | Risk Level |
|-------|-----------|
| >= 0.8 | 🔴 Critical |
| >= 0.6 | 🟠 High |
| >= 0.4 | 🟡 Medium |
| < 0.4  | 🟢 Low |

---

### Model Architecture

```
Input (4 features: amount, hour, txn_type, country)
        ↓
Dense(64) + BatchNorm + Dropout(0.3)
        ↓
Dense(32) + BatchNorm + Dropout(0.3)
        ↓
Dense(16)
        ↓
Dense(1) → Sigmoid → Fraud Score
```

---

### Tech Stack

<div align="center">

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![TensorFlow](https://img.shields.io/badge/TensorFlow-FF6F00?style=for-the-badge&logo=tensorflow&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-009688?style=for-the-badge&logo=fastapi&logoColor=white)
![Google Cloud](https://img.shields.io/badge/Google_Cloud-4285F4?style=for-the-badge&logo=google-cloud&logoColor=white)
![BigQuery](https://img.shields.io/badge/BigQuery-4285F4?style=for-the-badge&logo=google-cloud&logoColor=white)
![Apache Beam](https://img.shields.io/badge/Apache_Beam-E25A1C?style=for-the-badge&logo=apache&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-017CEE?style=for-the-badge&logo=apache-airflow&logoColor=white)
![Terraform](https://img.shields.io/badge/Terraform-7B42BC?style=for-the-badge&logo=terraform&logoColor=white)

</div>

---

### Setup

**1. Provision GCP infrastructure**
```bash
cd terraform
terraform init
terraform plan -var="project_id=<your-project-id>" -var="bucket_name=<your-bucket>"
terraform apply
```

**2. Install dependencies**
```bash
pip install -r requirements.txt
```

**3. Train deep learning model**
```bash
python model/train.py
```

**4. Start FastAPI server**
```bash
uvicorn api.main:app --reload --port 8000
```

**5. Run pipeline**
```bash
python pipeline/stream_pipeline.py
```

**6. Deploy Airflow DAG**
```bash
cp dags/fraud_dag.py gs://<your-bucket>/dags/
```

---

<div align="center">

<img src="https://capsule-render.vercel.app/api?type=waving&color=0:F2388F,50:1a1a2e,100:0a0a0f&height=100&section=footer"/>

*"Detect fraud in real time before it costs a dime."*

</div>

## Contributors

- [Mohammed Junaid Khan](https://github.com/mohammedjunaidkhan980)
- [Guruvk18](https://github.com/Guruvk18)
