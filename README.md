# Real-Time API Log Processing & Analytics Pipeline

## Architecture
```
Kafka Producer (Python)
        ↓
Raw Logs → AWS S3 (raw/)
        ↓
PySpark Processing (Google Colab)
        ↓
Processed Data → AWS S3 (processed/)
        ↓
Apache Airflow (Orchestration)
        ↓
SQL Analytics
        ↓
Alerts & Monitoring
```

## Tech Stack
- **Ingestion**: Apache Kafka (Python producer)
- **Processing**: Apache PySpark
- **Storage**: AWS S3
- **Orchestration**: Apache Airflow
- **Infrastructure**: Terraform
- **Analytics**: SQL
- **Language**: Python

## Project Structure
```
de-pipeline-project/
├── kafka/
│   └── log_producer.py        # Kafka producer — generates API logs
├── pyspark/
│   └── process_logs.py        # PySpark — processes logs, computes metrics
├── airflow/
│   └── dags/
│       └── api_log_pipeline.py # Airflow DAG — orchestrates pipeline
├── sql/
│   └── analytics.sql          # SQL queries for API analytics
└── terraform/
    └── main.tf                # AWS S3 infrastructure as code
```

## Pipeline Flow
1. **Kafka Producer** generates fake API logs (endpoint, status code, response time)
2. **PySpark** processes logs — computes error rates, slow requests, status distribution
3. **AWS S3** stores raw and processed data in organized folder structure
4. **Airflow DAG** orchestrates the pipeline — runs hourly automatically
5. **SQL queries** provide analytics on API reliability and performance
6. **Alerting** triggers when error rate > 30% or response time > 500ms

## Key Metrics Computed
- Error rate by endpoint
- Average response time by endpoint
- Status code distribution
- Slow requests (>500ms)
- Traffic by hour
- Top clients by error count

## Setup

### Prerequisites
- Python 3.11+
- AWS Account
- Terraform installed

### Configure AWS
```bash
aws configure
```

### Provision Infrastructure
```bash
cd terraform
terraform init
terraform apply
```

### Run Pipeline
```bash
# Generate logs
python kafka/log_producer.py

# Process with PySpark
python pyspark/process_logs.py

# Airflow DAG runs automatically every hour
```

## S3 Structure
```
de-pipeline-api-logs-tejj097/
├── raw/          # Raw logs from Kafka producer
├── processed/    # Processed results from PySpark
└── analytics/    # SQL analytics outputs
```
