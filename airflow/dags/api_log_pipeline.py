from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import random
import boto3
import os

# Default args
default_args = {
    'owner': 'tejaswini',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# AWS config — loaded from environment variables
BUCKET_NAME = 'de-pipeline-api-logs-tejj097'
REGION = 'eu-north-1'

def generate_logs(**context):
    """Simulate Kafka log ingestion — generate fake API logs"""
    ENDPOINTS = ["/api/v1/users", "/api/v1/products", "/api/v1/orders", "/api/v1/payments", "/api/v1/auth/login"]
    STATUS_CODES = [200, 200, 200, 201, 400, 401, 403, 404, 500]
    METHODS = ["GET", "POST", "PUT", "DELETE"]

    logs = []
    for i in range(50):
        log = {
            "timestamp": datetime.utcnow().isoformat(),
            "endpoint": random.choice(ENDPOINTS),
            "method": random.choice(METHODS),
            "status_code": random.choice(STATUS_CODES),
            "response_time_ms": random.randint(10, 2000),
            "client_ip": f"192.168.1.{random.randint(1, 20)}",
            "request_id": f"req_{random.randint(10000, 99999)}"
        }
        logs.append(log)

    s3 = boto3.client('s3', region_name=REGION)
    date_str = datetime.utcnow().strftime('%Y-%m-%d-%H-%M')
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=f'raw/logs_{date_str}.json',
        Body=json.dumps(logs)
    )
    print(f"Generated and saved {len(logs)} logs to S3")
    return f"raw/logs_{date_str}.json"

def process_logs(**context):
    """Process logs from S3"""
    s3 = boto3.client('s3', region_name=REGION)

    s3_key = context['task_instance'].xcom_pull(task_ids='generate_logs')
    response = s3.get_object(Bucket=BUCKET_NAME, Key=s3_key)
    logs = json.loads(response['Body'].read())

    endpoint_stats = {}
    for log in logs:
        ep = log['endpoint']
        if ep not in endpoint_stats:
            endpoint_stats[ep] = {'total': 0, 'errors': 0, 'total_response_time': 0}
        endpoint_stats[ep]['total'] += 1
        endpoint_stats[ep]['total_response_time'] += log['response_time_ms']
        if log['status_code'] >= 400:
            endpoint_stats[ep]['errors'] += 1

    results = []
    for ep, stats in endpoint_stats.items():
        results.append({
            'endpoint': ep,
            'total_requests': stats['total'],
            'error_count': stats['errors'],
            'error_rate': round(stats['errors'] / stats['total'] * 100, 2),
            'avg_response_time_ms': round(stats['total_response_time'] / stats['total'], 2)
        })

    date_str = datetime.utcnow().strftime('%Y-%m-%d-%H-%M')
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=f'processed/results_{date_str}.json',
        Body=json.dumps(results)
    )
    print(f"Processed {len(logs)} logs, results saved to S3")
    print(f"Results: {json.dumps(results, indent=2)}")

def check_alerts(**context):
    """Check for high error rates"""
    s3 = boto3.client('s3', region_name=REGION)

    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix='processed/')
    files = sorted([obj['Key'] for obj in response.get('Contents', [])], reverse=True)

    if not files:
        print("No processed files found")
        return

    latest = s3.get_object(Bucket=BUCKET_NAME, Key=files[0])
    results = json.loads(latest['Body'].read())

    alerts = []
    for r in results:
        if r['error_rate'] > 30:
            alerts.append(f"HIGH ERROR RATE: {r['endpoint']} has {r['error_rate']}% error rate")
        if r['avg_response_time_ms'] > 500:
            alerts.append(f"SLOW ENDPOINT: {r['endpoint']} avg response {r['avg_response_time_ms']}ms")

    if alerts:
        print("ALERTS TRIGGERED:")
        for alert in alerts:
            print(f"  - {alert}")
    else:
        print("All endpoints healthy")

# Define DAG
with DAG(
    'api_log_pipeline',
    default_args=default_args,
    description='Real-time API log processing pipeline',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    task_generate = PythonOperator(
        task_id='generate_logs',
        python_callable=generate_logs,
    )

    task_process = PythonOperator(
        task_id='process_logs',
        python_callable=process_logs,
    )

    task_alert = PythonOperator(
        task_id='check_alerts',
        python_callable=check_alerts,
    )

    task_generate >> task_process >> task_alert