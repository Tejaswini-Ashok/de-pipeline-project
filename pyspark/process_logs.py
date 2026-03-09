import json
import boto3
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, when, hour, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Sample logs to process (simulating what Kafka would send)
SAMPLE_LOGS = [
    {"timestamp": "2026-03-08T10:00:01", "endpoint": "/api/v1/users", "method": "GET", "status_code": 200, "response_time_ms": 150, "client_ip": "192.168.1.1", "request_id": "req_10001"},
    {"timestamp": "2026-03-08T10:00:02", "endpoint": "/api/v1/products", "method": "GET", "status_code": 200, "response_time_ms": 230, "client_ip": "192.168.1.2", "request_id": "req_10002"},
    {"timestamp": "2026-03-08T10:00:03", "endpoint": "/api/v1/orders", "method": "POST", "status_code": 201, "response_time_ms": 450, "client_ip": "192.168.1.3", "request_id": "req_10003"},
    {"timestamp": "2026-03-08T10:00:04", "endpoint": "/api/v1/payments", "method": "POST", "status_code": 500, "response_time_ms": 1200, "client_ip": "192.168.1.4", "request_id": "req_10004"},
    {"timestamp": "2026-03-08T10:00:05", "endpoint": "/api/v1/auth/login", "method": "POST", "status_code": 401, "response_time_ms": 80, "client_ip": "192.168.1.5", "request_id": "req_10005"},
    {"timestamp": "2026-03-08T10:00:06", "endpoint": "/api/v1/users", "method": "GET", "status_code": 200, "response_time_ms": 120, "client_ip": "192.168.1.6", "request_id": "req_10006"},
    {"timestamp": "2026-03-08T10:00:07", "endpoint": "/api/v1/products", "method": "PUT", "status_code": 404, "response_time_ms": 90, "client_ip": "192.168.1.7", "request_id": "req_10007"},
    {"timestamp": "2026-03-08T10:00:08", "endpoint": "/api/v1/orders", "method": "GET", "status_code": 200, "response_time_ms": 310, "client_ip": "192.168.1.8", "request_id": "req_10008"},
    {"timestamp": "2026-03-08T10:00:09", "endpoint": "/api/v1/payments", "method": "POST", "status_code": 200, "response_time_ms": 980, "client_ip": "192.168.1.9", "request_id": "req_10009"},
    {"timestamp": "2026-03-08T10:00:10", "endpoint": "/api/v1/auth/login", "method": "POST", "status_code": 403, "response_time_ms": 60, "client_ip": "192.168.1.10", "request_id": "req_10010"},
]

def create_spark_session():
    return SparkSession.builder \
        .appName("APILogProcessor") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

def process_logs(spark, logs):
    # Create DataFrame from logs
    df = spark.createDataFrame(logs)
    df = df.withColumn("timestamp", to_timestamp(col("timestamp")))

    print("\n--- Raw Logs ---")
    df.show(truncate=False)

    # 1. Error rate by endpoint
    print("\n--- Error Rate by Endpoint ---")
    error_df = df.withColumn(
        "is_error", when(col("status_code") >= 400, 1).otherwise(0)
    ).groupBy("endpoint").agg(
        count("*").alias("total_requests"),
        count(when(col("is_error") == 1, True)).alias("error_count"),
        avg("response_time_ms").alias("avg_response_time_ms")
    )
    error_df.show(truncate=False)

    # 2. Status code distribution
    print("\n--- Status Code Distribution ---")
    status_df = df.groupBy("status_code").count().orderBy("status_code")
    status_df.show()

    # 3. Slow requests (>500ms)
    print("\n--- Slow Requests (>500ms) ---")
    slow_df = df.filter(col("response_time_ms") > 500).select(
        "timestamp", "endpoint", "method", "status_code", "response_time_ms"
    )
    slow_df.show(truncate=False)

    return error_df, status_df, slow_df

def save_to_s3(df, bucket_name, path):
    output_path = f"s3a://{bucket_name}/{path}"
    df.coalesce(1).write.mode("overwrite").parquet(output_path)
    print(f"Saved to {output_path}")

def save_locally(df, path):
    os.makedirs(f"logs/{path}", exist_ok=True)
    df.coalesce(1).write.mode("overwrite").parquet(f"logs/{path}")
    print(f"Saved locally to logs/{path}")

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    print("Processing API logs with PySpark...")
    error_df, status_df, slow_df = process_logs(spark, SAMPLE_LOGS)

    # Save locally first
    save_locally(error_df, "error_rates")
    save_locally(status_df, "status_codes")
    save_locally(slow_df, "slow_requests")

    print("\nProcessing complete!")
    spark.stop()

if __name__ == "__main__":
    main()