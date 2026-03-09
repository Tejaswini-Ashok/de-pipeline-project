import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

# API endpoints to simulate
ENDPOINTS = [
    "/api/v1/users",
    "/api/v1/products", 
    "/api/v1/orders",
    "/api/v1/payments",
    "/api/v1/auth/login"
]

STATUS_CODES = [200, 200, 200, 201, 400, 401, 403, 404, 500]
METHODS = ["GET", "POST", "PUT", "DELETE"]
CLIENT_IPS = [f"192.168.1.{i}" for i in range(1, 20)]

def generate_log():
    return {
        "timestamp": datetime.utcnow().isoformat(),
        "endpoint": random.choice(ENDPOINTS),
        "method": random.choice(METHODS),
        "status_code": random.choice(STATUS_CODES),
        "response_time_ms": random.randint(10, 2000),
        "client_ip": random.choice(CLIENT_IPS),
        "request_id": f"req_{random.randint(10000, 99999)}"
    }

def main():
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    print("Starting API log producer...")
    count = 0
    while count < 100:
        log = generate_log()
        producer.send('api-logs', value=log)
        print(f"Sent log: {log['endpoint']} - {log['status_code']}")
        count += 1
        time.sleep(0.1)
    
    producer.flush()
    print(f"Done! Sent {count} logs")

if __name__ == "__main__":
    main()