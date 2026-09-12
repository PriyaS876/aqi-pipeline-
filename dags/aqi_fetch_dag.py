"""
aqi_fetch_dag.py

Airflow DAG jo har 5 minute mein OpenAQ se PM2.5 data fetch karke
Kafka topic mein bhejta hai (fetch_data.py ka wahi logic yahan direct call hota hai).
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import time
import os
from kafka import KafkaProducer

# ---------------------------------------------------------
# Configuration (fetch_data.py jaisa hi)
# ---------------------------------------------------------
API_KEY = os.environ.get("OPENAQ_API_KEY")
if not API_KEY:
    raise ValueError("OPENAQ_API_KEY environment variable set nahi hai.")
HEADERS = {"X-API-Key": API_KEY}

STATIONS = {
    17: "R K Puram, Delhi",
    50: "Punjabi Bagh, Delhi",
    235: "Anand Vihar, New Delhi",
    301: "Vikas Sadan, Gurugram",
    407: "Zoo Park, Hyderabad",
    860: "Sanjay Palace, Agra",
    2586: "Manali, Chennai",
    5598: "Sector-125, Noida"
}

# Note: Airflow container ke andar se Kafka tak pahunchne ke liye
# hume Docker network ka internal naam use karna hoga, localhost nahi.
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
TOPIC_NAME = "aqi-readings"


def find_pm25_sensor_ids(location_id):
    url = f"https://api.openaq.org/v3/locations/{location_id}"
    response = requests.get(url, headers=HEADERS)
    if response.status_code != 200:
        return []
    data = response.json()
    location = data['results'][0]
    return [s['id'] for s in location['sensors'] if s['parameter']['name'] == 'pm25']


def get_pm25_reading(location_id, station_name, sensor_ids):
    best_result = None
    for sensor_id in sensor_ids:
        url = f"https://api.openaq.org/v3/sensors/{sensor_id}/measurements?limit=1"
        response = requests.get(url, headers=HEADERS)
        if response.status_code != 200:
            continue
        data = response.json()
        if not data['results']:
            continue
        latest = data['results'][0]
        timestamp = latest['period']['datetimeFrom']['utc']
        if best_result is None or timestamp > best_result['timestamp_utc']:
            best_result = {
                "station": station_name,
                "location_id": location_id,
                "sensor_id": sensor_id,
                "parameter": "pm25",
                "value": latest['value'],
                "unit": "µg/m³",
                "timestamp_utc": timestamp,
                "fetched_at": datetime.now().isoformat()
            }
        time.sleep(0.3)
    return best_result


def fetch_and_send_to_kafka():
    """Yeh function Airflow task ke roop mein chalega"""
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    sent_count = 0
    for location_id, station_name in STATIONS.items():
        sensor_ids = find_pm25_sensor_ids(location_id)
        if not sensor_ids:
            print(f"No PM2.5 sensor found for {station_name}")
            continue

        time.sleep(0.5)
        result = get_pm25_reading(location_id, station_name, sensor_ids)
        if result:
            producer.send(TOPIC_NAME, value=result)
            sent_count += 1
            print(f"{station_name}: {result['value']} µg/m³ -> Kafka mein bheja")

    producer.flush()
    producer.close()
    print(f"\nTotal {sent_count} records Kafka mein bheje gaye.")


# ---------------------------------------------------------
# DAG definition
# ---------------------------------------------------------
default_args = {
    "owner": "aqi-pipeline",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="aqi_fetch_pipeline",
    default_args=default_args,
    description="Har 5 minute mein OpenAQ se PM2.5 data fetch karke Kafka mein bhejta hai",
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["aqi", "kafka"],
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_and_send_to_kafka",
        python_callable=fetch_and_send_to_kafka,
    )