import requests
import json
import time
import os
from datetime import datetime
from kafka import KafkaProducer

API_KEY = os.environ.get("OPENAQ_API_KEY")
if not API_KEY:
    raise ValueError(
        "OPENAQ_API_KEY environment variable set nahi hai. "
        "Terminal mein set karo: set OPENAQ_API_KEY=your_key_here (Windows) "
        "ya ek .env file banao."
    )
HEADERS = {"X-API-Key": API_KEY}

# Kafka producer setup — localhost:9092 pe Docker wala Kafka chal raha hai
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)
TOPIC_NAME = "aqi-readings"

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


def find_pm25_sensor_ids(location_id):
    """Location ke andar SAARE PM2.5 sensors ke IDs dhundta hai (kabhi 1+ hote hain)"""
    url = f"https://api.openaq.org/v3/locations/{location_id}"
    response = requests.get(url, headers=HEADERS)

    if response.status_code != 200:
        return []

    data = response.json()
    location = data['results'][0]

    sensor_ids = []
    for sensor in location['sensors']:
        if sensor['parameter']['name'] == 'pm25':
            sensor_ids.append(sensor['id'])

    return sensor_ids


def get_pm25_reading(location_id, station_name, sensor_ids):
    """Saare PM2.5 sensors try karta hai, jiska data sabse recent ho wahi return karta hai"""
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


def fetch_all_stations():
    all_data = []
    for location_id, station_name in STATIONS.items():
        print(f"Processing {station_name}...")

        sensor_ids = find_pm25_sensor_ids(location_id)
        if not sensor_ids:
            print(f"  No PM2.5 sensor found for {station_name}")
            continue

        time.sleep(0.5)

        result = get_pm25_reading(location_id, station_name, sensor_ids)
        if result:
            all_data.append(result)
            print(f"  PM2.5 = {result['value']} µg/m³ (as of {result['timestamp_utc']})")

            # Kafka topic mein bhejo
            producer.send(TOPIC_NAME, value=result)
            print(f"  -> Kafka topic '{TOPIC_NAME}' mein bhej diya")
        else:
            print(f"  No recent reading found for {station_name}")

    producer.flush()  # sunishchit karo ki sab messages bhej diye gaye
    return all_data


def run_once():
    """Ek baar sab stations ka data fetch karke Kafka mein bhejta hai"""
    all_data = fetch_all_stations()
    print("\n--- SUMMARY ---")
    for d in all_data:
        print(f"{d['station']}: {d['value']} µg/m³")
    return all_data


def run_continuous(interval_minutes=5):
    """Har interval_minutes mein ek baar data fetch karke Kafka mein bhejta hai, hamesha ke liye"""
    print(f"Continuous mode shuru — har {interval_minutes} minute mein data fetch hoga.")
    print("Rokne ke liye Ctrl+C dabao.\n")

    while True:
        print(f"\n{'='*50}")
        print(f"Fetch shuru: {datetime.now().isoformat()}")
        print(f"{'='*50}")

        try:
            run_once()
        except Exception as e:
            print(f"Error aaya, lekin loop chalta rahega: {e}")

        print(f"\nAgla fetch {interval_minutes} minute baad hoga...")
        time.sleep(interval_minutes * 60)


if __name__ == "__main__":
    run_continuous(interval_minutes=5)