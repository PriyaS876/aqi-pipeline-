import requests

API_KEY = "7e6cfad4f9237601fb9d0d88017f9bcdfdb4148085c6396f243a72da375a5d09"
HEADERS = {"X-API-Key": API_KEY}

url = "https://api.openaq.org/v3/locations?countries_id=9&parameters_id=2&limit=100"
response = requests.get(url, headers=HEADERS)
data = response.json()

print(f"Total stations found: {len(data['results'])}\n")

for loc in data['results']:
    last = loc.get('datetimeLast')
    if last:  # sirf wahi dikhao jinka recent data ho
        print(f"ID: {loc['id']} | Name: {loc['name']} | Last update: {last['local']}")