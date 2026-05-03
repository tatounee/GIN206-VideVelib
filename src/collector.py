# collector.py
import json
import os
import time

import paho.mqtt.client as mqtt
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("JCDECAUX_API_KEY")
JCDECAUX_CONTRACT = os.getenv("JCDECAUX_CONTRACT")
API_URL = "https://api.jcdecaux.com/vls/v3/stations"
MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
POLL_INTERVAL = 60  # secondes

client = mqtt.Client()
client.connect(MQTT_BROKER, 1883)

previous_states = {}


def collect_and_publish():
    response = requests.get(
        API_URL, params={"contract": JCDECAUX_CONTRACT, "apiKey": API_KEY}
    )
    stations = response.json()

    timestamp = time.time()

    for station in stations:
        station_id = station["number"]
        payload = {
            "id": station_id,
            "bikes": station["totalStands"]["availabilities"]["bikes"],
            "stands": station["totalStands"]["availabilities"]["stands"],
            "timestamp": timestamp,
        }
        topic = f"velib/{JCDECAUX_CONTRACT}/station/{station_id}"
        client.publish(topic, json.dumps(payload))

    print(
        f"Published data for {len(stations)} stations at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp))}",
    )


while True:
    collect_and_publish()
    time.sleep(POLL_INTERVAL)
