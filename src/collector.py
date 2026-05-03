# collector.py
import csv
import json
import os
import time
import zlib
from datetime import datetime

import msgpack
import paho.mqtt.client as mqtt
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("JCDECAUX_API_KEY")
JCDECAUX_CONTRACT = os.getenv("JCDECAUX_CONTRACT")
API_URL = "https://api.jcdecaux.com/vls/v3/stations"
MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
POLL_INTERVAL = 60  # secondes
CSV_PATH = os.getenv("CSV_PATH", "bandwidth.csv")

client = mqtt.Client()
client.connect(MQTT_BROKER, 1883)

previous_states = {}


def _compressed_size(payload: dict) -> int:
    minimal = {"id": payload["id"], "b": payload["bikes"], "s": payload["stands"]}
    return len(zlib.compress(msgpack.packb(minimal)))


def collect_and_publish():
    response = requests.get(
        API_URL, params={"contract": JCDECAUX_CONTRACT, "apiKey": API_KEY}
    )
    stations = response.json()
    timestamp = time.time()

    bytes_json = 0
    bytes_delta = 0
    bytes_compressed = 0
    bytes_delta_compressed = 0

    for station in stations:
        station_id = station["number"]
        payload = {
            "id": station_id,
            "bikes": station["totalStands"]["availabilities"]["bikes"],
            "stands": station["totalStands"]["availabilities"]["stands"],
            "timestamp": timestamp,
        }
        topic = f"velib/{JCDECAUX_CONTRACT}/station/{station_id}"
        raw = json.dumps(payload)
        msg_size = len(raw.encode())

        # Stratégie 1 : JSON brut — seul message réellement envoyé
        client.publish(topic, raw)
        bytes_json += msg_size

        # Stratégies 2, 3, 4 : calcul uniquement
        current_state = (payload["bikes"], payload["stands"])
        changed = previous_states.get(station_id) != current_state
        if changed:
            previous_states[station_id] = current_state

        # Stratégie 2 : Delta encoding
        if changed:
            bytes_delta += msg_size

        # Stratégie 3 : Compression msgpack+zlib
        bytes_compressed += _compressed_size(payload)

        # Stratégie 4 : Delta + Compression
        if changed:
            bytes_delta_compressed += _compressed_size(payload)

    dt = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
    write_header = not os.path.exists(CSV_PATH)
    with open(CSV_PATH, "a", newline="") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(
                ["timestamp", "json_bytes", "delta_bytes", "compressed_bytes", "delta_compressed_bytes"]
            )
        writer.writerow([dt, bytes_json, bytes_delta, bytes_compressed, bytes_delta_compressed])

    pct = lambda b: f"{b / 1024:.1f} KB ({b / bytes_json * 100:.0f}%)"
    print(
        f"[{dt}] JSON: {bytes_json / 1024:.1f} KB"
        f" | Delta: {pct(bytes_delta)}"
        f" | Compressé: {pct(bytes_compressed)}"
        f" | Delta+Compressé: {pct(bytes_delta_compressed)}"
    )


while True:
    collect_and_publish()
    time.sleep(POLL_INTERVAL)
