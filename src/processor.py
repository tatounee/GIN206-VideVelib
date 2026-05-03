# processor.py
import json
import os
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point

load_dotenv()

JCDECAUX_CONTRACT = os.getenv("JCDECAUX_CONTRACT")
MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://localhost:8086")
INFLUXDB_TOKEN = os.getenv("DOCKER_INFLUXDB_INIT_ADMIN_TOKEN", "mytoken")
CRITICAL_BIKES = 2  # station presque vide
CRITICAL_STANDS = 0  # station pleine

influx = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org="velib")
write_api = influx.write_api()


def on_message(client, userdata, msg):
    data = json.loads(msg.payload)

    # Écriture en base
    point = (
        Point("station_status")
        .tag("station_id", str(data["id"]))
        .field("available_bikes", data["bikes"])
        .field("available_stands", data["stands"])
        .time(datetime.now(timezone.utc))
    )
    write_api.write("velib", "velib", point)

    # Détection d'anomalie temps réel
    if data["bikes"] <= CRITICAL_BIKES:
        alert = {
            "station_id": data["id"],
            "level": "critical",
            "bikes_left": data["bikes"],
        }
        client.publish(f"velib/{JCDECAUX_CONTRACT}/alerts/critical", json.dumps(alert))


mqtt_client = mqtt.Client()
mqtt_client.on_message = on_message
mqtt_client.connect(MQTT_BROKER, 1883)
mqtt_client.subscribe(f"velib/{JCDECAUX_CONTRACT}/station/#")
mqtt_client.loop_forever()
