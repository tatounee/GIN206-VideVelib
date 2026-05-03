# Projet IoT - Optimisation du réseau Vélib en temps réel

**Durée estimée :** 2-3 jours | **Niveau :** Intermédiaire | **Matériel requis :** Aucun

---

## 1. Contexte et objectifs

### Problématique

Les systèmes IoT à grande échelle font face à des tensions fondamentales : comment collecter, transmettre et stocker des données de milliers de capteurs en temps réel sans saturer le réseau, épuiser les ressources ou perdre de l'information critique ?

Le réseau Vélib parisien constitue un cas d'étude parfait : **~1400 stations** remontent en permanence leur état via une API publique, ce qui permet de travailler sur des données IoT réelles sans aucun matériel.

### Objectifs pédagogiques

- Construire un pipeline IoT complet de bout en bout
- Observer et quantifier les tensions IoT (bande passante, stockage, latence)
- Comparer des stratégies d'optimisation concrètes
- Visualiser des données temps réel sur un dashboard

### Ce que le projet n'est PAS

- Un projet de data science / prédiction (même si c'est possible en extension)
- Un projet de développement web classique
- Une simulation : **les données sont réelles**

---

## 2. Données utilisées

### Source

**API JCDecaux** — accès gratuit, inscription sur https://developer.jcdecaux.com

```
GET https://api.jcdecaux.com/vls/v1/stations?contract=paris&apiKey={YOUR_KEY}
```

### Exemple de payload reçu

```json
{
  "number": 14111,
  "name": "14111 - DAGUERRE GASSENDI",
  "position": { "lat": 48.833, "lng": 2.323 },
  "available_bikes": 3,
  "available_bike_stands": 17,
  "total_stands": 20,
  "status": "OPEN",
  "last_update": 1701234567000
}
```

### Volume brut généré

```
1400 stations × 500 bytes × 1440 requêtes/jour ≈ 1 GB/jour
1400 stations × 1440 minutes/jour             = 2 016 000 points/jour
                                              = ~735 millions points/an
```

Ces chiffres sont le point de départ de toutes les tensions du projet.

---

## 3. Architecture globale

```
[API JCDecaux]
      │
      ▼
[Collecteur Python]  ←─ simule un gateway IoT
      │  publie sur topics MQTT
      ▼
[Broker MQTT - Mosquitto]
      │  souscription
      ▼
[Processeur Python]  ←─ détection d'anomalies, alertes
      │
      ├──► [InfluxDB]  ←─ stockage time-series
      │
      └──► [MQTT topic alertes]
                │
                ▼
          [Grafana Dashboard]
```

### Pourquoi MQTT et pas HTTP direct ?

| Critère         | HTTP polling         | MQTT           |
| --------------- | -------------------- | -------------- |
| Overhead réseau | Élevé (headers HTTP) | Minimal        |
| Modèle          | Pull (on demande)    | Push/Subscribe |
| Bande passante  | +++                  | +              |
| Adapté IoT      | Moyen                | ✅ Natif       |
| Scalabilité     | Faible               | Élevée         |

MQTT est le protocole standard de l'IoT : c'est ce qu'utilisent les vrais capteurs physiques pour remonter leurs données.

---

## 4. Stack technique

### Installation via Docker

```yaml
# docker-compose.yml
version: "3"
services:
  mosquitto:
    image: eclipse-mosquitto
    ports: ["1883:1883"]
    volumes:
      - ./mosquitto.conf:/mosquitto/config/mosquitto.conf

  influxdb:
    image: influxdb:2.0
    ports: ["8086:8086"]
    environment:
      - DOCKER_INFLUXDB_INIT_MODE=setup
      - DOCKER_INFLUXDB_INIT_USERNAME=admin
      - DOCKER_INFLUXDB_INIT_PASSWORD=password
      - DOCKER_INFLUXDB_INIT_ORG=velib
      - DOCKER_INFLUXDB_INIT_BUCKET=velib

  grafana:
    image: grafana/grafana
    ports: ["3000:3000"]
```

```bash
docker-compose up -d
```

### Dépendances Python

```bash
pip install paho-mqtt requests influxdb-client
```

---

## 5. Implémentation de base

### 5.1 Collecteur (simule un gateway IoT)

```python
# collector.py
import requests
import paho.mqtt.client as mqtt
import json
import time

API_KEY = "your_api_key"
API_URL = "https://api.jcdecaux.com/vls/v1/stations"
MQTT_BROKER = "localhost"
POLL_INTERVAL = 60  # secondes

client = mqtt.Client()
client.connect(MQTT_BROKER, 1883)

previous_states = {}

def collect_and_publish():
    response = requests.get(API_URL, params={"contract": "paris", "apiKey": API_KEY})
    stations = response.json()

    for station in stations:
        station_id = station["number"]
        payload = {
            "id": station_id,
            "bikes": station["available_bikes"],
            "stands": station["available_bike_stands"],
            "timestamp": time.time()
        }
        topic = f"velib/paris/station/{station_id}"
        client.publish(topic, json.dumps(payload))

while True:
    collect_and_publish()
    time.sleep(POLL_INTERVAL)
```

### 5.2 Processeur et détection d'alertes

```python
# processor.py
import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient, Point
from datetime import datetime
import json

CRITICAL_BIKES = 2   # station presque vide
CRITICAL_STANDS = 0  # station pleine

influx = InfluxDBClient(url="http://localhost:8086", token="mytoken", org="velib")
write_api = influx.write_api()

def on_message(client, userdata, msg):
    data = json.loads(msg.payload)

    # Écriture en base
    point = Point("station_status") \
        .tag("station_id", str(data["id"])) \
        .field("available_bikes", data["bikes"]) \
        .field("available_stands", data["stands"]) \
        .time(datetime.utcnow())
    write_api.write("velib", "velib", point)

    # Détection d'anomalie temps réel
    if data["bikes"] <= CRITICAL_BIKES:
        alert = {"station_id": data["id"], "level": "critical", "bikes_left": data["bikes"]}
        client.publish("velib/paris/alerts/critical", json.dumps(alert))

mqtt_client = mqtt.Client()
mqtt_client.on_message = on_message
mqtt_client.connect("localhost", 1883)
mqtt_client.subscribe("velib/paris/station/#")
mqtt_client.loop_forever()
```

### 5.3 Dashboard Grafana

- Accéder à `http://localhost:3000` (admin/admin)
- Ajouter InfluxDB comme datasource
- Créer les panels :
  - **Stat** : nombre de stations critiques en ce moment
  - **Time series** : évolution du nombre de vélos disponibles sur une station
  - **Bar chart** : top 10 des stations les plus souvent vides
  - **Logs** : flux des alertes MQTT en temps réel

---

## 6. Variations

Les variations ci-dessous sont indépendantes et peuvent être traitées séparément ou combinées.

---

### Variation A — Bande passante 📶

> _"Avec 1400 stations qui envoient chaque minute, comment réduire le volume de données transmises sans perdre l'information utile ?"_

#### Trois stratégies à implémenter et comparer

**Stratégie 1 : JSON brut (baseline)**
Tout envoyer, tout le temps, sans transformation.

```python
payload = json.dumps(station_data)
# ~500 bytes par message
```

**Stratégie 2 : Delta encoding**
N'envoyer que si la valeur a changé depuis le dernier envoi.

```python
previous_states = {}

def delta_publish(station):
    sid = station["id"]
    current = station["bikes"]
    if previous_states.get(sid) != current:
        client.publish(topic, json.dumps(station))
        previous_states[sid] = current
    # Sinon : aucun message envoyé
```

**Stratégie 3 : Payload compressé**
Réduire la taille du payload en supprimant les champs inutiles et en compressant.

```python
import msgpack
import zlib

def compressed_publish(station):
    minimal = {"id": station["id"], "b": station["bikes"], "s": station["stands"]}
    packed = msgpack.packb(minimal)       # binaire, plus compact que JSON
    compressed = zlib.compress(packed)    # compression gzip
    client.publish(topic, compressed)
```

#### Mesure comparative

```python
import sys

def measure_strategies(stations):
    results = {}

    # Stratégie 1 : JSON brut
    raw = [json.dumps(s) for s in stations]
    results["JSON brut"] = sum(sys.getsizeof(m) for m in raw)

    # Stratégie 2 : Delta (simuler 20% de changements)
    changed = [s for s in stations if hash(s["id"]) % 5 == 0]
    delta = [json.dumps(s) for s in changed]
    results["Delta encoding"] = sum(sys.getsizeof(m) for m in delta)

    # Stratégie 3 : Compression
    compressed = [zlib.compress(msgpack.packb({"id": s["id"], "b": s["available_bikes"]}))
                  for s in stations]
    results["Compressé"] = sum(sys.getsizeof(m) for m in compressed)

    for name, size in results.items():
        saving = (1 - size / results["JSON brut"]) * 100
        print(f"{name:20s} : {size/1024:.1f} KB  ({saving:.0f}% économie)")
```

#### Résultats attendus (ordre de grandeur)

```
JSON brut            : 700 KB   (0% économie)
Delta encoding       : 140 KB   (~80% économie si peu de changements)
Compressé (msgpack)  : 120 KB   (~83% économie)
```

#### Questions à documenter

- Quel est le volume économisé sur une journée complète avec chaque stratégie ?
- Le delta encoding fonctionne-t-il aussi bien aux heures de pointe (beaucoup de changements) ?
- Quel est le coût en CPU de la compression ?

---

### Variation B — Stockage 💾

> _"735 millions de points par an, on ne peut pas tout garder. Quelle stratégie de rétention adopter sans perdre l'essentiel ?"_

#### Le problème concret

```
Données brutes (1 min) : 2 016 000 points/jour   → ~100 MB/jour
Sur 1 an               : ~735 M points            → ~35 GB
Sur 5 ans              : ~3,6 milliards de points → ~175 GB
```

#### Trois stratégies de rétention à implémenter

**Stratégie 1 : Tout garder (raw)**
Simple mais coûteux. Sert de baseline.

**Stratégie 2 : Agrégation temporelle**
Garder les données brutes 7 jours, puis agréger.

```python
import pandas as pd

def aggregate_to_hourly(raw_df):
    """Réduit N points/heure à 1 point/heure avec statistiques"""
    return raw_df.resample('1H').agg({
        'available_bikes': ['mean', 'min', 'max', 'std']
    })

def aggregate_to_daily(raw_df):
    return raw_df.resample('1D').agg({
        'available_bikes': ['mean', 'min', 'max']
    })
```

**Stratégie 3 : Stockage orienté événements**
Ne stocker que les changements d'état significatifs.

```python
SIGNIFICANT_CHANGE = 3  # ne stocker que si variation > 3 vélos

def event_based_store(new_value, last_stored_value, station_id):
    if abs(new_value - last_stored_value) >= SIGNIFICANT_CHANGE:
        write_to_db(station_id, new_value)
        return new_value
    return last_stored_value
```

#### Politique de rétention dans InfluxDB

```
Données brutes (1 min)   → rétention 7 jours
Données horaires         → rétention 3 mois
Données journalières     → rétention 2 ans
Alertes et événements    → rétention illimitée
```

#### Comparaison du volume stocké

```python
def compare_storage_volume(raw_data):
    raw_points = len(raw_data)

    hourly = raw_points // 60
    daily = raw_points // 1440

    # Calcul volume (estimation 50 bytes/point)
    BYTES_PER_POINT = 50

    print(f"Brut (1 min)     : {raw_points:>10} points → {raw_points * BYTES_PER_POINT / 1e6:.1f} MB/jour")
    print(f"Agrégé (1h)      : {hourly:>10} points → {hourly * BYTES_PER_POINT / 1e6:.2f} MB/jour")
    print(f"Agrégé (1 jour)  : {daily:>10} points → {daily * BYTES_PER_POINT / 1e6:.3f} MB/jour")
```

#### Résultats attendus

```
Brut (1 min)     : 2 016 000 points → 100.8 MB/jour
Agrégé (1h)      :    33 600 points →   1.7 MB/jour  (98% économie)
Agrégé (1 jour)  :     1 400 points →   0.07 MB/jour (99.9% économie)
```

#### Questions à documenter

- Quelle information perd-on en agrégeant à l'heure vs à la minute ?
- Peut-on détecter une station qui s'est vidée en 5 minutes avec des données horaires ?
- Quelle est la stratégie optimale selon le cas d'usage (temps réel vs historique) ?

---

## 7. Planning suggéré

### Jour 1 — Infrastructure et collecte

- [x] Créer un compte JCDecaux et obtenir une clé API
- [x] Lancer le `docker-compose`
- [x] Écrire et tester le collecteur
- [x] Vérifier que les messages arrivent dans MQTT (`mosquitto_sub -t "velib/#" -v`)

### Jour 2 — Traitement et visualisation

- [x] Écrire le processeur avec détection d'alertes
- [x] Vérifier les données dans InfluxDB
- [x] Construire le dashboard Grafana

### Jour 3 — Variations et analyse

- [ ] Implémenter la variation choisie (A, B, ou les deux)
- [ ] Mesurer et comparer les métriques
- [ ] Documenter les résultats et les conclusions

---

## 8. Livrables attendus

1. **Le code** du collecteur, processeur et des variations
2. **Le dashboard** Grafana fonctionnel avec données en temps réel
3. **Un rapport** d'une page résumant les tensions observées :

```
┌──────────────────┬──────────────────────────┬────────────────────────┐
│ Tension          │ Observation mesurée       │ Solution implémentée   │
├──────────────────┼──────────────────────────┼────────────────────────┤
│ Bande passante   │ 1 GB/jour en JSON brut   │ Delta encoding (-80%)  │
│ Stockage         │ 35 GB/an en raw           │ Agrégation horaire     │
│ Latence alertes  │ Détection en < 60s        │ Traitement MQTT temps  │
│                  │                           │ réel                   │
└──────────────────┴──────────────────────────┴────────────────────────┘
```

---

## 9. Ressources

| Ressource              | Lien                                     |
| ---------------------- | ---------------------------------------- |
| API JCDecaux           | https://developer.jcdecaux.com           |
| Documentation MQTT     | https://mqtt.org                         |
| Paho MQTT Python       | https://pypi.org/project/paho-mqtt       |
| InfluxDB Python client | https://pypi.org/project/influxdb-client |
| Grafana Docs           | https://grafana.com/docs                 |
| Wokwi (simulateur IoT) | https://wokwi.com                        |
