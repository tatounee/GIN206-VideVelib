#!/bin/sh
set -e

echo "[setup] Création du bucket velib_1h (rétention 2 ans)..."
influx bucket create \
    --name velib_1h \
    --retention 17520h \
    --org "${INFLUX_ORG}" 2>/dev/null \
    && echo "[setup] Bucket velib_1h créé." \
    || echo "[setup] Bucket velib_1h existe déjà, skip."

echo "[setup] Création de la tâche d'agrégation horaire..."
if ! influx task list --org "${INFLUX_ORG}" | grep -q "aggregate_1h"; then
    influx task create --org "${INFLUX_ORG}" -f /etc/influxdb/task_aggregate_1h.flux
    echo "[setup] Tâche aggregate_1h créée."
else
    echo "[setup] Tâche aggregate_1h existe déjà, skip."
fi

echo "[setup] Done."
