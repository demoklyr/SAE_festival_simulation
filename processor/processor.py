"""
Processor — consomme Kafka (visitors.position, logistics.stock), maintient
un état courant en mémoire, agrège périodiquement par zone, détecte les
anomalies (règles + Isolation Forest) et écrit tout dans PostgreSQL.
Les alertes sont aussi republiées sur le topic alerts.critical.

C'est le remplaçant "léger" de Spark Streaming pour le MVP : même rôle
fonctionnel (ingestion + agrégation temps réel), implémentation en pandas/
Python pur pour aller vite. Le format des tables Postgres reste identique
à la version cible, ce qui permet de brancher un vrai job Spark plus tard
sans rien changer côté schéma ni côté Kafka.
"""

import os
import json
import time
import threading
import logging
from collections import defaultdict
from datetime import datetime, timezone

import numpy as np
import psycopg2
from psycopg2.extras import execute_values
from kafka import KafkaConsumer, KafkaProducer
from sklearn.ensemble import IsolationForest

logging.basicConfig(level=logging.INFO, format="%(asctime)s [processor] %(message)s")
log = logging.getLogger("processor")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://festival:festival@postgres:5432/festivalos")
AGG_WINDOW_SECONDS = int(os.getenv("AGG_WINDOW_SECONDS", "15"))

DENSITY_ALERT_THRESHOLD = 0.85   # % de la capacité de la scène
STOCK_ALERT_THRESHOLD = 15.0     # % de stock restant


# ------------------------------------------------------------------
# Connexions avec retry (les autres conteneurs peuvent démarrer avant Kafka/PG)
# ------------------------------------------------------------------
def connect_pg(retries=30, delay=2):
    for attempt in range(retries):
        try:
            conn = psycopg2.connect(POSTGRES_DSN)
            conn.autocommit = True
            log.info("Connecté à PostgreSQL")
            return conn
        except Exception as exc:
            log.warning("Postgres indisponible (%s), retry %s/%s", exc, attempt + 1, retries)
            time.sleep(delay)
    raise RuntimeError("Impossible de se connecter à PostgreSQL")


def make_consumer(retries=30, delay=2):
    for attempt in range(retries):
        try:
            consumer = KafkaConsumer(
                "visitors.position", "logistics.stock",
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id="processor-group",
                auto_offset_reset="latest",
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            )
            log.info("Connecté à Kafka (consumer)")
            return consumer
        except Exception as exc:
            log.warning("Kafka indisponible (%s), retry %s/%s", exc, attempt + 1, retries)
            time.sleep(delay)
    raise RuntimeError("Impossible de se connecter à Kafka")


def make_producer(retries=30, delay=2):
    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            log.info("Connecté à Kafka (producer)")
            return producer
        except Exception as exc:
            log.warning("Kafka indisponible (%s), retry %s/%s", exc, attempt + 1, retries)
            time.sleep(delay)
    raise RuntimeError("Impossible de se connecter à Kafka")


# ------------------------------------------------------------------
# État courant en mémoire
# ------------------------------------------------------------------
class State:
    def __init__(self, conn):
        self.lock = threading.Lock()
        self.visitor_zone = {}                       # visitor_id -> zone_id
        self.visitor_speed = {}                       # visitor_id -> speed
        self.stock_levels = {}                         # resource_id -> {...}
        self.scene_capacity = {}                        # zone_id -> capacity
        self.density_history = defaultdict(list)         # zone_id -> [density,...]
        self._load_scenes(conn)

    def _load_scenes(self, conn):
        with conn.cursor() as cur:
            cur.execute("SELECT scene_id, capacity FROM scenes;")
            for scene_id, capacity in cur.fetchall():
                self.scene_capacity[scene_id] = capacity
        log.info("Scènes chargées : %s", self.scene_capacity)

    def apply_position(self, msg):
        with self.lock:
            self.visitor_zone[msg["visitor_id"]] = msg["zone"]
            self.visitor_speed[msg["visitor_id"]] = msg.get("speed_mps", 0)

    def apply_stock(self, msg):
        with self.lock:
            self.stock_levels[msg["resource_id"]] = {
                "type": msg["type"],
                "zone_id": msg["zone"],
                "pct": msg["stock_level_pct"],
            }

    def snapshot_zone_counts(self):
        with self.lock:
            counts = defaultdict(int)
            speeds = defaultdict(list)
            for vid, zone in self.visitor_zone.items():
                counts[zone] += 1
                speeds[zone].append(self.visitor_speed.get(vid, 0))
            avg_speeds = {z: (sum(v) / len(v) if v else 0) for z, v in speeds.items()}
            return dict(counts), avg_speeds

    def snapshot_stocks(self):
        with self.lock:
            return dict(self.stock_levels)


# ------------------------------------------------------------------
# Détection d'anomalies — Isolation Forest léger, réentraîné à la volée
# ------------------------------------------------------------------
def detect_density_anomaly(state, zone_id, density):
    history = state.density_history[zone_id]
    history.append(density)
    if len(history) > 200:
        del history[: len(history) - 200]
    if len(history) < 30:
        return False  # pas assez d'historique pour juger

    X = np.array(history).reshape(-1, 1)
    model = IsolationForest(n_estimators=50, contamination=0.1, random_state=42)
    model.fit(X)
    prediction = model.predict([[density]])[0]  # -1 = anomalie, 1 = normal
    return prediction == -1


# ------------------------------------------------------------------
# Écriture PostgreSQL
# ------------------------------------------------------------------
def write_zone_metrics(conn, rows):
    if not rows:
        return
    with conn.cursor() as cur:
        execute_values(
            cur,
            "INSERT INTO zone_metrics (zone_id, ts, headcount_est, density, avg_speed) VALUES %s",
            rows,
        )


def write_stock_levels(conn, rows):
    if not rows:
        return
    with conn.cursor() as cur:
        execute_values(
            cur,
            """UPDATE resources AS r
               SET stock_level_pct = data.stock_level_pct,
                   updated_at = data.updated_at
               FROM (VALUES %s) AS data(resource_id, stock_level_pct, updated_at)
               WHERE r.resource_id = data.resource_id""",
            rows,
        )


def write_alert(conn, producer, alert):
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO alerts (type, severity, zone_id, value, threshold, recommended_action)
               VALUES (%s, %s, %s, %s, %s, %s)""",
            (
                alert["type"], alert["severity"], alert["zone_id"],
                alert["value"], alert["threshold"], alert["recommended_action"],
            ),
        )
    producer.send("alerts.critical", alert)
    log.warning("ALERTE %s zone=%s valeur=%.2f", alert["type"], alert["zone_id"], alert["value"])


# ------------------------------------------------------------------
# Boucles principales
# ------------------------------------------------------------------
def consume_loop(consumer, state):
    for msg in consumer:
        try:
            if msg.topic == "visitors.position":
                state.apply_position(msg.value)
            elif msg.topic == "logistics.stock":
                state.apply_stock(msg.value)
        except Exception as exc:
            log.error("Erreur traitement message: %s", exc)


def aggregate_loop(state, conn, producer):
    while True:
        time.sleep(AGG_WINDOW_SECONDS)
        now = datetime.now(timezone.utc)

        counts, avg_speeds = state.snapshot_zone_counts()
        rows = []
        for zone_id, capacity in state.scene_capacity.items():
            headcount = counts.get(zone_id, 0)
            density = headcount / capacity if capacity else 0
            avg_speed = avg_speeds.get(zone_id, 0)
            rows.append((zone_id, now, headcount, density, avg_speed))

            if density >= DENSITY_ALERT_THRESHOLD:
                write_alert(conn, producer, {
                    "type": "crowd_density",
                    "severity": "high" if density >= 0.95 else "medium",
                    "zone_id": zone_id,
                    "value": round(density, 3),
                    "threshold": DENSITY_ALERT_THRESHOLD,
                    "recommended_action": f"limiter l'accès à {zone_id}",
                })
            elif detect_density_anomaly(state, zone_id, density):
                write_alert(conn, producer, {
                    "type": "crowd_anomaly",
                    "severity": "medium",
                    "zone_id": zone_id,
                    "value": round(density, 3),
                    "threshold": DENSITY_ALERT_THRESHOLD,
                    "recommended_action": f"surveiller {zone_id} (comportement inhabituel)",
                })

        write_zone_metrics(conn, rows)
        log.info("Agrégation écrite pour %s zones", len(rows))

        stock_rows = []
        for resource_id, info in state.snapshot_stocks().items():
            stock_rows.append((resource_id, info["pct"], now))
            if info["pct"] < STOCK_ALERT_THRESHOLD:
                write_alert(conn, producer, {
                    "type": "stock_low",
                    "severity": "high" if info["pct"] < 5 else "medium",
                    "zone_id": info["zone_id"],
                    "value": round(info["pct"], 1),
                    "threshold": STOCK_ALERT_THRESHOLD,
                    "recommended_action": f"réapprovisionner {resource_id}",
                })
        write_stock_levels(conn, stock_rows)


def main():
    conn = connect_pg()
    consumer = make_consumer()
    producer = make_producer()
    state = State(conn)

    consumer_thread = threading.Thread(target=consume_loop, args=(consumer, state), daemon=True)
    consumer_thread.start()

    log.info("Processor démarré — agrégation toutes les %ss", AGG_WINDOW_SECONDS)
    aggregate_loop(state, conn, producer)


if __name__ == "__main__":
    main()
