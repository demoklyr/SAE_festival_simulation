"""
Simulator — génère en continu des événements visiteurs (position, mouvement,
hydratation, humeur) et des événements de stock (eau, nourriture), et les
publie sur Kafka.

Topics produits :
- visitors.position
- logistics.stock
"""

import os
import json
import time
import random
import logging

from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [simulator] %(message)s")
log = logging.getLogger("simulator")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
NUM_VISITORS = int(os.getenv("NUM_VISITORS", "300"))
TICK_SECONDS = float(os.getenv("TICK_SECONDS", "2"))
STOCK_TICK_EVERY = int(os.getenv("STOCK_TICK_EVERY", "5"))  # publie le stock toutes les N ticks

# Doit rester cohérent avec postgres/init.sql
SCENES = {
    "main_stage":      {"lat": 48.8580, "lon": 2.2950, "capacity": 5000, "attractivity": 0.45},
    "electro_stage":    {"lat": 48.8590, "lon": 2.2930, "capacity": 2500, "attractivity": 0.25},
    "rock_stage":       {"lat": 48.8570, "lon": 2.2970, "capacity": 2000, "attractivity": 0.20},
    "discovery_stage":  {"lat": 48.8560, "lon": 2.2940, "capacity": 800,  "attractivity": 0.10},
}

RESOURCES = [
    {"resource_id": "water_main",     "type": "water", "zone": "main_stage"},
    {"resource_id": "water_electro",  "type": "water", "zone": "electro_stage"},
    {"resource_id": "food_rock",      "type": "food",  "zone": "rock_stage"},
    {"resource_id": "food_discovery", "type": "food",  "zone": "discovery_stage"},
]


def make_producer(retries=30, delay=2):
    """Se connecte à Kafka avec retry (le broker met quelques secondes à être prêt)."""
    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            log.info("Connecté à Kafka (%s)", KAFKA_BOOTSTRAP)
            return producer
        except Exception as exc:
            log.warning("Kafka indisponible (%s), retry %s/%s", exc, attempt + 1, retries)
            time.sleep(delay)
    raise RuntimeError("Impossible de se connecter à Kafka après plusieurs tentatives")


def pick_zone():
    zones = list(SCENES.keys())
    weights = [SCENES[z]["attractivity"] for z in zones]
    return random.choices(zones, weights=weights, k=1)[0]


class Visitor:
    __slots__ = ("id", "zone", "lat", "lon", "speed", "hydration", "mood", "group_id")

    def __init__(self, vid, group_id):
        self.id = vid
        self.group_id = group_id
        self.zone = pick_zone()
        center = SCENES[self.zone]
        self.lat = center["lat"] + random.uniform(-0.0006, 0.0006)
        self.lon = center["lon"] + random.uniform(-0.0006, 0.0006)
        self.speed = random.uniform(0.2, 1.4)
        self.hydration = random.uniform(0.6, 1.0)
        self.mood = random.uniform(0.5, 1.0)

    def tick(self):
        # petite probabilité de changer de scène (déplacement entre zones)
        if random.random() < 0.01:
            self.zone = pick_zone()
            center = SCENES[self.zone]
            self.lat = center["lat"] + random.uniform(-0.0006, 0.0006)
            self.lon = center["lon"] + random.uniform(-0.0006, 0.0006)
        else:
            self.lat += random.uniform(-0.00005, 0.00005)
            self.lon += random.uniform(-0.00005, 0.00005)

        self.speed = max(0.05, min(1.6, self.speed + random.uniform(-0.15, 0.15)))
        self.hydration = max(0.0, self.hydration - random.uniform(0.0005, 0.002))
        mood_penalty = 0.05 if self.hydration < 0.2 else 0.0
        self.mood = max(0.0, min(1.0, self.mood + random.uniform(-0.02, 0.02) - mood_penalty))

    def to_event(self):
        return {
            "visitor_id": self.id,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "lat": round(self.lat, 6),
            "lon": round(self.lon, 6),
            "speed_mps": round(self.speed, 2),
            "zone": self.zone,
            "group_id": self.group_id,
            "mood_score": round(self.mood, 2),
            "hydration_level": round(self.hydration, 2),
        }


def main():
    producer = make_producer()
    visitors = [Visitor(f"v_{i:05d}", f"g_{i // 4:04d}") for i in range(NUM_VISITORS)]
    stock = {r["resource_id"]: 100.0 for r in RESOURCES}

    log.info("Simulation démarrée avec %s visiteurs", NUM_VISITORS)
    tick = 0

    while True:
        for v in visitors:
            v.tick()
            producer.send("visitors.position", v.to_event())
        tick += 1

        if tick % STOCK_TICK_EVERY == 0:
            for r in RESOURCES:
                consumption = random.uniform(1.0, 4.0)
                stock[r["resource_id"]] = max(0.0, stock[r["resource_id"]] - consumption)

                producer.send("logistics.stock", {
                    "resource_id": r["resource_id"],
                    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    "type": r["type"],
                    "stock_level_pct": round(stock[r["resource_id"]], 1),
                    "zone": r["zone"],
                })

                # réapprovisionnement simulé quand le stock est quasi épuisé
                if stock[r["resource_id"]] < 3:
                    stock[r["resource_id"]] = 100.0
                    log.info("Réapprovisionnement simulé : %s", r["resource_id"])

        producer.flush()
        time.sleep(TICK_SECONDS)


if __name__ == "__main__":
    main()
