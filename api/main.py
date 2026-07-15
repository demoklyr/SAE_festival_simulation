"""
Dashboard-API — point d'entrée unique pour le frontend.

Endpoints REST :
    GET  /health
    GET  /zones                      -> dernière mesure par zone + capacité
    GET  /zones/{zone_id}/history     -> historique de densité (fenêtre configurable)
    GET  /alerts                      -> alertes récentes
    GET  /resources                   -> niveaux de stock actuels
    GET  /predict/{zone_id}           -> prévision de densité à horizon N minutes
    GET  /optimize                    -> recommandations d'allocation (règle gloutonne)

WebSocket :
    WS   /ws/live                     -> pousse zones + alertes toutes les 3s

Le modèle de prévision est volontairement simple pour le MVP (régression
linéaire sur l'historique récent, via numpy.polyfit) : un seul modèle,
bien évalué, plutôt que plusieurs modèles bâclés. Remplaçable plus tard
par Prophet/XGBoost sans changer le contrat de l'endpoint.
"""

import os
import json
import asyncio
import logging
from datetime import datetime, timezone, timedelta

import asyncpg
import numpy as np
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [api] %(message)s")
log = logging.getLogger("api")

POSTGRES_DSN = os.getenv(
    "POSTGRES_DSN", "postgresql://festival:festival@postgres:5432/festivalos"
)
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
WS_PUSH_INTERVAL_SECONDS = float(os.getenv("WS_PUSH_INTERVAL_SECONDS", "3"))

STOCK_ALERT_THRESHOLD = 15.0
DENSITY_CRITICAL = 0.85
DENSITY_WATCH = 0.60


class RestockRequest(BaseModel):
    level_pct: float = Field(100.0, ge=0, le=100, description="Niveau de stock cible en %")


app = FastAPI(title="FestivalOS API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ------------------------------------------------------------------
# Pool de connexions Postgres
# ------------------------------------------------------------------
def _make_kafka_producer(retries=30, delay=2):
    """Connexion Kafka synchrone avec retry (appelée dans un thread au démarrage)."""
    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            log.info("Connecté à Kafka (producer, %s)", KAFKA_BOOTSTRAP)
            return producer
        except Exception as exc:
            log.warning("Kafka indisponible (%s), retry %s/%s", exc, attempt + 1, retries)
            import time as _time
            _time.sleep(delay)
    raise RuntimeError("Impossible de se connecter à Kafka après plusieurs tentatives")


@app.on_event("startup")
async def startup():
    app.state.pool = await asyncpg.create_pool(dsn=POSTGRES_DSN, min_size=2, max_size=10)
    log.info("Pool PostgreSQL initialisé")
    app.state.kafka_producer = await asyncio.to_thread(_make_kafka_producer)


@app.on_event("shutdown")
async def shutdown():
    await app.state.pool.close()
    if getattr(app.state, "kafka_producer", None):
        app.state.kafka_producer.close()


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------
async def fetch_latest_zones(conn):
    rows = await conn.fetch(
        """
        SELECT DISTINCT ON (zm.zone_id)
            zm.zone_id, s.name, s.capacity, s.lat, s.lon,
            zm.ts, zm.headcount_est, zm.density, zm.avg_speed
        FROM zone_metrics zm
        JOIN scenes s ON s.scene_id = zm.zone_id
        ORDER BY zm.zone_id, zm.ts DESC;
        """
    )
    return [dict(r) for r in rows]


async def fetch_history(conn, zone_id, minutes):
    since = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    rows = await conn.fetch(
        """
        SELECT ts, headcount_est, density, avg_speed
        FROM zone_metrics
        WHERE zone_id = $1 AND ts >= $2
        ORDER BY ts ASC;
        """,
        zone_id, since,
    )
    return [dict(r) for r in rows]


async def fetch_alerts(conn, limit, status):
    if status:
        rows = await conn.fetch(
            "SELECT * FROM alerts WHERE status = $1 ORDER BY ts DESC LIMIT $2;",
            status, limit,
        )
    else:
        rows = await conn.fetch(
            "SELECT * FROM alerts ORDER BY ts DESC LIMIT $1;", limit
        )
    return [dict(r) for r in rows]


async def fetch_resources(conn):
    rows = await conn.fetch("SELECT * FROM resources ORDER BY resource_id;")
    return [dict(r) for r in rows]


def _serialize(obj):
    """Convertit récursivement les datetime en ISO string pour le JSON / WebSocket."""
    if isinstance(obj, dict):
        return {k: _serialize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_serialize(v) for v in obj]
    if isinstance(obj, datetime):
        return obj.isoformat()
    return obj


# ------------------------------------------------------------------
# Endpoints REST
# ------------------------------------------------------------------
@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/zones")
async def get_zones():
    async with app.state.pool.acquire() as conn:
        zones = await fetch_latest_zones(conn)
    return _serialize(zones)


@app.get("/zones/{zone_id}/history")
async def get_zone_history(zone_id: str, minutes: int = Query(30, ge=1, le=1440)):
    async with app.state.pool.acquire() as conn:
        history = await fetch_history(conn, zone_id, minutes)
    if not history:
        raise HTTPException(status_code=404, detail=f"Aucune donnée pour la zone '{zone_id}'")
    return _serialize(history)


@app.get("/alerts")
async def get_alerts(limit: int = Query(20, ge=1, le=200), status: str | None = None):
    async with app.state.pool.acquire() as conn:
        alerts = await fetch_alerts(conn, limit, status)
    return _serialize(alerts)


@app.get("/resources")
async def get_resources():
    async with app.state.pool.acquire() as conn:
        resources = await fetch_resources(conn)
    return _serialize(resources)


@app.get("/predict/{zone_id}")
async def predict_zone(zone_id: str, horizon_minutes: int = Query(30, ge=5, le=120)):
    """
    Prévision simple par régression linéaire sur les 60 dernières minutes
    d'historique de densité. Retourne la densité prédite + un intervalle
    de confiance basé sur l'écart-type des résidus.
    """
    async with app.state.pool.acquire() as conn:
        history = await fetch_history(conn, zone_id, minutes=60)

    if len(history) < 5:
        raise HTTPException(
            status_code=422,
            detail="Historique insuffisant pour prédire (attendre plus de données)",
        )

    t0 = history[0]["ts"]
    x = np.array([(h["ts"] - t0).total_seconds() for h in history])
    y = np.array([h["density"] for h in history])

    # régression linéaire degré 1 (baseline volontairement simple)
    coeffs = np.polyfit(x, y, 1)
    slope, intercept = coeffs
    predicted_fit = slope * x + intercept
    residual_std = float(np.std(y - predicted_fit))

    target_x = x[-1] + horizon_minutes * 60
    predicted_density = float(slope * target_x + intercept)
    predicted_density = max(0.0, predicted_density)

    return {
        "zone_id": zone_id,
        "horizon_minutes": horizon_minutes,
        "predicted_density": round(predicted_density, 4),
        "confidence_interval": [
            round(max(0.0, predicted_density - 1.96 * residual_std), 4),
            round(predicted_density + 1.96 * residual_std, 4),
        ],
        "model": "linear_regression_v1",
        "trained_on_points": len(history),
    }


@app.get("/optimize")
async def optimize_allocation():
    """
    Allocation gloutonne : recommande une action par zone/ressource
    critique, triée par urgence décroissante. Remplaçable plus tard
    par OR-Tools sans changer le contrat de l'endpoint.
    """
    async with app.state.pool.acquire() as conn:
        zones = await fetch_latest_zones(conn)
        resources = await fetch_resources(conn)

    recommendations = []

    for z in zones:
        density = z["density"] or 0
        if density >= DENSITY_CRITICAL:
            recommendations.append({
                "type": "crowd", "zone_id": z["zone_id"],
                "urgency": round(density, 3),
                "action": f"Envoyer des agents de sécurité supplémentaires vers {z['zone_id']} et limiter les accès",
            })
        elif density >= DENSITY_WATCH:
            recommendations.append({
                "type": "crowd", "zone_id": z["zone_id"],
                "urgency": round(density * 0.6, 3),
                "action": f"Surveiller {z['zone_id']} de près (approche du seuil critique)",
            })

    for r in resources:
        pct = r["stock_level_pct"] or 0
        if pct < STOCK_ALERT_THRESHOLD:
            urgency = round((STOCK_ALERT_THRESHOLD - pct) / STOCK_ALERT_THRESHOLD, 3)
            recommendations.append({
                "type": "stock", "zone_id": r["zone_id"],
                "urgency": urgency,
                "action": f"Réapprovisionner en urgence {r['resource_id']} ({pct:.1f}% restant)",
            })

    recommendations.sort(key=lambda r: r["urgency"], reverse=True)
    return {"generated_at": datetime.now(timezone.utc).isoformat(), "recommendations": recommendations}


def _send_restock_command(resource_id: str, level_pct: float):
    app.state.kafka_producer.send("logistics.restock", {
        "resource_id": resource_id,
        "level_pct": level_pct,
    })
    app.state.kafka_producer.flush()


@app.post("/resources/{resource_id}/restock")
async def restock_resource(resource_id: str, payload: RestockRequest = RestockRequest()):
    """
    Déclenche un réapprovisionnement pour UNE ressource précise (ex: food_rock).
    La commande est publiée sur Kafka (logistics.restock) : le simulateur reste
    la source de vérité et republiera l'état confirmé via logistics.stock dans
    la foulée. La table `resources` est aussi mise à jour immédiatement pour un
    retour instantané au frontend (le prochain cycle du processor confirmera
    la même valeur).
    """
    async with app.state.pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM resources WHERE resource_id = $1;", resource_id)
        if not row:
            raise HTTPException(status_code=404, detail=f"Ressource '{resource_id}' introuvable")

        await asyncio.to_thread(_send_restock_command, resource_id, payload.level_pct)

        await conn.execute(
            "UPDATE resources SET stock_level_pct = $1, updated_at = $2 WHERE resource_id = $3;",
            payload.level_pct, datetime.now(timezone.utc), resource_id,
        )
        await conn.execute(
            """UPDATE alerts SET status = 'closed'
               WHERE type = 'stock_low' AND status = 'open'
                 AND recommended_action LIKE '%' || $1 || '%';""",
            resource_id,
        )

    log.info("Réapprovisionnement demandé via API : %s -> %.1f%%", resource_id, payload.level_pct)
    return {
        "resource_id": resource_id,
        "requested_level_pct": payload.level_pct,
        "status": "restock_command_sent",
    }


@app.post("/resources/restock")
async def restock_by_type(
    type: str = Query(..., pattern="^(food|water)$", description="'food' ou 'water'"),
    level_pct: float = Query(100.0, ge=0, le=100),
):
    """
    Réapprovisionne en une seule fois toutes les ressources d'un type donné
    (ex: toute la nourriture du festival). Pratique pour un bouton "réappro
    générale" côté dashboard.
    """
    async with app.state.pool.acquire() as conn:
        rows = await conn.fetch("SELECT resource_id FROM resources WHERE type = $1;", type)
        if not rows:
            raise HTTPException(status_code=404, detail=f"Aucune ressource de type '{type}'")

        resource_ids = [r["resource_id"] for r in rows]
        for resource_id in resource_ids:
            await asyncio.to_thread(_send_restock_command, resource_id, level_pct)
            await conn.execute(
                "UPDATE resources SET stock_level_pct = $1, updated_at = $2 WHERE resource_id = $3;",
                level_pct, datetime.now(timezone.utc), resource_id,
            )
            await conn.execute(
                """UPDATE alerts SET status = 'closed'
                   WHERE type = 'stock_low' AND status = 'open'
                     AND recommended_action LIKE '%' || $1 || '%';""",
                resource_id,
            )

    log.info("Réapprovisionnement en masse demandé via API : type=%s -> %.1f%%", type, level_pct)
    return {
        "type": type,
        "resource_ids": resource_ids,
        "requested_level_pct": level_pct,
        "status": "restock_command_sent",
    }


# ------------------------------------------------------------------
# WebSocket temps réel
# ------------------------------------------------------------------
@app.websocket("/ws/live")
async def ws_live(websocket: WebSocket):
    await websocket.accept()
    log.info("Client WebSocket connecté")
    try:
        while True:
            async with app.state.pool.acquire() as conn:
                zones = await fetch_latest_zones(conn)
                alerts = await fetch_alerts(conn, limit=10, status="open")

            payload = {
                "type": "update",
                "ts": datetime.now(timezone.utc).isoformat(),
                "zones": _serialize(zones),
                "alerts": _serialize(alerts),
            }
            await websocket.send_text(json.dumps(payload))
            await asyncio.sleep(WS_PUSH_INTERVAL_SECONDS)
    except WebSocketDisconnect:
        log.info("Client WebSocket déconnecté")
    except Exception as exc:
        log.error("Erreur WebSocket: %s", exc)
