import os
import json
import asyncio
import logging
from datetime import datetime, timezone, timedelta

import asyncpg
import numpy as np
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

logging.basicConfig(level=logging.INFO, format="%(asctime)s [api] %(message)s")
log = logging.getLogger("api")

POSTGRES_DSN = os.getenv(
    "POSTGRES_DSN"
)
WS_PUSH_INTERVAL_SECONDS = float(os.getenv("WS_PUSH_INTERVAL_SECONDS"))

STOCK_ALERT_THRESHOLD = 15.0
DENSITY_CRITICAL = 0.85
DENSITY_WATCH = 0.60

app = FastAPI(title="FestivalOS API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup():
    app.state.pool = await asyncpg.create_pool(dsn=POSTGRES_DSN, min_size=2, max_size=10)
    log.info("Pool PostgreSQL initialisé")


@app.on_event("shutdown")
async def shutdown():
    await app.state.pool.close()


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
