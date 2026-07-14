-- ============================================================
-- FestivalOS MVP — schéma PostgreSQL
-- ============================================================

CREATE TABLE IF NOT EXISTS scenes (
    scene_id    TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    capacity    INT NOT NULL,
    lat         DOUBLE PRECISION,
    lon         DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS zone_metrics (
    id              SERIAL PRIMARY KEY,
    zone_id         TEXT NOT NULL,
    ts              TIMESTAMPTZ NOT NULL,
    headcount_est   INT,
    density         DOUBLE PRECISION,   -- headcount / capacité, entre 0 et 1+
    avg_speed       DOUBLE PRECISION
);
CREATE INDEX IF NOT EXISTS idx_zone_metrics_zone_ts ON zone_metrics(zone_id, ts);

CREATE TABLE IF NOT EXISTS resources (
    resource_id     TEXT PRIMARY KEY,
    type            TEXT NOT NULL,      -- water | food
    zone_id         TEXT NOT NULL,
    stock_level_pct DOUBLE PRECISION,
    updated_at      TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS alerts (
    alert_id            SERIAL PRIMARY KEY,
    ts                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    type                TEXT NOT NULL,   -- crowd_density | crowd_anomaly | stock_low
    severity            TEXT NOT NULL,   -- medium | high
    zone_id             TEXT,
    value               DOUBLE PRECISION,
    threshold           DOUBLE PRECISION,
    recommended_action  TEXT,
    status              TEXT DEFAULT 'open'
);
CREATE INDEX IF NOT EXISTS idx_alerts_ts ON alerts(ts DESC);

-- ------------------------------------------------------------
-- Données de référence (les 4 scènes du festival)
-- ------------------------------------------------------------
INSERT INTO scenes (scene_id, name, capacity, lat, lon) VALUES
    ('main_stage',      'Main Stage',        5000, 48.8580, 2.2950),
    ('electro_stage',   'Electro Stage',     2500, 48.8590, 2.2930),
    ('rock_stage',      'Rock Stage',        2000, 48.8570, 2.2970),
    ('discovery_stage', 'Scène Découverte',   800, 48.8560, 2.2940)
ON CONFLICT (scene_id) DO NOTHING;

INSERT INTO resources (resource_id, type, zone_id, stock_level_pct, updated_at) VALUES
    ('water_main',      'water', 'main_stage',      100, now()),
    ('water_electro',   'water', 'electro_stage',   100, now()),
    ('food_rock',       'food',  'rock_stage',      100, now()),
    ('food_discovery',  'food',  'discovery_stage', 100, now())
ON CONFLICT (resource_id) DO NOTHING;
