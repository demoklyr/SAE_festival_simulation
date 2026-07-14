# FestivalOS — Système intelligent de gestion d'un festival musical

Plateforme de supervision temps réel pour événements de masse : ingestion de
données de capteurs simulés (visiteurs, scènes, logistique), traitement en
continu, détection d'anomalies et exposition via une API REST/WebSocket.

Projet réalisé dans le cadre d'une SAE (BUT Informatique) — voir les
documents de conception (`SAE_Festival_Intelligent_Projet_Complet.md` et
`SAE_Festival_MVP_4jours.md`) pour l'architecture cible complète et la
roadmap d'évolution.

---

## Sommaire

1. [Description du projet](#1-description-du-projet)
2. [Stack technique](#2-stack-technique)
3. [Architecture & pipeline de données](#3-architecture--pipeline-de-données)
4. [Structure du dépôt](#4-structure-du-dépôt)
5. [Schéma de la base de données](#5-schéma-de-la-base-de-données)
6. [Topics Kafka](#6-topics-kafka)
7. [Services — détail de fonctionnement](#7-services--détail-de-fonctionnement)
8. [API — endpoints](#8-api--endpoints)
9. [Installation & exécution](#9-installation--exécution)
10. [Variables d'environnement](#10-variables-denvironnement)
11. [Commandes utiles (exploitation & debug)](#11-commandes-utiles-exploitation--debug)
12. [Limites connues du MVP](#12-limites-connues-du-mvp)
13. [Roadmap](#13-roadmap)

---

## 1. Description du projet

FestivalOS simule un festival de musique à 4 scènes et plusieurs centaines
de visiteurs virtuels afin de démontrer, de bout en bout, un système capable
de :

- **collecter** en continu des données hétérogènes (position des visiteurs,
  niveaux de stock) via un bus d'événements ;
- **traiter et agréger** ces données en temps quasi réel (densité de foule
  par zone, temps d'attente, consommation) ;
- **détecter des situations anormales** (sur-affluence, rupture de stock,
  comportement de foule inhabituel) en combinant règles métier et
  apprentissage non supervisé (Isolation Forest) ;
- **prévoir l'affluence** à court terme par zone (régression linéaire) ;
- **recommander des actions** d'allocation de ressources (sécurité, stocks)
  selon une logique gloutonne priorisée par urgence ;
- **exposer** toutes ces informations via une API REST et un flux
  WebSocket temps réel, prêts à être consommés par un dashboard.

## 2. Stack technique

| Domaine | Technologie | Rôle |
|---|---|---|
| Bus d'événements | **Apache Kafka** (mode KRaft, broker unique) | Découplage producteurs/consommateurs, ingestion temps réel |
| Traitement / agrégation | **Python** (thread consumer + pandas-like agrégation maison) | Remplace un job Spark Streaming pour le MVP (même contrat de sortie) |
| Détection d'anomalies | **scikit-learn** (Isolation Forest) + règles de seuil | Anomalies de densité, stock bas |
| Prévision | **NumPy** (régression linéaire `polyfit`) | Prévision de densité à horizon configurable |
| Base de données | **PostgreSQL 16** | Source de vérité relationnelle (référentiels, métriques, alertes) |
| API | **FastAPI** + **Uvicorn** + **asyncpg** | REST + WebSocket asynchrones |
| Orchestration | **Docker Compose** | Déploiement reproductible de tous les services |
| Langages | **Python 3.11** (100% des services applicatifs), **SQL** (schéma & requêtes), **YAML** (Compose) | |

## 3. Architecture & pipeline de données

```
┌─────────────┐      JSON       ┌───────────┐      consumer      ┌───────────────┐
│  simulator   │ ───────────────▶│   Kafka    │◀───────────────────│   processor     │
│  (Python)    │  visitors.pos.  │  (KRaft)   │  visitors.position  │ (agrégation +    │
│              │  logistics.stock│            │  logistics.stock    │  IsolationForest)│
└─────────────┘                 └───────────┘                     └────────┬─────────┘
                                       ▲                                    │ UPDATE/INSERT
                                       │ produce                            ▼
                                       │ alerts.critical            ┌───────────────┐
                                       └─────────────────────────── │  PostgreSQL     │
                                                                     │  (zone_metrics, │
                                                                     │   alerts,        │
                                                                     │   resources,     │
                                                                     │   scenes)        │
                                                                     └────────┬─────────┘
                                                                              │ SELECT (asyncpg)
                                                                              ▼
                                                                     ┌───────────────┐
                                                                     │   api (FastAPI) │
                                                                     │ REST + WebSocket │
                                                                     └───────────────┘
```

**Flux détaillé :**

1. `simulator` génère à intervalle régulier (`TICK_SECONDS`) la position,
   vitesse, humeur et hydratation de chaque visiteur virtuel, ainsi que le
   niveau des stocks (eau/nourriture) toutes les `STOCK_TICK_EVERY` ticks.
   Chaque événement est publié en JSON sur Kafka.
2. `processor` consomme les deux topics en continu et maintient un **état
   courant en mémoire** (dernière zone connue de chaque visiteur, dernier
   niveau connu de chaque ressource). Toutes les `AGG_WINDOW_SECONDS`
   secondes, il calcule un instantané agrégé par zone (headcount, densité,
   vitesse moyenne), l'écrit dans `zone_metrics`, met à jour `resources`,
   et applique la logique de détection d'alertes (seuils + Isolation
   Forest). Les alertes sont écrites dans `alerts` et republiées sur le
   topic `alerts.critical`.
3. `api` interroge PostgreSQL à la demande (endpoints REST) ou en boucle
   (WebSocket, toutes les `WS_PUSH_INTERVAL_SECONDS`) et sert les données
   agrégées, les prévisions calculées à la volée et les recommandations
   d'allocation.

## 4. Structure du dépôt

```
FESTIVAL/
├── docker-compose.yml
├── README.md
├── postgres/
│   └── init.sql              # schéma + données de référence (scènes, stocks initiaux)
├── simulator/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── simulator.py           # génération des événements visiteurs/stocks
├── processor/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── processor.py           # consumer Kafka + agrégation + détection d'anomalies
└── api/
    ├── Dockerfile
    ├── requirements.txt
    └── main.py                # API REST + WebSocket
```

## 5. Schéma de la base de données

```
scenes
├── scene_id           TEXT PRIMARY KEY
├── name                TEXT NOT NULL
├── capacity            INT NOT NULL
├── lat                 DOUBLE PRECISION
└── lon                 DOUBLE PRECISION

zone_metrics
├── id                  SERIAL PRIMARY KEY
├── zone_id             TEXT NOT NULL          -- FK logique vers scenes.scene_id
├── ts                  TIMESTAMPTZ NOT NULL   -- horodatage de l'agrégation
├── headcount_est       INT                    -- nb de visiteurs estimé dans la zone
├── density             DOUBLE PRECISION       -- headcount / capacity (0 à 1+)
└── avg_speed           DOUBLE PRECISION       -- vitesse moyenne des visiteurs (m/s)
    index: (zone_id, ts)

resources
├── resource_id         TEXT PRIMARY KEY
├── type                TEXT NOT NULL          -- 'water' | 'food'
├── zone_id             TEXT NOT NULL          -- FK logique vers scenes.scene_id
├── stock_level_pct     DOUBLE PRECISION       -- % de stock restant
└── updated_at          TIMESTAMPTZ

alerts
├── alert_id            SERIAL PRIMARY KEY
├── ts                  TIMESTAMPTZ NOT NULL DEFAULT now()
├── type                TEXT NOT NULL          -- 'crowd_density' | 'crowd_anomaly' | 'stock_low'
├── severity            TEXT NOT NULL          -- 'medium' | 'high'
├── zone_id             TEXT
├── value               DOUBLE PRECISION       -- valeur mesurée ayant déclenché l'alerte
├── threshold           DOUBLE PRECISION       -- seuil de référence
├── recommended_action  TEXT
└── status              TEXT DEFAULT 'open'    -- 'open' | 'closed' (fermeture manuelle à prévoir)
    index: (ts DESC)
```

**Relations** : `zone_metrics.zone_id` et `resources.zone_id` référencent
`scenes.scene_id` (pas de contrainte `FOREIGN KEY` physique dans le MVP
pour rester simple à modifier — à ajouter en V2 si le référentiel de scènes
devient dynamique).

Le schéma complet et les données de départ sont dans `postgres/init.sql`,
exécuté automatiquement au premier démarrage du conteneur `postgres`
(monté dans `/docker-entrypoint-initdb.d/`).

## 6. Topics Kafka

| Topic | Producteur | Consommateur | Format |
|---|---|---|---|
| `visitors.position` | `simulator` | `processor` | `{visitor_id, timestamp, lat, lon, speed_mps, zone, group_id, mood_score, hydration_level}` |
| `logistics.stock` | `simulator` | `processor` | `{resource_id, timestamp, type, stock_level_pct, zone}` |
| `alerts.critical` | `processor` | *(libre — prêt pour un futur service de notification)* | `{type, severity, zone_id, value, threshold, recommended_action}` |

Créés automatiquement au démarrage par le service `kafka-init`
(`visitors.position` : 3 partitions ; les deux autres : 1 partition ;
facteur de réplication 1, cohérent avec un broker unique en dev).

## 7. Services — détail de fonctionnement

### `simulator`
- Génère `NUM_VISITORS` visiteurs virtuels répartis entre les 4 scènes
  selon un poids d'attractivité fixe (`main_stage` = 45%, `electro_stage`
  = 25%, `rock_stage` = 20%, `discovery_stage` = 10%).
- Chaque visiteur a 1% de chance par tick de changer de zone (simule les
  déplacements entre scènes), sinon dérive légèrement autour de sa
  position (marche aléatoire).
- L'hydratation décroît en continu ; l'humeur baisse si l'hydratation
  passe sous 20%.
- Les stocks (eau/nourriture, un par zone) se consomment aléatoirement
  toutes les `STOCK_TICK_EVERY` ticks et se réapprovisionnent
  automatiquement à 100% quand ils tombent sous 3% (simule une livraison).

### `processor`
- Maintient un état en mémoire (`State`) protégé par un verrou thread
  (`threading.Lock`), mis à jour en continu par un thread consumer dédié.
- Toutes les `AGG_WINDOW_SECONDS`, la boucle d'agrégation :
  1. calcule headcount/densité/vitesse moyenne par zone à partir de
     l'état courant et les insère dans `zone_metrics` ;
  2. déclenche une alerte `crowd_density` si la densité dépasse
     `DENSITY_ALERT_THRESHOLD` (0.85, sévérité `high` au-delà de 0.95) ;
  3. sinon, teste une anomalie statistique (z-score > 2.5 sur
     l'historique glissant de la zone) confirmée par un Isolation Forest
     réentraîné à la volée (`contamination=0.05`) → alerte
     `crowd_anomaly` ;
  4. met à jour `resources.stock_level_pct` pour chaque ressource connue ;
  5. déclenche une alerte `stock_low` si un stock passe sous
     `STOCK_ALERT_THRESHOLD` (15%, sévérité `high` sous 5%).
- Chaque alerte est à la fois **écrite dans PostgreSQL** et **republiée**
  sur `alerts.critical`.

### `api`
- Pool de connexions PostgreSQL asynchrone (`asyncpg`) initialisé au
  démarrage (`startup` event).
- Tous les endpoints REST sont non-bloquants ; le WebSocket tourne une
  boucle `while True` par connexion, interrogeant Postgres toutes les
  `WS_PUSH_INTERVAL_SECONDS`.
- Le modèle de prévision (`/predict/{zone_id}`) est calculé **à la
  volée** à chaque appel (pas de modèle pré-entraîné stocké) : régression
  linéaire sur les 60 dernières minutes d'historique de densité de la
  zone demandée.

## 8. API — endpoints

Base URL par défaut : `http://localhost:8000`
Documentation interactive auto-générée : `http://localhost:8000/docs`

### `GET /health`
Vérifie que l'API répond.
```json
{"status": "ok"}
```

### `GET /zones`
Dernière mesure connue pour chaque zone (une ligne par scène).
```json
[
  {
    "zone_id": "main_stage", "name": "Main Stage", "capacity": 5000,
    "lat": 48.858, "lon": 2.295, "ts": "2026-07-12T14:13:57Z",
    "headcount_est": 129, "density": 0.0258, "avg_speed": 0.843
  }
]
```

### `GET /zones/{zone_id}/history?minutes=30`
Historique brut des agrégations pour une zone donnée sur la fenêtre
demandée (1 à 1440 minutes). Réponse `404` si la zone n'existe pas /
n'a pas encore de données.
```json
[{"ts": "2026-07-12T14:10:00Z", "headcount_est": 120, "density": 0.024, "avg_speed": 0.81}]
```

### `GET /alerts?limit=20&status=open`
Alertes les plus récentes. `status` optionnel (`open`/`closed`), `limit`
entre 1 et 200 (défaut 20).
```json
[{"alert_id": 26, "ts": "2026-07-12T14:14:43Z", "type": "crowd_anomaly",
  "severity": "medium", "zone_id": "rock_stage", "value": 0.036,
  "threshold": 0.85, "recommended_action": "surveiller rock_stage (comportement inhabituel)",
  "status": "open"}]
```

### `GET /resources`
Niveau de stock actuel de toutes les ressources suivies.
```json
[{"resource_id": "water_main", "type": "water", "zone_id": "main_stage",
  "stock_level_pct": 42.3, "updated_at": "2026-07-12T14:14:00Z"}]
```

### `GET /predict/{zone_id}?horizon_minutes=30`
Prévision de densité à horizon donné (5 à 120 minutes). Renvoie `422`
si moins de 5 points d'historique sont disponibles pour la zone (attendre
quelques cycles d'agrégation après le démarrage).
```json
{
  "zone_id": "main_stage", "horizon_minutes": 30,
  "predicted_density": 0.0137,
  "confidence_interval": [0.0124, 0.015],
  "model": "linear_regression_v1", "trained_on_points": 32
}
```

### `GET /optimize`
Recommandations d'allocation triées par urgence décroissante (foule
critique/en approche du seuil + stocks sous le seuil d'alerte).
```json
{
  "generated_at": "2026-07-12T14:14:43Z",
  "recommendations": [
    {"type": "stock", "zone_id": "electro_stage", "urgency": 0.96,
     "action": "Réapprovisionner en urgence water_electro (0.6% restant)"}
  ]
}
```

### `WS /ws/live`
Pousse un message JSON toutes les `WS_PUSH_INTERVAL_SECONDS` (3s par
défaut) tant que la connexion est ouverte.
```json
{
  "type": "update", "ts": "2026-07-12T14:14:43Z",
  "zones": [ /* même format que GET /zones */ ],
  "alerts": [ /* 10 alertes ouvertes les plus récentes */ ]
}
```
Test rapide depuis une console JS :
```js
const ws = new WebSocket("ws://localhost:8000/ws/live");
ws.onmessage = (e) => console.log(JSON.parse(e.data));
```

## 9. Installation & exécution

**Prérequis** : Docker + Docker Compose v2.

```bash
git clone <url-du-depot>
cd FESTIVAL
docker compose up --build
```

Premier démarrage : compter 20-30s (healthcheck Kafka, création des
topics par `kafka-init`, puis démarrage de `simulator`/`processor`/`api`).

Pour démarrer/reconstruire un seul service (ex : après modification du
code) :
```bash
docker compose up --build processor
docker compose up --build api
```

Arrêt :
```bash
docker compose down          # conserve les données
docker compose down -v       # supprime aussi les volumes (Kafka + Postgres) — repart de zéro
```

## 10. Variables d'environnement

| Variable | Service | Défaut | Effet |
|---|---|---|---|
| `NUM_VISITORS` | simulator | `300` | Nombre de visiteurs simulés |
| `TICK_SECONDS` | simulator | `2` | Fréquence de mise à jour des positions |
| `STOCK_TICK_EVERY` | simulator | `5` | Publie le stock toutes les N ticks |
| `KAFKA_BOOTSTRAP` | simulator, processor | `kafka:9092` | Adresse du broker Kafka |
| `POSTGRES_DSN` | processor, api | `postgresql://festival:festival@postgres:5432/festivalos` | Chaîne de connexion PostgreSQL |
| `AGG_WINDOW_SECONDS` | processor | `15` | Fréquence d'agrégation / écriture Postgres |
| `WS_PUSH_INTERVAL_SECONDS` | api | `3` | Fréquence de push du WebSocket |

Seuils codés en dur (à externaliser en variables d'env si besoin de les
ajuster sans rebuild) :
- `processor.py` : `DENSITY_ALERT_THRESHOLD = 0.85`, `STOCK_ALERT_THRESHOLD = 15.0`
- `api/main.py` : `DENSITY_CRITICAL = 0.85`, `DENSITY_WATCH = 0.60`, `STOCK_ALERT_THRESHOLD = 15.0`

## 11. Commandes utiles (exploitation & debug)

**Logs**
```bash
docker compose logs -f simulator processor api
```

**Lire les messages Kafka bruts**
```bash
docker exec -it festival-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic visitors.position \
  --from-beginning --max-messages 5
```

**Vérifier l'état d'un consumer group**
```bash
docker exec -it festival-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group processor-group
```

**Requêtes SQL directes**
```bash
docker exec -it festival-postgres psql -U festival -d festivalos
```
```sql
SELECT * FROM zone_metrics ORDER BY ts DESC LIMIT 10;
SELECT * FROM alerts ORDER BY ts DESC LIMIT 10;
SELECT * FROM resources;
```

**Tester l'API en ligne de commande**
```bash
curl http://localhost:8000/zones
curl http://localhost:8000/predict/main_stage
curl http://localhost:8000/optimize
```

## 12. Limites connues du MVP

- Pas de `FOREIGN KEY` physique entre `zone_metrics`/`resources` et
  `scenes` — cohérence garantie uniquement par le code applicatif.
- `alerts.status` n'est jamais mis à `closed` automatiquement (pas de
  logique de résolution d'alerte dans le MVP).
- Un seul broker Kafka (`replication-factor=1`) : aucune tolérance de
  panne, adapté au développement uniquement.
- Le modèle de prévision est une régression linéaire simple recalculée à
  chaque appel — pas de persistance/versioning de modèle (pas de MLflow
  à ce stade).
- Pas de frontend à ce jour : consommation uniquement via `curl`/Swagger/
  client WebSocket.

## 13. Roadmap

Voir `SAE_Festival_Intelligent_Projet_Complet.md` (section 16) et
`SAE_Festival_MVP_4jours.md` (section 7 — Roadmap d'upgrade) pour le détail
des étapes prévues : Spark Streaming en remplacement du `processor` Python,
MinIO pour l'archivage brut, MLflow pour le tracking des modèles, OR-Tools
pour l'allocation, Airflow pour le ré-entraînement périodique, et frontend
React/Next.js (carte temps réel, heatmap, panneau d'alertes, courbes de
prévision).