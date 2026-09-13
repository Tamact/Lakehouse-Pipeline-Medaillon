# Plateforme Data Lakehouse — FakeStoreAPI → NiFi → MinIO → Spark/Iceberg → Dremio

> **DIT — Master 2 Ingénierie des données / Big Data**
> Module *Architectures Data Lakehouse & Ingestion de données* — Examen individuel (M. PENE)
>
> Architecture **médaillon** (Bronze → Silver → Gold) conteneurisée, ingestion
> **Apache NiFi** depuis l'API publique **FakeStoreAPI**, tables **Apache Iceberg**
> cataloguées par **Project Nessie**, stockage **MinIO**, traitements **Apache
> Spark** orchestrés par **Apache Airflow**, requêtage **Dremio OSS**, supervision
> **Prometheus + Grafana**.

---

## 1. Architecture

```
                          ┌───────────────────────────── FakeStoreAPI (https) ─────────────────────────────┐
                          │            /products          /users            /carts?limit=N                 │
                          └───────────────────────────────────┬─────────────────────────────────────────────┘
                                                              │  HTTP GET (InvokeHTTP)
                                        ┌─────────────────────▼─────────────────────┐
                                        │              APACHE NiFi                   │   (unique point d'ingestion)
                                        │  HandleHttpRequest / GenerateFlowFile      │
                                        │  → contrôle minimal (HTTP 200, non vide)   │
                                        │  → PutS3Object (path-style, endpoint minio)│
                                        └─────────────────────┬─────────────────────┘
                                                              │  JSON brut
                                        ┌─────────────────────▼─────────────────────┐
                                        │           MinIO  (S3-compatible)          │
                                        │  bucket  raw/       ← zone brute (JSON)    │
                                        │  bucket  warehouse/ ← tables Iceberg       │
                                        └───────┬───────────────────────┬───────────┘
                                                │ s3a:// (lecture)      │ s3:// S3FileIO
              ┌───────────── Apache Airflow ────┼──────────────┐        │
              │  DAG lakehouse_medallion        │              │        │
              │   trigger→wait→BRONZE→SILVER→GOLD→smoke        │        │
              │  DAG lakehouse_backfill_history (26 snapshots) │        │
              │  DAG lakehouse_scan_raw (option A, repli)      │        │
              └───────────────┬─────────────────┘              │        │
                              │ spark-submit (client mode)     │        │
                    ┌─────────▼──────────┐            ┌─────────▼────────▼─────────┐
                    │  Spark master +    │◀──────────▶│      Project Nessie        │
                    │  1 worker          │  catalogue │  version store = Postgres  │
                    │  jobs Bronze/      │  Iceberg   └─────────┬──────────────────┘
                    │  Silver/Gold (py)  │                      │  REST API v2
                    └────────────────────┘            ┌─────────▼──────────┐
                                                      │     Dremio OSS     │  SQL : Bronze/Silver/Gold,
                                                      │  source « nessie » │  jointures inter-domaines,
                                                      └────────────────────┘  agrégations, time travel

     Supervision : Prometheus  ← scrape (MinIO, Nessie, Spark, NiFi:9092)  →  Grafana (dashboard « Lakehouse »)
     Métadonnées : PostgreSQL  (bases  airflow  +  nessie)
```

Détails et **justification des choix** : [`docs/architecture.md`](docs/architecture.md).

---

## 2. Prérequis

- Docker Engine ≥ 24 + Docker Compose v2
- ~8 Go de RAM disponibles pour Docker, ~10 Go de disque
- Ports libres : `9000 9001 5432 19120 8080 8081 8082 7077 8443 9047 31010 32010 9090 3000 9092 9080`
- Python 3.9+ **sur l'hôte** uniquement pour les scripts d'aide (`nifi/scripts`,
  `dremio/scripts`) — `pip install requests`.

---

## 3. Démarrage rapide

> Procédure détaillée, étape par étape, avec vérifications et dépannage :
> **[`docs/lancement.md`](docs/lancement.md)**.

```bash
# 1. Construire les images custom (Spark + Airflow : téléchargement des jars)
make build            # ou : docker compose build

# 2. Démarrer toute la plateforme
make up               # ou : docker compose up -d

# 3. Vérifier que tout est UP (attendre ~2-3 min, Dremio/NiFi sont lents)
make health

# 4. Amorcer NiFi (Parameter Context + Process Group) puis construire le flow
make nifi-flow        # crée le squelette ; compléter les processeurs via nifi/README.md §3.3
#   … construire / importer le dataflow dans https://localhost:8443/nifi …

# 5. Configurer Dremio (source Nessie + MinIO)
make dremio-setup     # ou : python dremio/scripts/configure_dremio.py --with-views

# 6. Reconstituer l'historique 6 mois puis lancer tout le médaillon
make backfill         # DAG lakehouse_backfill_history → 26 snapshots → médaillon complet

# 7. Rejouer un cycle nominal quand on veut
make run-pipeline     # DAG lakehouse_medallion
```

### Interfaces

| Service     | URL                             | Identifiants                    |
|-------------|---------------------------------|---------------------------------|
| MinIO       | http://localhost:9001           | `minioadmin` / `minioadmin123`  |
| NiFi        | https://localhost:8443/nifi     | `admin` / `nifiAdminPass2026`   |
| Airflow     | http://localhost:8080           | `admin` / `admin`               |
| Spark master| http://localhost:8081           | —                               |
| Dremio      | http://localhost:9047           | `dremio` / `dremio123`          |
| Nessie      | http://localhost:19120          | —                               |
| Prometheus  | http://localhost:9090           | —                               |
| Grafana     | http://localhost:3000           | `admin` / `admin`               |

---

## 4. Correspondance avec le sujet

| Partie du sujet | Où c'est traité |
|---|---|
| **P1 — Infrastructure** (`docker-compose.yml`, réseau, volumes, buckets, Nessie catalogue) | [`docker-compose.yml`](docker-compose.yml), [`docker/`](docker/), [`minio/init/create-buckets.sh`](minio/init/create-buckets.sh) |
| **P2 — NiFi déployé & intégré** (réseau, HTTPS/auth, volumes persistants) | service `nifi` du compose + [`nifi/README.md`](nifi/README.md) §1-2 |
| **P3 — Flux d'ingestion NiFi → MinIO** (InvokeHTTP → PutS3Object path-style) | [`nifi/README.md`](nifi/README.md) §3, [`nifi/scripts/`](nifi/scripts/), export → `nifi/flow/` |
| **P4 — Pipeline Spark médaillon** (Bronze/Silver/Gold Iceberg, écrit intégralement) | [`spark/jobs/`](spark/jobs/) |
| **P5 — Test Dremio** (Bronze/Silver/Gold, jointure, agrégation) | [`dremio/README.md`](dremio/README.md) + [`dremio/queries/`](dremio/queries/) |
| **P6 — Orchestration Airflow** (DAG bout-en-bout, option A/B) | [`airflow/dags/`](airflow/dags/) |
| **P7 — Bonus** (supervision NiFi, idempotence, 3e domaine, Parameter Context, time travel) | Grafana + NiFi `DetectDuplicate` + domaine `carts` + Parameter Context + [`dremio/queries/06_time_travel.sql`](dremio/queries/06_time_travel.sql) |
| Contrainte **6 mois d'historique** (section 4.1) | [`airflow/dags/lakehouse_backfill_history.py`](airflow/dags/lakehouse_backfill_history.py) + colonne `snapshot_date` de bout en bout |

---

## 5. Deux domaines (min.) + un troisième — médaillon complet

| Domaine  | Source            | Bronze              | Silver                          | Gold (extraits) |
|----------|-------------------|---------------------|---------------------------------|-----------------|
| products | `/products`       | `bronze.products_raw` | `silver.products`             | `gold.dim_products`, `gold.agg_product_price_history` |
| users    | `/users`          | `bronze.users_raw`  | `silver.users`                  | `gold.dim_users` |
| carts    | `/carts?limit=N`  | `bronze.carts_raw`  | `silver.carts`, `silver.cart_items` | `gold.fact_cart_items`, `gold.agg_sales_by_*_month` |

---

## 6. Flux de données de bout en bout (démo vidéo P6)

1. `make run-pipeline` **avec** `{"run_ingestion": true}` — ou `make ingest SNAP=$(date +%F)` ;
2. NiFi interroge FakeStoreAPI → dépose le JSON dans `raw/fakestoreapi/<domaine>/snapshot_date=…/` ;
3. NiFi appelle l'API REST d'Airflow (option B) → le DAG `lakehouse_medallion` démarre ;
4. Spark : `bronze_ingest` (MERGE idempotent) → `silver_build` → `gold_build` ;
5. `smoke_test` valide la présence + volumétrie des tables ;
6. Dans Dremio : `dremio/queries/03_gold_checks.sql`, `04_join_cross_domain.sql`, `05_aggregation_gold.sql`.

---

## 7. Dépannage

| Symptôme | Piste |
|---|---|
| `make build` très long | 1er build = téléchargement Spark + jars (~500 Mo). Suivant : cache. |
| Spark job : `Class org.apache.iceberg... not found` | image pas reconstruite après modif Dockerfile → `docker compose build --no-cache spark-master` |
| Airflow : `spark-submit: not found` | idem côté image airflow |
| `wait_for_raw_zone` échoue | l'ingestion NiFi n'a pas encore tourné → `make ingest` puis relancer le DAG |
| NiFi UI inaccessible | attendre 90 s (démarrage lent) ; certificat auto-signé → accepter l'exception |
| Dremio ne voit pas les tables | pipeline pas encore exécuté, ou source Nessie mal configurée (`dremio/README.md` §5) |
| Nessie `relation "..." does not exist` au 1er boot | laisser Nessie créer son schéma (redémarre 1 fois) : `docker compose restart nessie` |

Remise à zéro complète : `make destroy` (supprime tous les volumes).

---

## 8. Arborescence

```
Exam_BD/
├── docker-compose.yml            # P1 — toute la stack
├── .env                          # tous les paramètres / identifiants (démo)
├── Makefile                      # raccourcis d'exploitation
├── docker/
│   ├── download-jars.sh          # jars Iceberg/Nessie/AWS (partagé Spark+Airflow)
│   ├── spark/Dockerfile          # image cluster Spark
│   ├── airflow/Dockerfile        # image Airflow + client Spark
│   └── postgres/init-databases.sh
├── minio/init/create-buckets.sh  # P1 — buckets raw + warehouse
├── nifi/                         # P2 + P3
│   ├── README.md                 # procédure complète (UI + API)
│   ├── scripts/                  # build_flow.py, trigger_ingest.sh, get_credentials.sh
│   └── flow/                     # export .json du flow (livrable)
├── spark/
│   ├── conf/spark-defaults.conf
│   └── jobs/                     # P4 — médaillon écrit intégralement
│       ├── common/               # SparkSession + helpers Iceberg/MERGE
│       ├── bronze/bronze_ingest.py
│       ├── silver/silver_build.py
│       ├── gold/gold_build.py
│       └── checks/smoke_test.py
├── airflow/dags/                 # P6
│   ├── lakehouse_medallion.py        # pipeline nominal (option B)
│   ├── lakehouse_scan_raw.py         # option A (repli, planifié)
│   └── lakehouse_backfill_history.py # section 4.1 — historique 6 mois
├── dremio/                       # P5
│   ├── README.md
│   ├── queries/*.sql             # Bronze/Silver/Gold, jointure, agrégation, time travel
│   └── scripts/configure_dremio.py
├── prometheus/prometheus.yml     # P1 + P7
├── grafana/                      # datasource + dashboard « Lakehouse »
├── scripts/healthcheck.sh
└── docs/
    ├── lancement.md              # guide de lancement pas à pas (+ dépannage)
    ├── architecture.md           # choix d'architecture (support oral)
    └── video_script.md           # trame de la vidéo de soutenance (15 min)
```
