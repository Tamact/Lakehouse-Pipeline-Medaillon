# Script de la vidéo de soutenance (~15 min) — trame section 6.1

Format : voix commentée + écran partagé, écran **réel** (terminal + navigateur),
seul(e) à l'écran et à l'oral. Cible 14–17 min.

| # | Durée | Séquence | À montrer à l'écran | Points à dire |
|---|-------|----------|---------------------|---------------|
| 1 | 1–2 min | **Architecture** | `docs/architecture.md` §1 (schéma ASCII) ou un schéma dessiné | Médaillon Bronze/Silver/Gold ; NiFi = unique ingestion ; Nessie = catalogue Iceberg partagé Spark/Dremio ; MinIO 2 buckets (raw vs warehouse) ; Airflow orchestre ; option B (NiFi → API Airflow) ; historique 6 mois = snapshots horodatés |
| 2 | 1–2 min | **docker-compose.yml** | parcourir le fichier : réseau `lakehouse-net`, volumes nommés, ports `.env`, `depends_on` + healthchecks, images custom Spark/Airflow (mêmes jars) | Justifier : un seul réseau, découverte par nom de service, aucun `localhost` inter-conteneurs, Postgres = 2 bases (airflow + nessie) |
| 3 | 3 min | **Flow NiFi** (UI) | `https://localhost:8443/nifi` → Process Group `FakeStoreAPI_Ingestion` : Parameter Context, `HandleHttpRequest`/`GenerateFlowFile`, `InvokeHTTP`, `RouteOnAttribute` (contrôle 200/taille), `DetectDuplicate` (idempotence), `PutS3Object` (endpoint `http://minio:9000`, **Path Style = true**), `InvokeHTTP` → Airflow. Montrer la **Provenance** d'un objet. | Justifier chaque processeur ; pourquoi Path Style pour MinIO ; planification (HTTP à la demande vs CRON) ; gestion d'erreurs (Retry + funnel) ; **aucune transformation métier** |
| 4 | 2–3 min | **Jobs Spark médaillon** | `spark/jobs/` : `common/spark_session.py` (catalogue Nessie + S3), `bronze/bronze_ingest.py` (MERGE idempotent, `snapshot_date` extrait de la clé S3), `silver/silver_build.py` (aplatissement `rating`/`address`/`products[]`, dédup, qualité), `gold/gold_build.py` (`fact_cart_items` = jointure 3 domaines, agrégats `snapshot_month`) | Contrat de chaque couche ; idempotence (MERGE vs createOrReplace) ; pourquoi l'axe temporel = `snapshot_date` |
| 5 | 2–3 min | **Dremio** | `http://localhost:9047` → source `nessie` → exécuter **en direct** : `01_bronze_checks.sql`, `02_silver_checks.sql`, `03_gold_checks.sql`, puis **`04_join_cross_domain.sql`** (produits × clients × paniers) et **`05_aggregation_gold.sql`** (GROUP BY + tendance 6 mois `LAG`) | Dremio lit les **mêmes** tables Iceberg que Spark via Nessie ; commenter les résultats (tendance, top clients) |
| 6 | 2–3 min | **Bout-en-bout en direct** | `make ingest SNAP=$(date +%F)` **ou** Airflow → Trigger `lakehouse_medallion` avec `{"run_ingestion": true}` → suivre le DAG (graph view) → voir l'objet arriver dans MinIO (`raw/…`) → jobs Spark verts → relancer une requête Gold dans Dremio qui inclut le nouveau snapshot | Montrer la latence réelle ; le déclenchement option B (log NiFi `InvokeHTTP` → dagRun créé) |
| 7 | 1 min | **Bilan** | liste : difficultés (client mode Spark, Path Style MinIO, version store Nessie), bonus réalisés (supervision NiFi Grafana, idempotence `DetectDuplicate`, 3e domaine `carts`, Parameter Context réutilisable, time travel `06_time_travel.sql`) | Ce que je referais autrement ; limites assumées (7 paniers, `us_region` heuristique) |

## Préparation avant d'enregistrer

```bash
make build && make up
make health                      # tout UP
make nifi-flow                   # + compléter/importer le flow dans l'UI
make dremio-setup                # source Nessie
make backfill                    # 6 mois d'historique + médaillon complet (laisser finir)
# vérifier une requête Gold dans Dremio
```

Garder ouverts : 1 terminal, onglets NiFi / Airflow / MinIO / Dremio / Grafana.
