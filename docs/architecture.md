# Choix d'architecture — support pour la soutenance orale

Ce document justifie les décisions de conception. Le sujet impose *les briques*,
pas *leur agencement* : c'est cet agencement qui est argumenté ici.

---

## 1. Vue logique

Flux nominal :

```
FakeStoreAPI ──(1)──▶ NiFi ──(2)──▶ MinIO raw/ ──(3)──▶ Spark Bronze ──▶ Silver ──▶ Gold ──(4)──▶ Dremio
                        │                                    ▲
                        └──(5) POST /api/v1/.../dagRuns ─────┘  (Airflow orchestre 3→4)
```

1. NiFi interroge l'API en HTTP GET (un appel par domaine).
2. NiFi dépose la **réponse JSON brute, non modifiée** dans la zone brute MinIO.
3. Spark lit le JSON, construit les 3 couches Iceberg via le catalogue Nessie.
4. Dremio lit **les mêmes tables Iceberg** (catalogue Nessie + stockage MinIO).
5. En fin d'ingestion, NiFi déclenche le DAG Airflow (option B).

---

## 2. Réseau, découverte de services, ports

- **Un seul réseau bridge** `lakehouse-net`. Tous les services se joignent par
  leur **nom de service** (`minio`, `nessie`, `spark-master`, `airflow-webserver`…).
- **Aucun `localhost`** dans les configurations inter-conteneurs : l'endpoint S3
  utilisé par NiFi, Spark et Dremio est `http://minio:9000`.
- Les ports hôte sont tous distincts (cf. `.env`) ; Spark master UI est déplacé
  sur `8081` pour ne pas entrer en conflit avec Airflow (`8080`).

---

## 3. Stockage — MinIO : séparation stricte des zones

| Bucket        | Rôle | Écrit par | Lu par |
|---------------|------|-----------|--------|
| `raw`         | **zone brute** : JSON FakeStoreAPI verbatim | NiFi | Spark (Bronze) |
| `warehouse`   | **warehouse Iceberg** : données + métadonnées des tables | Spark | Spark, Dremio |

- Séparation par **bucket** (et non par simple préfixe) → permissions,
  cycle de vie et supervision indépendants ; élimine tout risque de mélange
  entre données brutes et tables gérées.
- **Convention de clé** de la zone brute :
  ```
  raw/fakestoreapi/<domaine>/snapshot_date=YYYY-MM-DD/ingestion_ts=<epoch_ms>/<domaine>_<date>_<ts>.json
  ```
  Le partitionnement logique par `snapshot_date` est **porté par la clé S3**
  → Bronze reconstruit la dimension temporelle sans dépendre de NiFi
  (regex sur `input_file_name()`), ce qui découple les deux briques.
- Versioning activé sur `raw` : trace de chaque re-dépôt d'un même objet.

---

## 4. Ingestion — Apache NiFi

- **NiFi 1.28** (et non 2.x) : le mode *single-user* (HTTPS + login/mot de passe
  fixes via variables d'environnement) donne un accès **reproductible** pour la
  correction, tout en respectant la contrainte « HTTPS + auth par défaut ».
  La procédure « identifiants générés + `docker logs` » est documentée et
  scriptée (`nifi/scripts/get_credentials.sh`) pour répondre au point d'attention
  du sujet.
- **Un Process Group + un Parameter Context** : toutes les valeurs (URL API,
  endpoint/clefs MinIO, préfixe, URL Airflow) sont des paramètres → le flow est
  **réutilisable pour une autre API** sans le modifier (bonus P7).
- **NiFi ne fait aucune transformation métier** : seulement
  `InvokeHTTP` (collecte) → contrôle (`RouteOnAttribute` sur `status.code == 200`,
  taille non nulle, déduplication) → `PutS3Object` (dépôt). Aucune jointure,
  aucun calcul, aucun agrégat.
- **`PutS3Object` + MinIO** : `Endpoint Override URL = http://minio:9000` et
  **`Use Path Style Access = true`** (obligatoire — MinIO n'implémente pas le
  *virtual-hosted style* par défaut).
- **Deux déclencheurs** :
  - `HandleHttpRequest` (port 9080, `/ingest?snapshot_date=&domains=&cart_limit=`)
    → mode **nominal**, piloté par Airflow (backfill + runs à la demande) ;
  - `GenerateFlowFile` en CRON quotidien → mode **autonome** (snapshot du jour).
- **Gestion des erreurs** : relation `Retry` d'`InvokeHTTP` avec *penalty* pour
  les 5xx ; `Failure` routé vers un funnel `ingestion-errors` + `LogAttribute` ;
  backpressure 10k FlowFiles / 1 Go ; provenance NiFi conservée sur volume.
- **Idempotence** (bonus) : `CryptographicHashContent` (SHA-256) +
  `DetectDuplicate` sur `${domaine}|${snapshot_date}|${hash}` via
  `DistributedMapCacheServer` → un ré-appel du même snapshot ne redépose rien.

---

## 5. Reconstitution d'un historique ~6 mois (section 4.1 du sujet)

Problème : l'API ne renvoie que l'état courant, sans dimension temporelle.

Solution retenue — **snapshots hebdomadaires horodatés + fenêtre glissante sur les paniers** :

1. Le DAG `lakehouse_backfill_history` génère **26 dates** (1 par semaine sur
   6 mois) et, pour chacune, appelle NiFi avec `snapshot_date=<date>`.
2. Chaque exécution est **systématiquement horodatée** : `snapshot_date` +
   `ingestion_ts` sont inscrits dans la clé S3, puis propagés **inchangés**
   jusqu'à la couche Gold.
3. `products` et `users` (dimensions) sont ingérés en entier à chaque snapshot :
   leur « état connu » est le même à chaque date.
4. `carts` : on fait **croître le paramètre `limit`** de l'API au fil des
   semaines (`GET /carts?limit=N&sort=desc`). On **simule ainsi l'arrivée
   progressive des commandes** — technique explicitement autorisée par le sujet
   (« backfill simulé avec des dates réparties sur 6 mois »).
5. **Axe temporel d'analyse = `snapshot_date`** (timeline reconstituée), et non
   la `date` native des paniers (figée en 2019-2020 dans l'API). La couche Gold
   agrège par `snapshot_month` → tendances, évolutions, variations mois-à-mois
   (fonctions fenêtre `LAG`).

Conséquence : `gold.agg_sales_by_category_month`, `agg_sales_by_region_month` et
`agg_product_price_history` présentent 6 mois de points analysables.

---

## 6. Traitements Spark — le médaillon

### 6.1 Cluster
- Image `spark:3.5.1-python3` (officielle) + jars ajoutés : image **identique**
  pour le master, le worker **et** le driver Airflow → classpath cohérent.
- 1 master + 1 worker (2 cœurs / 3 Go) : suffisant pour la volumétrie
  FakeStoreAPI ; le code passe à l'échelle sans modification (pas de `collect`,
  pas de logique mono-nœud).

### 6.2 Catalogue & format
- **Iceberg** via `SparkCatalog` + `catalog-impl = NessieCatalog`
  (`uri = http://nessie:19120/api/v2`, `ref = main`).
- Warehouse dans MinIO via **`S3FileIO`** (`io-impl`), endpoint + clés passés en
  `spark.sql.catalog.nessie.s3.*` → **propagés automatiquement aux executors**
  (contrairement à des variables d'environnement). Aucun secret dans l'image
  worker.
- Lecture de la zone brute via le connecteur **`s3a://`** (hadoop-aws) —
  `path.style.access = true`, `ssl.enabled = false`.

### 6.3 Règles Bronze → Silver → Gold (conçues ici, non imposées)

| Couche | Contrat |
|--------|---------|
| **Bronze** | Fidélité totale à la source. Colonnes source **inchangées** (structures imbriquées conservées) + colonnes techniques (`_source_file`, `snapshot_date`, `ingestion_ts`, `_ingested_at`, `_batch_id`, `_record_hash`). Partition `snapshot_date`. **Append-only**, MERGE idempotent sur `(_record_hash, snapshot_date)`. |
| **Silver** | Typage explicite ; **aplatissement** des structures (`rating`, `name`, `address`, `products[]` → `cart_items`) ; qualité (ids non nuls, `price ≥ 0`, `quantity > 0`, e-mail valide) ; **déduplication** : 1 version par `(clé métier, snapshot_date)` = ingestion la plus récente ; enrichissements déterministes documentés (`price_band`, `us_region` dérivée du ZIP). |
| **Gold** | Modèle décisionnel : `dim_products`, `dim_users` (état courant + fenêtre de vie), `fact_cart_items` (**jointure carts × products × users**, mesure `line_revenue`), agrégats temporels par mois/catégorie/zone, historique de prix avec variation `LAG`. Tables **reconstruites** à chaque run (`createOrReplace`). |

### 6.4 Idempotence & rejouabilité
- Bronze : MERGE `WHEN NOT MATCHED` uniquement (immuable).
- Silver : MERGE `WHEN MATCHED UPDATE * / WHEN NOT MATCHED INSERT *` sur la clé
  métier + `snapshot_date` → rejouer un run corrige sans dupliquer.
- Gold : `createOrReplace` → toujours cohérent avec Silver.
- Un même `snapshot_date` peut donc être ré-ingéré et retraité sans effet de bord.

---

## 7. Orchestration — Apache Airflow

- **`LocalExecutor`** + backend PostgreSQL : simple, suffisant, sans broker.
- **`SparkSubmitOperator` en `client mode`** vers `spark://spark-master:7077` :
  le driver tourne dans `airflow-scheduler` (qui embarque le client Spark 3.5.1
  et les mêmes jars). `spark.driver.host = airflow-scheduler` pour que les
  executors du worker joignent le driver.
  *Alternative écartée* : `deploy-mode cluster` — non supporté pour les
  applications **PySpark** en Spark Standalone.
- **DAG `lakehouse_medallion`** : `trigger_nifi? → wait_for_raw_zone →
  bronze[×3] → silver[×3] → gold → smoke_test`. `schedule=None` : déclenché par
  événement.
- **Déclenchement (P6)** :
  - **Option B — événementiel (retenue comme nominale)** : NiFi `InvokeHTTP`
    `POST /api/v1/dags/lakehouse_medallion/dagRuns` (auth *basic*, utilisateur
    `nifi_trigger`) en fin d'ingestion.
  - **Option A — planifiée (repli)** : DAG `lakehouse_scan_raw` (cron `*/15`)
    sonde MinIO (boto3), compare à une `Variable` Airflow « dernière clé vue »,
    et `TriggerDagRunOperator` si nouveauté.
  Justification : l'option B minimise la latence et le gaspillage (pas de
  polling) ; l'option A est conservée pour les environnements où NiFi ne peut
  pas appeler Airflow.
- **DAG `lakehouse_backfill_history`** : provisioning initial des 6 mois, puis
  `TriggerDagRunOperator(wait_for_completion=True)` sur le médaillon.

---

## 8. Requêtage — Dremio OSS

- Source unique **`nessie`** de type *Nessie* : `nessieEndpoint =
  http://nessie:19120/api/v2`, stockage MinIO (`awsRootPath = warehouse`,
  `fs.s3a.path.style.access = true`, `dremio.s3.compat = true`, SSL off).
- Dremio et Spark partagent **le même catalogue et le même stockage** : aucune
  copie, cohérence immédiate.
- Tests explicites (P5) : `dremio/queries/01..05` — lecture des 3 couches,
  **jointure inter-domaines** (produits × clients × paniers), **agrégations**
  (GROUP BY, moyennes, tendance 6 mois avec `LAG`).
- Bonus : VDS `lakehouse.vds_sales_trend`, `vds_customer_360` ; `06_time_travel`
  (Iceberg `AT SNAPSHOT/TIMESTAMP`, Nessie `AT BRANCH`, `ADD COLUMNS`).

---

## 9. Métadonnées — PostgreSQL

Une seule instance, deux bases créées à l'init (`docker/postgres/init-databases.sh`) :
- `airflow` — métastore Airflow ;
- `nessie` — *version store* JDBC de Nessie (les commits du catalogue Iceberg y
  sont persistés → survivent à un redémarrage).

---

## 10. Supervision — Prometheus + Grafana

- Prometheus scrape : lui-même, **MinIO** (`/minio/v2/metrics/cluster`, auth
  publique), **Nessie** (`/q/metrics` Micrometer), **Spark** master/worker,
  **NiFi** (`:9092`, *PrometheusReportingTask* — bonus P7).
- Grafana provisionné (datasource + dashboard `Lakehouse - Vue d'ensemble`) :
  état des services, files d'attente NiFi, débit lecture/écriture des
  processeurs, objets par bucket MinIO, débit requêtes Nessie.

---

## 11. Limites connues / pistes

- FakeStoreAPI expose ~7 paniers : la fenêtre `limit` croissante donne une
  tendance en escalier plutôt que lisse — suffisant pour démontrer l'analyse
  temporelle, mentionné à l'oral.
- `us_region` est une heuristique sur le 1er chiffre du code postal (assumée).
- Pas d'authentification Nessie/Dremio (démo) — en production : jetons + TLS.
- `LocalExecutor` mono-nœud : pour de la charge réelle, passer à
  `CeleryExecutor`/`KubernetesExecutor`.
