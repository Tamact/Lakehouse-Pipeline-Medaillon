# Guide de lancement de la plateforme — pas à pas

> Procédure complète pour démarrer la plateforme Data Lakehouse depuis zéro,
> jusqu'à des tables Gold interrogeables dans Dremio. Suivre les étapes **dans
> l'ordre** ; chaque étape indique quoi vérifier avant de passer à la suivante.
>
> Toutes les commandes se lancent depuis la racine du projet :
> `C:\Users\Smart Business\Desktop\pp\Exam_BD`

---

## 0. Prérequis

| Élément | Vérification |
|---|---|
| Docker Engine ≥ 24 + Compose v2 | `docker version` puis `docker compose version` |
| Docker Desktop **démarré** | `docker info` répond sans erreur (moteur Linux) |
| RAM allouée à Docker ≥ 8 Go | Docker Desktop → Settings → Resources |
| Disque libre ≥ 10 Go | 1er build ≈ 3 Go d'images + volumes |
| Python 3.9+ sur l'hôte | `python --version` — uniquement pour `nifi/scripts` et `dremio/scripts` |
| Lib Python `requests` | `pip install requests` |
| Ports hôte libres | `9000 9001 5432 19120 7077 8080 8081 8082 8443 9047 31010 32010 9090 3000 9092 9080` |

Vérifier rapidement les ports (PowerShell) :
```powershell
9000,9001,5432,19120,7077,8080,8081,8082,8443,9047,31010,32010,9090,3000,9092,9080 |
  ForEach-Object { "{0,-6} {1}" -f $_, ((Test-NetConnection -ComputerName localhost -Port $_ -WarningAction SilentlyContinue).TcpTestSucceeded) }
# TcpTestSucceeded = False  => port libre (attendu)
```

Fichier de configuration : **`.env`** (déjà fourni, identifiants de démo).
Le modifier si un port est déjà pris sur la machine.

---

## 1. Construire les images custom

Deux images sont construites localement (les autres sont tirées de registries) :

| Image | Contenu |
|---|---|
| `lakehouse/spark:3.5.1` | `spark:3.5.1-python3` + jars Iceberg 1.6.1 / Nessie 0.99.0 / hadoop-aws 3.3.4 + `requests`, `boto3` |
| `lakehouse/airflow:2.9.3` | `apache/airflow:2.9.3` + JRE 17 + client Spark 3.5.1 + mêmes jars + providers Spark/Amazon |

```bash
docker compose build
```

- Durée : **5 à 10 min au 1er lancement** (téléchargement de Spark ~400 Mo + jars ~350 Mo, deux fois).
- Relancer plus tard = quasi instantané (cache Docker).
- En cas de coupure réseau pendant les `curl` de jars : `docker compose build --no-cache spark-master airflow-webserver`.

**Vérifier :**
```bash
docker images | grep lakehouse
# lakehouse/spark     3.5.1
# lakehouse/airflow   2.9.3
```

---

## 2. Démarrer la plateforme

```bash
docker compose up -d
```

Ordre de démarrage géré par `depends_on` + healthchecks :

```
postgres ─┬─▶ nessie ────────────────┐
          └─▶ (bases airflow/nessie)  │
minio ─▶ minio-init (crée buckets) ──┼─▶ spark-master ─▶ spark-worker
                                      ├─▶ dremio
                                      └─▶ airflow-init ─▶ airflow-webserver ─▶ airflow-scheduler
nifi           (indépendant)
prometheus ─▶ grafana
```

- `minio-init` et `airflow-init` sont des conteneurs **éphémères** : ils
  s'exécutent une fois puis sortent en `Exited (0)` — c'est normal.
- **NiFi** met ~90-120 s à répondre. **Dremio** ~60-90 s.

**Vérifier (attendre ~2-3 min) :**
```bash
docker compose ps
```
Attendu : tous les services `running`/`healthy`, sauf `lakehouse-minio-init` et
`lakehouse-airflow-init` en `exited (0)`.

```bash
bash scripts/healthcheck.sh
```
Attendu : `[ OK ]` sur MinIO, Nessie, Spark, Airflow, NiFi, Dremio, Prometheus, Grafana.

### Interfaces web

| Service | URL | Identifiants |
|---|---|---|
| MinIO Console | http://localhost:9001 | `minioadmin` / `minioadmin123` |
| NiFi | https://localhost:8443/nifi | `admin` / `nifiAdminPass2026` |
| Airflow | http://localhost:8080 | `admin` / `admin` |
| Spark master | http://localhost:8081 | — |
| Dremio | http://localhost:9047 | `dremio` / `dremio123` |
| Nessie | http://localhost:19120 | — |
| Prometheus | http://localhost:9090 | — |
| Grafana | http://localhost:3000 | `admin` / `admin` |

> NiFi : certificat auto-signé → accepter l'exception de sécurité du navigateur.

---

## 3. Vérifier les fondations (buckets + catalogue)

**Buckets MinIO :**
```bash
docker compose exec minio-init sh -c \
  "mc alias set l http://minio:9000 minioadmin minioadmin123 && mc ls l"
```
> `minio-init` étant sorti, si la commande échoue : le lancer ponctuellement —
> ```bash
> docker run --rm --network lakehouse-net minio/mc:RELEASE.2024-08-17T11-33-50Z \
>   sh -c "mc alias set l http://minio:9000 minioadmin minioadmin123 && mc ls l"
> ```
Attendu : `raw/` et `warehouse/`.

**Catalogue Nessie :**
```bash
curl -s http://localhost:19120/api/v2/config | python -m json.tool
```
Attendu : JSON avec `"defaultBranch": "main"`.

---

## 4. Apache NiFi — flow d'ingestion (Parties 2 & 3)

### 4.1 Amorcer le Parameter Context + le Process Group
```bash
pip install requests          # si pas déjà fait
python nifi/scripts/build_flow.py
```
Attendu : `+ Parameter Context 'fakestore-ingestion' créé`, `+ Process Group 'FakeStoreAPI_Ingestion' créé`, `+ Process Group lié au Parameter Context`.

### 4.2 Construire le dataflow dans l'UI
Ouvrir https://localhost:8443/nifi → entrer dans le Process Group
`FakeStoreAPI_Ingestion` → créer les processeurs en suivant
**`nifi/README.md` section 3.3** (graphe complet + valeurs de chaque propriété).

Points de contrôle :
- `PutS3Object` : `Endpoint Override URL = http://minio:9000`, **`Use Path Style Access = true`**.
- `HandleHttpRequest` : `Listening Port = 9080`, `Allowed Paths = /ingest`.
- Démarrer tous les processeurs (bouton ▶ sur le Process Group).

### 4.3 Test d'ingestion manuel
```bash
bash nifi/scripts/trigger_ingest.sh                 # snapshot du jour, 3 domaines
```
**Vérifier le dépôt dans la zone brute :**
```bash
docker run --rm --network lakehouse-net minio/mc:RELEASE.2024-08-17T11-33-50Z \
  sh -c "mc alias set l http://minio:9000 minioadmin minioadmin123 && mc ls -r l/raw/fakestoreapi/"
```
Attendu : des objets
`raw/fakestoreapi/<domaine>/snapshot_date=YYYY-MM-DD/ingestion_ts=.../<domaine>_....json`.

### 4.4 Exporter le flow (livrable obligatoire)
Clic droit sur le Process Group → **Download flow definition** → *without external
services* → enregistrer sous **`nifi/flow/FakeStoreAPI_Ingestion.json`**.

---

## 5. Dremio — source Nessie (Partie 5)

```bash
python dremio/scripts/configure_dremio.py --with-views
```
Attendu : `+ Source Nessie 'nessie' créée`, `+ VDS lakehouse.vds_sales_trend`,
`+ VDS lakehouse.vds_customer_360`.

Si le script échoue sur la création de la source : la créer manuellement via
l'UI (http://localhost:9047), voir `dremio/README.md` section 2.

**Vérifier :** dans Dremio, le panneau de gauche montre la source `nessie`
(les namespaces `bronze`/`silver`/`gold` apparaîtront après le 1er run du pipeline).

---

## 6. Charger les données — 2 scénarios

### 6.1 Historique 6 mois (à faire une fois, section 4.1 du sujet)
```bash
docker compose exec airflow-scheduler airflow dags unpause lakehouse_medallion
docker compose exec airflow-scheduler airflow dags unpause lakehouse_backfill_history
docker compose exec airflow-scheduler airflow dags trigger  lakehouse_backfill_history
```
Ce DAG :
1. demande à NiFi **26 snapshots hebdomadaires** horodatés (fenêtre `limit`
   croissante sur `/carts`) ;
2. déclenche ensuite `lakehouse_medallion` **et attend sa fin**.

Suivre l'avancement : Airflow UI → DAG `lakehouse_backfill_history` → *Graph*.
Durée : ~5-15 min selon la machine.

### 6.2 Cycle nominal (rejouable à volonté)
```bash
docker compose exec airflow-scheduler airflow dags trigger lakehouse_medallion \
  --conf '{"run_ingestion": true, "snapshot_date": "'"$(date +%F)"'"}'
```
Enchaîne : `trigger NiFi → wait_for_raw_zone → bronze[×3] → silver[×3] → gold → smoke_test`.

> **Option B (événementiel)** : une fois les processeurs NiFi complets (étape 4.2),
> c'est NiFi qui appelle `POST /api/v1/dags/lakehouse_medallion/dagRuns` en fin
> d'ingestion — plus besoin de `trigger` manuel.
> **Option A (planifié)** : `airflow dags unpause lakehouse_scan_raw` active un
> scan MinIO toutes les 15 min qui déclenche le médaillon si nouveauté.

---

## 7. Vérifier le résultat

### 7.1 Smoke test Spark
```bash
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 /opt/spark/jobs/checks/smoke_test.py
```
Attendu : toutes les tables `bronze.*`, `silver.*`, `gold.*` présentes et peuplées
+ un aperçu de `gold.agg_sales_by_category_month`.

### 7.2 Requêtes Dremio (Partie 5)
Dans l'éditeur SQL de Dremio (http://localhost:9047), exécuter dans l'ordre :

| Fichier | Vérifie |
|---|---|
| `dremio/queries/01_bronze_checks.sql` | lecture Bronze (3 domaines) |
| `dremio/queries/02_silver_checks.sql` | Silver typé/aplati + contrôles qualité |
| `dremio/queries/03_gold_checks.sql` | dimensions + faits + agrégats |
| `dremio/queries/04_join_cross_domain.sql` | **jointure produits × clients × paniers** |
| `dremio/queries/05_aggregation_gold.sql` | **agrégations + tendance 6 mois** |
| `dremio/queries/06_time_travel.sql` | bonus Iceberg/Nessie |

### 7.3 Supervision
- Prometheus : http://localhost:9090/targets → cibles `minio`, `nessie`, `spark` `UP`
  (`nifi` devient `UP` après activation du *PrometheusReportingTask*, cf. `nifi/README.md` §5).
- Grafana : http://localhost:3000 → dashboard **Lakehouse - Vue d'ensemble**.

---

## 8. Exploitation courante

| Besoin | Commande |
|---|---|
| État des conteneurs | `docker compose ps` |
| Logs d'un service | `docker compose logs -f nifi` (ou `spark-worker`, `airflow-scheduler`…) |
| Relancer un service | `docker compose restart nessie` |
| Ingestion ponctuelle datée | `bash nifi/scripts/trigger_ingest.sh 2026-05-01 products,users,carts 4` |
| Rejouer le médaillon | `docker compose exec airflow-scheduler airflow dags trigger lakehouse_medallion` |
| Lancer un job Spark seul | `docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/jobs/silver/silver_build.py --domain products` |
| Shell dans Spark | `docker compose exec spark-master bash` |
| Lister la zone brute | voir étape 4.3 |

Les mêmes actions sont disponibles via `make` : `make help`.

---

## 9. Arrêt / redémarrage / remise à zéro

```bash
docker compose stop          # pause (reprise avec `docker compose start`)
docker compose down          # arrête et supprime les conteneurs, GARDE les volumes (données conservées)
docker compose down -v       # + supprime tous les volumes = remise à zéro TOTALE
```

Après `down` simple, un `docker compose up -d` repart avec les buckets, les tables
Iceberg et le flow NiFi intacts.

---

## 10. Dépannage

| Symptôme | Cause probable | Correctif |
|---|---|---|
| `docker info` : `cannot connect` | Docker Desktop non démarré | lancer Docker Desktop, attendre le moteur Linux |
| `port is already allocated` au `up` | port hôte occupé | changer la valeur dans `.env` puis `docker compose up -d` |
| `lakehouse-nessie` redémarre en boucle | schéma JDBC pas encore créé | `docker compose restart nessie` (1 fois) ; vérifier logs Postgres |
| Job Spark : `ClassNotFoundException org.apache.iceberg…` | image Spark pas reconstruite | `docker compose build --no-cache spark-master spark-worker && docker compose up -d` |
| Airflow : `spark-submit: not found` | image Airflow pas reconstruite | `docker compose build --no-cache airflow-webserver airflow-scheduler` |
| Tâche `wait_for_raw_zone` en échec | zone brute vide (NiFi pas lancé) | exécuter l'étape 4.3, puis re-trigger le DAG |
| Spark : `Path does not exist: s3a://raw/...` | idem (aucun `.json` déposé) | idem |
| Dremio ne voit aucune table | pipeline pas encore exécuté | lancer l'étape 6 ; rafraîchir la source Nessie dans Dremio |
| Dremio : `Access Denied` / `NoSuchKey` sur MinIO | `path.style.access` absent | recréer la source avec `fs.s3a.path.style.access=true` (cf. `dremio/README.md` §5) |
| NiFi UI : `ERR_CONNECTION_REFUSED` | démarrage lent (< 2 min) | attendre ; `docker compose logs -f nifi` jusqu'à `NiFi has started` |
| Driver Spark injoignable depuis le worker | `spark.driver.host` incorrect | vérifier `SPARK_DRIVER_HOST=airflow-scheduler` (service `airflow-scheduler` du compose) |
| `airflow dags trigger` : `DAG not found` | scheduler pas encore parsé les DAGs | attendre 30 s ; `docker compose logs airflow-scheduler` |

Diagnostic réseau inter-conteneurs :
```bash
docker compose exec nifi sh -c "getent hosts minio; getent hosts nessie"
docker compose exec spark-master sh -c "curl -sf http://nessie:19120/q/health/live && echo OK"
```
