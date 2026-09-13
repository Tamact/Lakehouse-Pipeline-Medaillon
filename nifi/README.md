# Apache NiFi — ingestion FakeStoreAPI → MinIO (zone brute)

> Parties 2 et 3 du sujet. NiFi est **l'unique point d'ingestion** : il interroge
> l'API, fait un **contrôle minimal**, et **dépose le JSON brut** dans MinIO.
> **Aucune transformation métier** (jointure / calcul / agrégat) n'est faite ici.

---

## 1. Accès à l'interface (Partie 2)

NiFi 1.28 impose **HTTPS + authentification**. Deux options existent ; on retient
la **1re** (reproductibilité de la correction), la 2e est expliquée pour le sujet.

### Option retenue — « single-user » à identifiants fixes
Défini dans `docker-compose.yml` via les variables d'environnement :

```yaml
SINGLE_USER_CREDENTIALS_USERNAME: ${NIFI_USERNAME}   # admin
SINGLE_USER_CREDENTIALS_PASSWORD: ${NIFI_PASSWORD}   # nifiAdminPass2026 (>= 12 car.)
```

- URL : **https://localhost:8443/nifi** (certificat auto-signé → accepter l'exception)
- Identifiants : `admin` / `nifiAdminPass2026` (voir `.env`)

### Option « identifiants générés » (à montrer dans la vidéo)
Si on ne fixe pas `SINGLE_USER_CREDENTIALS_*`, NiFi génère un compte au 1er
démarrage et l'écrit dans les logs :

```bash
docker logs lakehouse-nifi 2>&1 | grep -E "Generated Username|Generated Password"
# ou forcer un mot de passe connu :
docker exec -it lakehouse-nifi ./bin/nifi.sh set-single-user-credentials admin nifiAdminPass2026
```

`nifi/scripts/get_credentials.sh` automatise cette récupération.

### Persistance (Partie 2.3)
Tous les *repositories* NiFi sont sur des volumes nommés (voir `docker-compose.yml`,
service `nifi`) : `conf`, `state`, `flowfile_repository`, `content_repository`,
`provenance_repository`, `database_repository`, `logs`. Le flow survit à un
`docker compose restart`.

---

## 2. Résolution du nom `minio` (Partie 2.1)

NiFi est sur le réseau `lakehouse-net`. Depuis un terminal NiFi :

```bash
docker exec lakehouse-nifi sh -c "getent hosts minio && curl -s -o /dev/null -w '%{http_code}\n' http://minio:9000/minio/health/live"
```

L'endpoint S3 utilisé par `PutS3Object` est donc **`http://minio:9000`**, jamais
`localhost`.

---

## 3. Dataflow d'ingestion (Partie 3)

Le flow est contenu dans **un Process Group** `FakeStoreAPI_Ingestion`, piloté par
un **Parameter Context** (→ réutilisable pour une autre API, bonus Partie 7).

### 3.1 Parameter Context `fakestore-ingestion`

| Paramètre            | Valeur                                                              | Sensible |
|----------------------|-------------------------------------------------------------------|:--------:|
| `api.base.url`       | `https://fakestoreapi.com`                                        |          |
| `s3.endpoint`        | `http://minio:9000`                                               |          |
| `s3.bucket.raw`      | `raw`                                                             |          |
| `s3.access.key`      | `minioadmin`                                                     |          |
| `s3.secret.key`      | `minioadmin123`                                                  |   oui    |
| `s3.region`          | `us-east-1`                                                      |          |
| `raw.prefix`         | `fakestoreapi`                                                   |          |
| `airflow.dagrun.url` | `http://airflow-webserver:8080/api/v1/dags/lakehouse_medallion/dagRuns` | |
| `airflow.auth.basic` | base64(`nifi_trigger:nifi_trigger_123`) = `bmlmaV90cmlnZ2VyOm5pZmlfdHJpZ2dlcl8xMjM=` | oui |

### 3.2 Controller Services (dans le Process Group)

| Service                              | Rôle                                          |
|--------------------------------------|-----------------------------------------------|
| `StandardHttpContextMap`             | requis par `HandleHttpRequest/Response`       |
| `DistributedMapCacheServer` (:4557)  | serveur de cache (idempotence, bonus)         |
| `DistributedMapCacheClientService`   | client → `DetectDuplicate` (Server Hostname `localhost`) |

> `PutS3Object` reçoit les clés directement via paramètres → pas de
> `AWSCredentialsProviderControllerService` nécessaire.

### 3.3 Graphe des processeurs

```
 (A) GenerateFlowFile              (B) HandleHttpRequest  (port 9080, /ingest)
     run schedule: 0 0 6 * * ?          |  attributs http.query.param.*
     notify.airflow = true              |
            |                           +--> HandleHttpResponse (202 Accepted)   [clone]
            |                           |
            +-----------+---------------+
                        v
             UpdateAttribute  « set-ingestion-context »
               snapshot_date = ${http.query.param.snapshot_date:isEmpty()
                                  :ifElse(${now():format("yyyy-MM-dd")},
                                          ${http.query.param.snapshot_date})}
               domains       = ${http.query.param.domains:isEmpty()
                                  :ifElse("products,users,carts",
                                          ${http.query.param.domains})}
               cart.limit    = ${http.query.param.cart_limit:isEmpty():ifElse("0",
                                          ${http.query.param.cart_limit})}
               ingestion.ts  = ${now():toNumber()}
                        v
             ReplaceText   (Replacement Value = ${domains:replace(",","\n")},
                            Replacement Strategy = Always Replace)
                        v
             SplitText     (Line Split Count = 1)          --> 1 FlowFile / domaine
                        v
             ExtractText   (domain = (\S+))                --> attribut "domain"
                        v
             UpdateAttribute  « build-request »
               api.path     = ${domain:equals("products"):ifElse("/products",
                                ${domain:equals("users"):ifElse("/users","/carts")})}
               request.url  = #{api.base.url}${api.path}${domain:equals("carts")
                                :ifElse(${cart.limit:gt(0)
                                  :ifElse("?limit=${cart.limit}&sort=desc","")},"")}
               filename     = ${domain}_${snapshot_date}_${ingestion.ts}.json
               s3.object.key= #{raw.prefix}/${domain}/snapshot_date=${snapshot_date}/ingestion_ts=${ingestion.ts}/${filename}
                        v
             InvokeHTTP    (GET ${request.url}, "Response Body" -> FlowFile)
                        v
             RouteOnAttribute  « check-http-200 »
               ok = ${invokehttp.status.code:equals(200)}      (sinon -> LogAttribute -> funnel échec)
                        v  (ok)
             CryptographicHashContent  (SHA-256 -> attribut content_SHA-256)     [BONUS idempotence]
                        v
             DetectDuplicate  (id = ${domain}|${snapshot_date}|${content_SHA-256},
                               cache = DistributedMapCacheClientService,
                               Age Off = 30 days)
               duplicate     -> LogAttribute -> (auto-terminate)
               non-duplicate v
             PutS3Object
               Bucket                = #{s3.bucket.raw}
               Object Key            = ${s3.object.key}
               Endpoint Override URL = #{s3.endpoint}
               Access Key ID         = #{s3.access.key}
               Secret Access Key     = #{s3.secret.key}
               Region                = #{s3.region}
               Use Path Style Access = true                # OBLIGATOIRE pour MinIO
                        v (success)
             MergeContent  (Min Entries = 1, Max Bin Age = 20 s)   # regroupe les 3 domaines
                        v
             RouteOnAttribute  « notify? »   ok = ${notify.airflow:equals("true")}
                        v (ok)                                     # OPTION B (Partie 6)
             InvokeHTTP    POST #{airflow.dagrun.url}
               Body        = {"conf":{"snapshot_date":"${snapshot_date}","triggered_by":"nifi"}}
               Headers     : Content-Type=application/json
                             Authorization=Basic #{airflow.auth.basic}
```

**Contrôle minimal effectué (Partie 3, « contrôle »)** : code HTTP == 200,
`Content-Type` JSON, contenu non vide (`${fileSize:gt(2)}`), et déduplication.
Aucune règle métier.

### 3.4 Planification des appels
- **Mode nominal** : `HandleHttpRequest` (port 9080) — l'ingestion est déclenchée
  *à la demande* par Airflow (DAG `lakehouse_backfill_history` pour les 26
  snapshots, ou `lakehouse_medallion` avec `run_ingestion=true`).
- **Mode autonome** : `GenerateFlowFile` en CRON quotidien `0 0 6 * * ?`
  (snapshot du jour). Désactivé par défaut, activable pour une démo « vivante ».

### 3.5 Gestion des erreurs
- `InvokeHTTP` : relations `Retry` (5xx) → reboucle avec *Penalty* 30 s ;
  `No Retry` / `Failure` (4xx, timeout) → `LogAttribute` (niveau WARN) → funnel
  `ingestion-errors` (visible dans la vidéo).
- `PutS3Object` : `failure` → `LogAttribute` → funnel ; `success` → suite.
- Backpressure : 10 000 FlowFiles / 1 GB sur chaque connexion.
- La *Provenance* NiFi trace chaque objet déposé (démo : clic droit → View
  Data Provenance).

---

## 4. Construire le flow

### 4.1 Automatiquement (recommandé pour la reproductibilité)
```bash
# la stack doit être démarrée
python nifi/scripts/build_flow.py            # crée Parameter Context + Process Group + processeurs
```
Le script est **idempotent** : relancé, il ne recrée pas ce qui existe.
Il ne câble PAS les 2 bonus (DetectDuplicate, PrometheusReportingTask) — à
ajouter à la main (section 3.3 / 5), c'est rapide et se montre bien en vidéo.

### 4.2 Manuellement
Suivre le graphe de la section 3.3, processeur par processeur, avec les valeurs
de propriétés indiquées. C'est cette version que l'on commente dans la vidéo.

### 4.3 Export du flow (livrable obligatoire)
Une fois le flow au point :
`clic droit sur le Process Group → Download flow definition → without external services`
→ enregistrer sous **`nifi/flow/FakeStoreAPI_Ingestion.json`** et committer.

---

## 5. Supervision NiFi (BONUS Partie 7)

1. Menu ≡ → *Controller Settings* → *Reporting Tasks* → **PrometheusReportingTask**
   - *Metrics Endpoint Port* : `9092`
   - *Instance ID* : `lakehouse-nifi`
   - *Send JVM metrics* : `true`
   - démarrer la tâche.
2. `prometheus/prometheus.yml` scrape déjà `nifi:9092` (job `nifi`).
3. Le dashboard Grafana **Lakehouse - Vue d'ensemble** contient 2 panneaux NiFi
   (`nifi_amount_flowfiles_queued`, `rate(nifi_amount_bytes_read/written)`).

---

## 6. Déclencher / tester l'ingestion

```bash
# snapshot du jour, 3 domaines
bash nifi/scripts/trigger_ingest.sh

# snapshot daté + limite de paniers (utilisé par le backfill)
curl -k -X POST "https://localhost:8443/..."   # via NiFi UI, ou :
curl -X POST "http://localhost:9080/ingest?snapshot_date=2026-04-15&domains=products,users,carts&cart_limit=3"

# vérifier le dépôt dans MinIO
docker exec lakehouse-minio-init sh -c "mc ls -r local/raw/fakestoreapi/ | head"
```
