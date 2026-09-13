# Dremio OSS — moteur de requête SQL du lakehouse (Partie 5)

Dremio interroge **les mêmes tables Iceberg** que Spark, via **le catalogue
Nessie** et **MinIO**. Objectif : prouver, requêtes à l'appui, que Bronze / Silver
/ Gold sont exploitables en SQL, avec **au moins une jointure inter-domaines** et
**une agrégation**.

---

## 1. Première connexion

- URL : http://localhost:9047
- Un utilisateur admin par défaut est créé (`-Ddebug.addDefaultUser=true` dans
  `docker-compose.yml`) : **`dremio` / `dremio123`**.
  Sinon, créer le premier utilisateur via l'écran d'accueil.

## 2. Ajouter la source Nessie (+ MinIO)

Automatique :
```bash
python dremio/scripts/configure_dremio.py
```

Manuel : **Add Source → Nessie**
| Champ                         | Valeur                          |
|-------------------------------|---------------------------------|
| Name                          | `nessie`                        |
| Nessie Endpoint URL           | `http://nessie:19120/api/v2`    |
| Nessie Authentication Type    | `None`                          |
| **Storage** → AWS access key  | `minioadmin`                    |
| AWS access secret             | `minioadmin123`                 |
| AWS root path                 | `warehouse`                     |
| Encrypt connection            | **désactivé** (MinIO en HTTP)   |
| Connection Properties         | `fs.s3a.path.style.access = true` |
|                               | `fs.s3a.endpoint = minio:9000`  |
|                               | `dremio.s3.compat = true`        |
|                               | `fs.s3a.connection.ssl.enabled = false` |

Après « Save », les tables apparaissent sous
`nessie.bronze.*`, `nessie.silver.*`, `nessie.gold.*`.

## 3. Requêtes de validation (à exécuter EN DIRECT dans la vidéo)

Fichiers dans `dremio/queries/` :

| Fichier                        | Démontre                                             |
|--------------------------------|-----------------------------------------------------|
| `01_bronze_checks.sql`         | lecture brute Bronze (3 domaines)                   |
| `02_silver_checks.sql`         | lecture Silver typée / aplatie                      |
| `03_gold_checks.sql`           | lecture Gold (dimensions + faits + agrégats)        |
| `04_join_cross_domain.sql`     | **jointure produits × clients × paniers**           |
| `05_aggregation_gold.sql`      | **agrégations** (GROUP BY, moyennes, tendance 6 mois)|
| `06_time_travel.sql`           | **BONUS** : time travel Iceberg / AT BRANCH Nessie  |

## 4. Vues (virtual datasets) — valorisé, optionnel

`dremio/scripts/configure_dremio.py --with-views` crée un espace `lakehouse` avec
des VDS au-dessus des tables Gold (`vds_sales_trend`, `vds_customer_360`) pour
faciliter la démo et l'éventuel branchement d'un outil BI.

## 5. Dépannage

| Symptôme                                   | Cause / correctif                                   |
|-------------------------------------------|----------------------------------------------------|
| `NoSuchKey` / `Access Denied` sur MinIO   | `fs.s3a.path.style.access` manquant, ou mauvaises clés |
| Aucune table sous `nessie.*`              | Le pipeline Spark n'a pas encore tourné (lancer le DAG) |
| `Unable to reach Nessie`                  | mauvais endpoint : doit être `http://nessie:19120/api/v2` |
| Erreurs SSL                               | désactiver « Encrypt connection » + `...ssl.enabled=false` |
