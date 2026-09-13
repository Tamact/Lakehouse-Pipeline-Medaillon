#!/bin/sh
# =============================================================================
#  minio-init : cree les buckets de la plateforme et separe clairement
#               la zone brute (NiFi) du warehouse Iceberg (Spark).
#  Image utilisee : minio/mc  (client MinIO)
# =============================================================================
set -e

echo "[minio-init] Attente de MinIO sur ${MINIO_ENDPOINT} ..."
until mc alias set local "${MINIO_ENDPOINT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" >/dev/null 2>&1; do
  echo "[minio-init] MinIO pas encore pret, nouvelle tentative dans 3s..."
  sleep 3
done
echo "[minio-init] Connecte a MinIO."

# --- Zone brute : donnees JSON deposees par NiFi (aucune transformation) -----
mc mb --ignore-existing "local/${RAW_BUCKET}"
# --- Warehouse Iceberg : tables Bronze / Silver / Gold gerees par Nessie -----
mc mb --ignore-existing "local/${WAREHOUSE_BUCKET}"

# Versioning sur la zone brute : trace de chaque re-ingestion d'un meme objet
mc version enable "local/${RAW_BUCKET}" || true

# NB : on ne cree PAS de fichier placeholder dans les prefixes de domaine.
# Les prefixes  raw/fakestoreapi/<domaine>/snapshot_date=.../  sont crees par
# NiFi au 1er depot. Spark filtre de toute facon sur "*.json" (pathGlobFilter).

echo "[minio-init] Buckets disponibles :"
mc ls local
echo "[minio-init] Termine."
