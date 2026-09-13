"""
Configuration centralisee des jobs Spark - lue depuis l'environnement.

Toutes les valeurs proviennent des variables definies dans `.env` et injectees
par docker compose (cote cluster Spark) ou par Airflow (cote driver client mode).
Aucune valeur sensible n'est ecrite en dur dans le code.
"""
from __future__ import annotations

import os

# --- Acces objet MinIO / S3 -------------------------------------------------
# On accepte plusieurs noms de variables (Spark / Airflow / MinIO) pour rester
# robuste quel que soit le conteneur qui lance le job.
S3_ENDPOINT: str = (
    os.getenv("MINIO_ENDPOINT")
    or os.getenv("S3_ENDPOINT")
    or "http://minio:9000"
)
S3_ACCESS_KEY: str = (
    os.getenv("MINIO_ROOT_USER")
    or os.getenv("AWS_ACCESS_KEY_ID")
    or "minioadmin"
)
S3_SECRET_KEY: str = (
    os.getenv("MINIO_ROOT_PASSWORD")
    or os.getenv("AWS_SECRET_ACCESS_KEY")
    or "minioadmin123"
)
S3_REGION: str = os.getenv("MINIO_REGION", "us-east-1")

# --- Buckets --------------------------------------------------------------
RAW_BUCKET: str = os.getenv("RAW_BUCKET", "raw")
WAREHOUSE_BUCKET: str = os.getenv("WAREHOUSE_BUCKET", "warehouse")

# Prefixe racine de la zone brute alimentee par NiFi
RAW_ROOT_PREFIX: str = os.getenv("RAW_ROOT_PREFIX", "fakestoreapi")

# --- Catalogue Nessie ---------------------------------------------------
NESSIE_URI: str = os.getenv("NESSIE_URI", "http://nessie:19120/api/v2")
NESSIE_REF: str = os.getenv("NESSIE_REF", "main")
CATALOG: str = os.getenv("LAKEHOUSE_CATALOG", "nessie")

# --- Namespaces (bases) du medaillon ----------------------------------
BRONZE_NS: str = "bronze"
SILVER_NS: str = "silver"
GOLD_NS: str = "gold"

# --- Domaines fonctionnels couverts ----------------------------------
DOMAINS: list[str] = [
    d.strip() for d in os.getenv("LAKEHOUSE_DOMAINS", "products,users,carts").split(",") if d.strip()
]

# --- Source imposee -------------------------------------------------
FAKESTORE_API_BASE: str = os.getenv("FAKESTORE_API_BASE", "https://fakestoreapi.com")

# Endpoint REST par domaine
DOMAIN_ENDPOINTS: dict[str, str] = {
    "products": "/products",
    "users": "/users",
    "carts": "/carts",
}


def raw_domain_path(domain: str, protocol: str = "s3a") -> str:
    """URI de la zone brute d'un domaine, ex. s3a://raw/fakestoreapi/products."""
    return f"{protocol}://{RAW_BUCKET}/{RAW_ROOT_PREFIX}/{domain}"


def table(namespace: str, name: str) -> str:
    """Nom pleinement qualifie d'une table Iceberg dans le catalogue Nessie."""
    return f"{CATALOG}.{namespace}.{name}"
