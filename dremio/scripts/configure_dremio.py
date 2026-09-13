#!/usr/bin/env python3
"""
=============================================================================
  Configuration automatique de Dremio OSS (Partie 5)
=============================================================================
  * cree le 1er utilisateur admin si necessaire ;
  * ajoute la source "nessie" (catalogue Nessie + stockage MinIO) ;
  * (option --with-views) cree un espace "lakehouse" + quelques VDS Gold.

Usage :
  python dremio/scripts/configure_dremio.py [--with-views]
Env : DREMIO_URL (def http://localhost:9047), DREMIO_USER, DREMIO_PASSWORD
=============================================================================
"""
from __future__ import annotations

import argparse
import os
import sys
import time

import requests

URL = os.getenv("DREMIO_URL", "http://localhost:9047")
USER = os.getenv("DREMIO_USER", "dremio")
PASSWORD = os.getenv("DREMIO_PASSWORD", "dremio123")

# Endpoints internes (noms de services docker)
NESSIE_ENDPOINT = os.getenv("NESSIE_URI", "http://nessie:19120/api/v2")
S3_ENDPOINT_HOST = os.getenv("S3_ENDPOINT_HOST", "minio:9000")
S3_ACCESS = os.getenv("MINIO_ROOT_USER", "minioadmin")
S3_SECRET = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")
WAREHOUSE_BUCKET = os.getenv("WAREHOUSE_BUCKET", "warehouse")
SOURCE_NAME = "nessie"


def wait_up() -> None:
    for _ in range(60):
        try:
            if requests.get(URL, timeout=5).status_code < 500:
                return
        except requests.RequestException:
            pass
        time.sleep(3)
    sys.exit("Dremio injoignable sur " + URL)


def ensure_first_user() -> None:
    try:
        r = requests.post(
            f"{URL}/apiv2/bootstrap/firstuser",
            json={
                "userName": USER, "firstName": "Lake", "lastName": "House",
                "email": "admin@dit.sn", "createdAt": int(time.time() * 1000),
                "password": PASSWORD,
            },
            headers={"Authorization": "_dremionull"},
            timeout=15,
        )
        if r.status_code in (200, 201):
            print(f"+ Utilisateur admin '{USER}' cree")
        else:
            print(f"= 1er utilisateur deja present (HTTP {r.status_code})")
    except requests.RequestException as e:
        print(f"= bootstrap firstuser ignore ({e})")


def login() -> str:
    r = requests.post(
        f"{URL}/apiv2/login",
        json={"userName": USER, "password": PASSWORD},
        timeout=15,
    )
    r.raise_for_status()
    return r.json()["token"]


def create_nessie_source(tok: str) -> None:
    h = {"Authorization": f"_dremio{tok}", "Content-Type": "application/json"}
    existing = requests.get(f"{URL}/api/v3/catalog", headers=h, timeout=15).json()
    for item in existing.get("data", []):
        if item.get("path", [None])[0] == SOURCE_NAME:
            print(f"= Source '{SOURCE_NAME}' deja presente")
            return

    body = {
        "entityType": "source",
        "type": "NESSIE",
        "name": SOURCE_NAME,
        "config": {
            "nessieEndpoint": NESSIE_ENDPOINT,
            "nessieAuthType": "NONE",
            "credentialType": "ACCESS_KEY",
            "awsAccessKey": S3_ACCESS,
            "awsAccessSecret": S3_SECRET,
            "awsRootPath": WAREHOUSE_BUCKET,
            "secure": False,
            "propertyList": [
                {"name": "fs.s3a.path.style.access", "value": "true"},
                {"name": "fs.s3a.endpoint", "value": S3_ENDPOINT_HOST},
                {"name": "fs.s3a.connection.ssl.enabled", "value": "false"},
                {"name": "dremio.s3.compat", "value": "true"},
            ],
        },
    }
    r = requests.post(f"{URL}/api/v3/catalog", headers=h, json=body, timeout=30)
    if r.status_code in (200, 201):
        print(f"+ Source Nessie '{SOURCE_NAME}' creee (endpoint {NESSIE_ENDPOINT}, bucket {WAREHOUSE_BUCKET})")
    else:
        print(f"! Creation source: HTTP {r.status_code} -> {r.text[:400]}")
        print("  -> creer la source manuellement (dremio/README.md section 2).")


VIEWS = {
    "vds_sales_trend": (
        "SELECT snapshot_month, category, revenue, units_sold, n_carts "
        f"FROM {SOURCE_NAME}.gold.agg_sales_by_category_month"
    ),
    "vds_customer_360": (
        "SELECT u.user_id, u.full_name, u.us_region, "
        "COUNT(DISTINCT f.cart_id) AS nb_paniers, "
        "ROUND(SUM(f.line_revenue),2) AS ca_total "
        f"FROM {SOURCE_NAME}.gold.fact_cart_items f "
        f"JOIN {SOURCE_NAME}.gold.dim_users u ON u.user_id = f.user_id "
        "GROUP BY u.user_id, u.full_name, u.us_region"
    ),
}


def create_views(tok: str) -> None:
    h = {"Authorization": f"_dremio{tok}", "Content-Type": "application/json"}
    requests.post(f"{URL}/api/v3/catalog", headers=h,
                  json={"entityType": "space", "name": "lakehouse"}, timeout=15)
    for name, sql in VIEWS.items():
        body = {
            "entityType": "dataset", "type": "VIRTUAL_DATASET",
            "path": ["lakehouse", name], "sql": sql,
        }
        r = requests.post(f"{URL}/api/v3/catalog", headers=h, json=body, timeout=30)
        print(f"{'+' if r.status_code in (200, 201) else '!'} VDS lakehouse.{name} (HTTP {r.status_code})")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--with-views", action="store_true")
    args = ap.parse_args()

    wait_up()
    ensure_first_user()
    tok = login()
    create_nessie_source(tok)
    if args.with_views:
        create_views(tok)
    print("\nDremio pret : http://localhost:9047  ->  source 'nessie' (bronze/silver/gold)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
