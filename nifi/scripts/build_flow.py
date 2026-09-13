#!/usr/bin/env python3
"""
=============================================================================
  Amorce du dataflow NiFi via l'API REST (NiFi 1.28)
=============================================================================
Cree de facon IDEMPOTENTE :
  * le Parameter Context `fakestore-ingestion` (tous les parametres) ;
  * le Process Group `FakeStoreAPI_Ingestion` ;
  * la liaison Process Group <-> Parameter Context.

La creation des processeurs eux-memes reste manuelle (voir nifi/README.md
section 3.3) : c'est cette partie qui est commentee dans la video de soutenance.
Ce script eliminie la saisie fastidieuse et sans risque des ~10 parametres.

Usage :
  python nifi/scripts/build_flow.py
Variables d'env optionnelles : NIFI_BASE_URL, NIFI_USERNAME, NIFI_PASSWORD
=============================================================================
"""
from __future__ import annotations

import os
import sys
import urllib3

import requests

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE = os.getenv("NIFI_BASE_URL", "https://localhost:8443/nifi-api")
USER = os.getenv("NIFI_USERNAME", "admin")
PWD = os.getenv("NIFI_PASSWORD", "nifiAdminPass2026")

PC_NAME = "fakestore-ingestion"
PG_NAME = "FakeStoreAPI_Ingestion"

PARAMETERS = [
    ("api.base.url", "https://fakestoreapi.com", False),
    ("s3.endpoint", "http://minio:9000", False),
    ("s3.bucket.raw", "raw", False),
    ("s3.access.key", "minioadmin", False),
    ("s3.secret.key", "minioadmin123", True),
    ("s3.region", "us-east-1", False),
    ("raw.prefix", "fakestoreapi", False),
    ("airflow.dagrun.url",
     "http://airflow-webserver:8080/api/v1/dags/lakehouse_medallion/dagRuns", False),
    # base64("nifi_trigger:nifi_trigger_123")
    ("airflow.auth.basic", "bmlmaV90cmlnZ2VyOm5pZmlfdHJpZ2dlcl8xMjM=", True),
]


def _die(msg: str) -> None:
    print(f"ERREUR: {msg}", file=sys.stderr)
    sys.exit(1)


def token() -> str:
    r = requests.post(
        f"{BASE}/access/token",
        data={"username": USER, "password": PWD},
        verify=False, timeout=15,
    )
    if r.status_code != 201:
        _die(f"authentification NiFi refusee ({r.status_code}): {r.text}")
    return r.text.strip()


def main() -> int:
    s = requests.Session()
    s.verify = False
    s.headers["Authorization"] = f"Bearer {token()}"

    # --- Parameter Context (creer si absent) -------------------------------
    pcs = s.get(f"{BASE}/flow/parameter-contexts", timeout=15).json()["parameterContexts"]
    pc = next((p for p in pcs if p["component"]["name"] == PC_NAME), None)
    if pc:
        pc_id = pc["id"]
        print(f"= Parameter Context '{PC_NAME}' deja present ({pc_id})")
    else:
        body = {
            "revision": {"version": 0},
            "component": {
                "name": PC_NAME,
                "description": "Parametres d'ingestion FakeStoreAPI -> MinIO",
                "parameters": [
                    {"parameter": {"name": n, "value": v, "sensitive": sens}}
                    for (n, v, sens) in PARAMETERS
                ],
            },
        }
        r = s.post(f"{BASE}/parameter-contexts", json=body, timeout=15)
        if r.status_code not in (200, 201):
            _die(f"creation Parameter Context: {r.status_code} {r.text}")
        pc_id = r.json()["id"]
        print(f"+ Parameter Context '{PC_NAME}' cree ({pc_id})")

    # --- Process Group racine ------------------------------------------
    root = s.get(f"{BASE}/flow/process-groups/root", timeout=15).json()
    root_id = root["processGroupFlow"]["id"]

    groups = root["processGroupFlow"]["flow"]["processGroups"]
    pg = next((g for g in groups if g["component"]["name"] == PG_NAME), None)
    if pg:
        pg_id = pg["id"]
        print(f"= Process Group '{PG_NAME}' deja present ({pg_id})")
    else:
        body = {
            "revision": {"version": 0},
            "component": {"name": PG_NAME, "position": {"x": 400.0, "y": 200.0}},
        }
        r = s.post(f"{BASE}/process-groups/{root_id}/process-groups", json=body, timeout=15)
        if r.status_code not in (200, 201):
            _die(f"creation Process Group: {r.status_code} {r.text}")
        pg_id = r.json()["id"]
        print(f"+ Process Group '{PG_NAME}' cree ({pg_id})")

    # --- Liaison PG -> Parameter Context ---------------------------
    pg_entity = s.get(f"{BASE}/process-groups/{pg_id}", timeout=15).json()
    current_pc = (pg_entity["component"].get("parameterContext") or {}).get("id")
    if current_pc == pc_id:
        print("= Liaison Process Group <-> Parameter Context deja en place")
    else:
        upd = {
            "revision": pg_entity["revision"],
            "component": {"id": pg_id, "parameterContext": {"id": pc_id}},
        }
        r = s.put(f"{BASE}/process-groups/{pg_id}", json=upd, timeout=15)
        if r.status_code != 200:
            _die(f"liaison PG/PC: {r.status_code} {r.text}")
        print("+ Process Group lie au Parameter Context")

    print(
        "\nOK. Ouvrez https://localhost:8443/nifi , entrez dans le Process Group "
        f"'{PG_NAME}' et ajoutez les processeurs (nifi/README.md section 3.3)."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
