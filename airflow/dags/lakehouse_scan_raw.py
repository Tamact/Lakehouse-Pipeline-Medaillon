"""
================================================================================
  DAG DE REPLI - OPTION A (declenchement planifie)
================================================================================
Sonde periodiquement la zone brute de MinIO. Des qu'un objet JSON plus recent
que le dernier traitement est detecte, declenche `lakehouse_medallion`.

L'etat "derniere cle vue" est conserve dans une Variable Airflow
(`raw_zone_last_seen_iso`) -> le DAG ne redeclenche pas inutilement.

Active ce DAG (unpause) uniquement si tu veux le mode planifie ; en mode
nominal (option B), NiFi declenche directement `lakehouse_medallion`.
================================================================================
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

RAW_BUCKET = os.getenv("RAW_BUCKET", "raw")
RAW_PREFIX = os.getenv("RAW_ROOT_PREFIX", "fakestoreapi")
VAR_LAST_SEEN = "raw_zone_last_seen_iso"


def _s3():
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.getenv("MINIO_ROOT_USER", "minioadmin"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123"),
        region_name=os.getenv("MINIO_REGION", "us-east-1"),
    )


def detect_new_objects(**_) -> str:
    s3 = _s3()
    last_seen = Variable.get(VAR_LAST_SEEN, default_var="1970-01-01T00:00:00+00:00")
    last_seen_dt = datetime.fromisoformat(last_seen)

    newest = last_seen_dt
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=RAW_BUCKET, Prefix=f"{RAW_PREFIX}/"):
        for obj in page.get("Contents", []):
            if not obj["Key"].endswith(".json"):
                continue
            lm = obj["LastModified"].astimezone(timezone.utc)
            if lm > newest:
                newest = lm

    if newest > last_seen_dt:
        Variable.set(VAR_LAST_SEEN, newest.isoformat())
        print(f"Nouvelles donnees detectees (jusqu'a {newest.isoformat()}) -> declenchement.")
        return "trigger_medallion"
    print("Aucune nouvelle donnee dans la zone brute.")
    return "noop"


with DAG(
    dag_id="lakehouse_scan_raw",
    description="OPTION A : sonde MinIO et declenche lakehouse_medallion",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={"owner": "data-eng", "retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["lakehouse", "option-A", "scheduler"],
) as dag:

    branch = BranchPythonOperator(task_id="detect_new_objects", python_callable=detect_new_objects)

    trigger = TriggerDagRunOperator(
        task_id="trigger_medallion",
        trigger_dag_id="lakehouse_medallion",
        conf={"triggered_by": "scan_raw", "run_ingestion": False},
        wait_for_completion=False,
        reset_dag_run=True,
    )

    noop = EmptyOperator(task_id="noop")

    branch >> [trigger, noop]
