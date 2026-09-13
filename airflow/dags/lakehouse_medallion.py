"""
================================================================================
  DAG PRINCIPAL - Pipeline medaillon de bout en bout
================================================================================
Enchaine, pour chaque domaine FakeStoreAPI (products, users, carts) :

    [trigger NiFi]  ->  [attente zone brute]  ->  BRONZE  ->  SILVER  ->  GOLD

Modes de declenchement (Partie 6 du sujet) :
  * Mode nominal  = OPTION B (evenementiel) : a la fin de son ingestion, NiFi
    appelle l'API REST d'Airflow :
        POST /api/v1/dags/lakehouse_medallion/dagRuns
    avec, dans `conf`, {"snapshot_date": "...", "triggered_by": "nifi"}.
  * Mode repli    = OPTION A (planifie) : le DAG `lakehouse_scan_raw` sonde
    periodiquement MinIO et declenche ce DAG quand de nouveaux objets arrivent.

Ce DAG peut aussi declencher lui-meme l'ingestion NiFi (parametre `conf`
`{"run_ingestion": true}`) pour une demo "un seul clic".
================================================================================
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.trigger_rule import TriggerRule

# --- Parametres d'environnement (injectes par docker compose) ----------------
DOMAINS = [d.strip() for d in os.getenv("LAKEHOUSE_DOMAINS", "products,users,carts").split(",") if d.strip()]
JOBS_DIR = "/opt/spark/jobs"
SPARK_CONN_ID = "spark_default"
NIFI_TRIGGER_URL = os.getenv("NIFI_INGEST_URL", "http://nifi:9080/ingest")

# Confs Spark communes : le driver tourne dans airflow-scheduler (client mode),
# il doit etre joignable par les executors du worker Spark.
SPARK_CONF = {
    "spark.driver.host": os.getenv("SPARK_DRIVER_HOST", "airflow-scheduler"),
    "spark.driver.bindAddress": "0.0.0.0",
    "spark.sql.shuffle.partitions": "8",
}

default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}


def _submit(task_id: str, script: str, args: list[str]) -> SparkSubmitOperator:
    return SparkSubmitOperator(
        task_id=task_id,
        conn_id=SPARK_CONN_ID,
        application=f"{JOBS_DIR}/{script}",
        name=f"medallion_{task_id}",
        conf=SPARK_CONF,
        application_args=args,
        verbose=False,
    )


def trigger_nifi_ingestion(**context) -> str:
    """Optionnel : demande a NiFi d'ingerer le snapshot courant (OPTION B inverse)."""
    conf = context["dag_run"].conf or {}
    if not conf.get("run_ingestion"):
        return "Ingestion NiFi non demandee (run_ingestion != true) - on suppose la zone brute deja alimentee."
    import requests

    snapshot_date = conf.get("snapshot_date") or datetime.utcnow().strftime("%Y-%m-%d")
    resp = requests.post(
        NIFI_TRIGGER_URL,
        params={"snapshot_date": snapshot_date, "domains": ",".join(DOMAINS)},
        timeout=30,
    )
    resp.raise_for_status()
    return f"NiFi declenche pour snapshot_date={snapshot_date} (HTTP {resp.status_code})"


def wait_for_raw(**context) -> None:
    """
    Verifie qu'au moins un objet existe dans la zone brute de chaque domaine.
    Sonde MinIO via boto3 (S3-compatible).
    """
    import boto3

    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.getenv("MINIO_ROOT_USER", "minioadmin"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123"),
        region_name=os.getenv("MINIO_REGION", "us-east-1"),
    )
    bucket = os.getenv("RAW_BUCKET", "raw")
    prefix_root = os.getenv("RAW_ROOT_PREFIX", "fakestoreapi")
    missing = []
    for domain in DOMAINS:
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=f"{prefix_root}/{domain}/", MaxKeys=5)
        json_objs = [o for o in resp.get("Contents", []) if o["Key"].endswith(".json")]
        if not json_objs:
            missing.append(domain)
    if missing:
        raise ValueError(f"Zone brute vide pour : {missing}. Lancez l'ingestion NiFi d'abord.")
    print(f"Zone brute OK pour tous les domaines : {DOMAINS}")


with DAG(
    dag_id="lakehouse_medallion",
    description="FakeStoreAPI -> NiFi/MinIO -> Spark (Bronze/Silver/Gold Iceberg) -> Dremio",
    default_args=default_args,
    schedule=None,  # declenche par NiFi (option B) ou par lakehouse_scan_raw (option A)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "medallion", "iceberg", "nessie"],
    params={
        "run_ingestion": Param(False, type="boolean", description="Demander a NiFi d'ingerer avant de traiter"),
        "snapshot_date": Param("", type="string", description="Snapshot cible (defaut: aujourd'hui)"),
    },
) as dag:

    start = EmptyOperator(task_id="start")

    t_trigger_nifi = PythonOperator(
        task_id="trigger_nifi_ingestion",
        python_callable=trigger_nifi_ingestion,
    )

    t_wait_raw = PythonOperator(
        task_id="wait_for_raw_zone",
        python_callable=wait_for_raw,
        retries=5,
        retry_delay=timedelta(seconds=30),
    )

    bronze_done = EmptyOperator(task_id="bronze_done")
    silver_done = EmptyOperator(task_id="silver_done")

    # --- BRONZE : 1 job par domaine (parallele) --------------------------
    for domain in DOMAINS:
        b = _submit(
            f"bronze_{domain}",
            "bronze/bronze_ingest.py",
            ["--domain", domain, "--batch-id", "{{ run_id }}"],
        )
        start >> t_trigger_nifi >> t_wait_raw >> b >> bronze_done

    # --- SILVER : 1 job par domaine (parallele, apres tout le Bronze) ----
    for domain in DOMAINS:
        s = _submit(
            f"silver_{domain}",
            "silver/silver_build.py",
            ["--domain", domain],
        )
        bronze_done >> s >> silver_done

    # --- GOLD : un seul job (dimensions + faits + agregats) -------------
    t_gold = _submit("gold_build", "gold/gold_build.py", [])

    # --- Controle qualite final --------------------------------------
    t_smoke = _submit("smoke_test_gold", "checks/smoke_test.py", [])
    t_smoke.trigger_rule = TriggerRule.ALL_SUCCESS

    end = EmptyOperator(task_id="end")

    silver_done >> t_gold >> t_smoke >> end
