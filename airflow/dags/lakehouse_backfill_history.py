"""
================================================================================
  DAG BACKFILL - Reconstitution d'un historique ~6 mois (contrainte section 4.1)
================================================================================
FakeStoreAPI ne renvoie que l'etat courant, sans dimension temporelle. Ce DAG
reconstitue une timeline en demandant a NiFi une serie de SNAPSHOTS HEBDOMADAIRES
horodates, repartis sur ~6 mois (26 semaines par defaut).

Simulation de tendance (honnete et documentee) :
  * products / users : ingeres en entier a chaque snapshot (ce sont des
    dimensions - leur etat "connu" est le meme a chaque date) ;
  * carts : on fait CROITRE le parametre `limit` de l'API au fil des semaines
    (`GET /carts?limit=N&sort=desc`) -> on simule l'arrivee progressive des
    commandes. Le revenu Gold agrege par `snapshot_month` presente alors une
    tendance croissante analysable sur 6 mois.

L'axe temporel d'analyse en couche Gold est `snapshot_date` (timeline
reconstituee), pas la date native des paniers (figee en 2019-2020).

Declenchement : manuel (Trigger DAG), une fois, au provisioning de la plateforme.
================================================================================
"""
from __future__ import annotations

import os
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

DOMAINS = [d.strip() for d in os.getenv("LAKEHOUSE_DOMAINS", "products,users,carts").split(",") if d.strip()]
NIFI_TRIGGER_URL = os.getenv("NIFI_INGEST_URL", "http://nifi:9080/ingest")
DEFAULT_WEEKS = int(os.getenv("BACKFILL_WEEKS", "26"))


def _weekly_snapshots(weeks: int, end_date: datetime) -> list[datetime]:
    """Dates de snapshot, une par semaine, de la plus ancienne a la plus recente."""
    return sorted(end_date - timedelta(weeks=w) for w in range(weeks))


def run_backfill(**context) -> str:
    import requests

    conf = context["dag_run"].conf or {}
    weeks = int(conf.get("weeks") or DEFAULT_WEEKS)
    cart_total = int(conf.get("cart_total") or 7)  # FakeStoreAPI expose ~7 paniers
    end_date = datetime.utcnow()
    if conf.get("end_date"):
        end_date = datetime.strptime(conf["end_date"], "%Y-%m-%d")

    snapshots = _weekly_snapshots(weeks, end_date)
    report = []
    for i, snap in enumerate(snapshots, start=1):
        # limite de paniers croissante : de ~1 (semaine la plus ancienne) a cart_total
        cart_limit = max(1, round(cart_total * i / weeks))
        snap_str = snap.strftime("%Y-%m-%d")
        params = {
            "snapshot_date": snap_str,
            "domains": ",".join(DOMAINS),
            "cart_limit": cart_limit,
        }
        resp = requests.post(NIFI_TRIGGER_URL, params=params, timeout=60)
        resp.raise_for_status()
        report.append(f"{snap_str} (cart_limit={cart_limit}) -> HTTP {resp.status_code}")
        time.sleep(float(conf.get("sleep_between") or 2.0))

    msg = f"{len(snapshots)} snapshots hebdomadaires demandes a NiFi :\n" + "\n".join(report)
    print(msg)
    return msg


with DAG(
    dag_id="lakehouse_backfill_history",
    description="Reconstitue ~6 mois d'historique via des snapshots NiFi horodates",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-eng", "retries": 0, "execution_timeout": timedelta(hours=1)},
    tags=["lakehouse", "backfill", "history"],
    params={
        "weeks": Param(DEFAULT_WEEKS, type="integer", description="Nb de snapshots hebdomadaires (~26 = 6 mois)"),
        "cart_total": Param(7, type="integer", description="Nb total de paniers exposes par l'API"),
        "end_date": Param("", type="string", description="Date de fin (YYYY-MM-DD), defaut = aujourd'hui"),
        "sleep_between": Param(2.0, type="number", description="Pause (s) entre deux appels NiFi"),
    },
) as dag:

    t_backfill = PythonOperator(
        task_id="request_weekly_snapshots_from_nifi",
        python_callable=run_backfill,
    )

    t_run_medallion = TriggerDagRunOperator(
        task_id="run_full_medallion",
        trigger_dag_id="lakehouse_medallion",
        conf={"triggered_by": "backfill", "run_ingestion": False},
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
        allowed_states=["success"],
    )

    t_backfill >> t_run_medallion
