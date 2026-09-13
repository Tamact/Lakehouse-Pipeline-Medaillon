"""
================================================================================
  MEDAILLON - COUCHE BRONZE
================================================================================
Objectif : charger, SANS transformation metier, les fichiers JSON deposes par
NiFi dans la zone brute MinIO vers une table Iceberg `bronze.<domaine>_raw`.

Principes de la couche Bronze retenus pour cette plateforme :
  * fidelite a la source : on conserve les champs bruts de FakeStoreAPI tels
    quels (y compris structures imbriquees : rating, name, address, products...) ;
  * tracabilite : chaque ligne porte des colonnes techniques (_source_file,
    snapshot_date, ingestion_ts, _ingested_at, _batch_id, _record_hash) ;
  * historisation : partitionnement par `snapshot_date` -> l'historique 6 mois
    reconstitue par le backfill NiFi est materialise ici ;
  * idempotence : MERGE sur `_record_hash` -> rejouer un batch NiFi ne cree
    pas de doublons.

Usage :
  spark-submit bronze_ingest.py --domain products [--batch-id ...] [--branch main]
================================================================================
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F  # noqa: E402

from common import config as C  # noqa: E402
from common.io_utils import merge_upsert  # noqa: E402
from common.spark_session import build_spark, ensure_namespaces  # noqa: E402

# Regex d'extraction des metadonnees d'ingestion depuis la cle d'objet S3 :
#   raw/fakestoreapi/<domaine>/snapshot_date=YYYY-MM-DD/ingestion_ts=<epoch_ms>/<fichier>.json
RE_SNAPSHOT = r"snapshot_date=(\d{4}-\d{2}-\d{2})"
RE_INGEST_TS = r"ingestion_ts=(\d+)"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Bronze - ingestion zone brute -> Iceberg")
    p.add_argument("--domain", required=True, choices=["products", "users", "carts"])
    p.add_argument("--batch-id", default="manual", help="Identifiant du batch d'orchestration")
    p.add_argument("--branch", default=C.NESSIE_REF, help="Branche Nessie cible")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    domain = args.domain

    spark = build_spark(
        f"bronze_ingest_{domain}",
        extra_conf={f"spark.sql.catalog.{C.CATALOG}.ref": args.branch},
    )
    ensure_namespaces(spark)

    raw_path = C.raw_domain_path(domain)  # s3a://raw/fakestoreapi/<domain>
    print(f"[bronze] Lecture de la zone brute : {raw_path}")

    # Lecture recursive de tous les snapshots du domaine.
    #   multiLine=true       -> chaque fichier = le tableau JSON renvoye par l'API
    #   recursiveFileLookup  -> descend dans snapshot_date=.../ingestion_ts=...
    #   pathGlobFilter=*.json -> ignore tout objet non-JSON eventuel
    try:
        df_raw = (
            spark.read.option("multiLine", "true")
            .option("recursiveFileLookup", "true")
            .option("pathGlobFilter", "*.json")
            .json(raw_path)
        )
    except Exception as exc:  # chemin inexistant = zone brute encore vide
        print(f"[bronze] Zone brute absente/illisible ({exc}) - rien a faire.")
        spark.stop()
        return 0

    if len(df_raw.columns) == 0 or df_raw.rdd.isEmpty():
        print(f"[bronze] Aucun fichier JSON dans {raw_path} - rien a faire.")
        spark.stop()
        return 0

    # --- Colonnes techniques (aucune transformation metier) -----------------
    src_col = F.input_file_name()
    df_bronze = (
        df_raw
        .withColumn("_source_file", src_col)
        .withColumn(
            "snapshot_date",
            F.coalesce(
                F.to_date(F.regexp_extract(src_col, RE_SNAPSHOT, 1), "yyyy-MM-dd"),
                F.current_date(),
            ),
        )
        .withColumn(
            "ingestion_ts",
            F.coalesce(
                (F.regexp_extract(src_col, RE_INGEST_TS, 1).cast("long") / F.lit(1000)).cast("timestamp"),
                F.current_timestamp(),
            ),
        )
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_batch_id", F.lit(args.batch_id))
        .withColumn("source_domain", F.lit(domain))
        .withColumn("source_system", F.lit("fakestoreapi"))
    )

    # Empreinte de la ligne = hash de toutes les colonnes SOURCE (hors techniques).
    business_cols = [c for c in df_raw.columns]
    df_bronze = df_bronze.withColumn(
        "_record_hash",
        F.sha2(F.to_json(F.struct(*[F.col(c) for c in business_cols])), 256),
    )

    target = C.table(C.BRONZE_NS, f"{domain}_raw")
    print(f"[bronze] MERGE idempotent -> {target}  ({df_bronze.count()} lignes candidates)")

    # Idempotence : une meme ligne (meme hash) dans un meme snapshot n'est
    # inseree qu'une fois. On ne met PAS a jour (Bronze = append-only immuable).
    merge_upsert(
        spark,
        df_bronze,
        target,
        key_cols=["snapshot_date", "_record_hash"],
        partition_by=["snapshot_date"],
        update=False,
    )

    total = spark.table(target).count()
    print(f"[bronze] OK - {target} contient desormais {total} lignes.")
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
