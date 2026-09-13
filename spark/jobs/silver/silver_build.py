"""
================================================================================
  MEDAILLON - COUCHE SILVER
================================================================================
Objectif : a partir de `bronze.<domaine>_raw`, produire des tables Silver
nettoyees, typees, deduupliquees et "aplaties" (structures imbriquees eclatees),
pretes pour l'analyse. Une ligne Silver = 1 entite metier x 1 snapshot.

Regles de transformation retenues (Bronze -> Silver) :
  * typage explicite (ids en INT, price en DOUBLE, dates en DATE/TIMESTAMP) ;
  * aplatissement des structures FakeStoreAPI :
        products.rating -> rating_rate / rating_count
        users.name      -> first_name / last_name / full_name
        users.address   -> city / street / zipcode / lat / long
        carts.products[] -> table de faits cart_items (1 ligne par article) ;
  * qualite : filtres (ids non nuls, price >= 0, quantity > 0, email valide) ;
  * deduplication : on ne garde, pour chaque (cle metier, snapshot_date), que
    la version issue de l'ingestion la plus recente (ingestion_ts max) ;
  * enrichissements deterministes documentes :
        products.price_band   (bucket de prix)
        users.us_region       (zone derivee du 1er chiffre du zipcode US).

Usage :
  spark-submit silver_build.py --domain products|users|carts [--branch main]
================================================================================
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.window import Window  # noqa: E402

from common import config as C  # noqa: E402
from common.io_utils import merge_upsert  # noqa: E402
from common.spark_session import build_spark, ensure_namespaces  # noqa: E402

EMAIL_RE = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"


def _dedupe_latest(df: DataFrame, keys: list[str]) -> DataFrame:
    """Garde la ligne la plus recente (ingestion_ts) par (cles + snapshot_date)."""
    w = Window.partitionBy(*keys, "snapshot_date").orderBy(F.col("ingestion_ts").desc())
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


# --------------------------------------------------------------------------- #
#  PRODUCTS                                                                   #
# --------------------------------------------------------------------------- #
def build_products(spark: SparkSession) -> None:
    src = spark.table(C.table(C.BRONZE_NS, "products_raw"))

    df = (
        src.select(
            F.col("id").cast("int").alias("product_id"),
            F.trim(F.col("title")).alias("title"),
            F.col("price").cast("double").alias("price"),
            F.lower(F.trim(F.col("category"))).alias("category"),
            F.trim(F.col("description")).alias("description"),
            F.col("image").alias("image_url"),
            F.col("rating.rate").cast("double").alias("rating_rate"),
            F.col("rating.count").cast("int").alias("rating_count"),
            F.col("snapshot_date"),
            F.col("ingestion_ts"),
        )
        # --- Qualite -----------------------------------------------------
        .filter(F.col("product_id").isNotNull())
        .filter(F.col("price").isNotNull() & (F.col("price") >= 0))
    )

    df = _dedupe_latest(df, ["product_id"])

    # --- Enrichissement : tranche de prix ------------------------------
    df = df.withColumn(
        "price_band",
        F.when(F.col("price") < 20, "0-20")
        .when(F.col("price") < 50, "20-50")
        .when(F.col("price") < 100, "50-100")
        .otherwise("100+"),
    ).withColumn("_silver_at", F.current_timestamp())

    merge_upsert(
        spark, df, C.table(C.SILVER_NS, "products"),
        key_cols=["product_id", "snapshot_date"], partition_by=["snapshot_date"],
    )
    print(f"[silver] products -> {spark.table(C.table(C.SILVER_NS, 'products')).count()} lignes")


# --------------------------------------------------------------------------- #
#  USERS                                                                      #
# --------------------------------------------------------------------------- #
def build_users(spark: SparkSession) -> None:
    src = spark.table(C.table(C.BRONZE_NS, "users_raw"))

    df = (
        src.select(
            F.col("id").cast("int").alias("user_id"),
            F.lower(F.trim(F.col("email"))).alias("email"),
            F.col("username"),
            F.initcap(F.trim(F.col("name.firstname"))).alias("first_name"),
            F.initcap(F.trim(F.col("name.lastname"))).alias("last_name"),
            F.col("phone"),
            F.initcap(F.trim(F.col("address.city"))).alias("city"),
            F.trim(F.col("address.street")).alias("street"),
            F.col("address.number").cast("int").alias("street_number"),
            F.trim(F.col("address.zipcode")).alias("zipcode"),
            F.col("address.geolocation.lat").cast("double").alias("latitude"),
            F.col("address.geolocation.long").cast("double").alias("longitude"),
            F.col("snapshot_date"),
            F.col("ingestion_ts"),
        )
        .filter(F.col("user_id").isNotNull())
    )

    df = _dedupe_latest(df, ["user_id"])

    df = (
        df.withColumn("full_name", F.concat_ws(" ", "first_name", "last_name"))
        .withColumn("email_is_valid", F.col("email").rlike(EMAIL_RE))
        # Zone US derivee du 1er chiffre du code postal (documente).
        .withColumn(
            "us_region",
            F.when(F.substring("zipcode", 1, 1).isin("0", "1", "2"), "East")
            .when(F.substring("zipcode", 1, 1).isin("3", "4", "5"), "South/Central")
            .when(F.substring("zipcode", 1, 1).isin("6", "7"), "Central")
            .when(F.substring("zipcode", 1, 1).isin("8", "9"), "West")
            .otherwise("Unknown"),
        )
        .withColumn("_silver_at", F.current_timestamp())
    )

    merge_upsert(
        spark, df, C.table(C.SILVER_NS, "users"),
        key_cols=["user_id", "snapshot_date"], partition_by=["snapshot_date"],
    )
    print(f"[silver] users -> {spark.table(C.table(C.SILVER_NS, 'users')).count()} lignes")


# --------------------------------------------------------------------------- #
#  CARTS  ->  silver.carts (entete) + silver.cart_items (detail eclate)       #
# --------------------------------------------------------------------------- #
def build_carts(spark: SparkSession) -> None:
    src = spark.table(C.table(C.BRONZE_NS, "carts_raw"))

    base = src.select(
        F.col("id").cast("int").alias("cart_id"),
        F.col("userId").cast("int").alias("user_id"),
        F.to_date(F.col("date")).alias("cart_date"),
        F.col("products").alias("products"),
        F.col("snapshot_date"),
        F.col("ingestion_ts"),
    ).filter(F.col("cart_id").isNotNull() & F.col("user_id").isNotNull())

    base = _dedupe_latest(base, ["cart_id"])

    # --- Detail : 1 ligne par article du panier ----------------------
    items = (
        base.select(
            "cart_id", "user_id", "cart_date", "snapshot_date",
            F.explode_outer("products").alias("p"),
        )
        .select(
            "cart_id", "user_id", "cart_date", "snapshot_date",
            F.col("p.productId").cast("int").alias("product_id"),
            F.col("p.quantity").cast("int").alias("quantity"),
        )
        .filter(F.col("product_id").isNotNull() & (F.col("quantity") > 0))
        .withColumn("_silver_at", F.current_timestamp())
    )

    merge_upsert(
        spark, items, C.table(C.SILVER_NS, "cart_items"),
        key_cols=["cart_id", "product_id", "snapshot_date"], partition_by=["snapshot_date"],
    )

    # --- Entete : agregats par panier ------------------------------
    header = (
        items.groupBy("cart_id", "user_id", "cart_date", "snapshot_date")
        .agg(
            F.countDistinct("product_id").alias("n_distinct_products"),
            F.sum("quantity").alias("total_quantity"),
        )
        .withColumn("cart_month", F.date_format("cart_date", "yyyy-MM"))
        .withColumn("_silver_at", F.current_timestamp())
    )

    merge_upsert(
        spark, header, C.table(C.SILVER_NS, "carts"),
        key_cols=["cart_id", "snapshot_date"], partition_by=["snapshot_date"],
    )
    print(
        f"[silver] carts -> {spark.table(C.table(C.SILVER_NS, 'carts')).count()} entetes / "
        f"{spark.table(C.table(C.SILVER_NS, 'cart_items')).count()} lignes de detail"
    )


BUILDERS = {"products": build_products, "users": build_users, "carts": build_carts}


def main() -> int:
    p = argparse.ArgumentParser(description="Silver - nettoyage / typage / aplatissement")
    p.add_argument("--domain", required=True, choices=list(BUILDERS))
    p.add_argument("--branch", default=C.NESSIE_REF)
    args = p.parse_args()

    spark = build_spark(
        f"silver_build_{args.domain}",
        extra_conf={f"spark.sql.catalog.{C.CATALOG}.ref": args.branch},
    )
    ensure_namespaces(spark)
    BUILDERS[args.domain](spark)
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
