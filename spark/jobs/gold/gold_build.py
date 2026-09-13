"""
================================================================================
  MEDAILLON - COUCHE GOLD
================================================================================
Objectif : a partir des tables Silver, produire des tables Gold "pretes a la
decision" : dimensions courantes, table de faits cross-domaines, et agregats
temporels exploitant l'historique ~6 mois reconstitue.

Tables produites :
  gold.dim_products              1 ligne / produit (etat courant + fenetre de vie)
  gold.dim_users                 1 ligne / client  (etat courant)
  gold.fact_cart_items           grain cart x produit x snapshot ; jointure
                                 carts x products x users ; mesure line_revenue
  gold.agg_sales_by_category_month   revenu / unites / paniers par categorie et mois
  gold.agg_sales_by_region_month     revenu / unites par zone client et mois
  gold.agg_product_price_history     evolution mensuelle prix & note par produit

Ces tables permettent :
  * une jointure entre domaines (fact_cart_items, agg_sales_by_region_month) ;
  * des agregations (tous les agg_*) ;
  * des analyses temporelles sur 6 mois (colonnes *_month).

Usage :
  spark-submit gold_build.py [--branch main]
================================================================================
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.window import Window  # noqa: E402

from common import config as C  # noqa: E402
from common.io_utils import overwrite_table  # noqa: E402
from common.spark_session import build_spark, ensure_namespaces  # noqa: E402


def _latest_per(df, key: str):
    """Derniere version connue d'une entite (snapshot_date max)."""
    w = Window.partitionBy(key).orderBy(F.col("snapshot_date").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")


def build_dim_products(spark: SparkSession) -> None:
    s = spark.table(C.table(C.SILVER_NS, "products"))
    life = s.groupBy("product_id").agg(
        F.min("snapshot_date").alias("first_seen_date"),
        F.max("snapshot_date").alias("last_seen_date"),
        F.round(F.avg("price"), 2).alias("avg_price_6m"),
        F.round(F.avg("rating_rate"), 2).alias("avg_rating_6m"),
    )
    current = _latest_per(s, "product_id").select(
        "product_id", "title", "category", "price", "price_band",
        "rating_rate", "rating_count", "image_url",
    )
    dim = current.join(life, "product_id", "left")
    overwrite_table(spark, dim, C.table(C.GOLD_NS, "dim_products"))
    print(f"[gold] dim_products -> {dim.count()} produits")


def build_dim_users(spark: SparkSession) -> None:
    s = spark.table(C.table(C.SILVER_NS, "users"))
    dim = _latest_per(s, "user_id").select(
        "user_id", "full_name", "email", "email_is_valid",
        "city", "zipcode", "us_region", "latitude", "longitude", "phone",
    )
    overwrite_table(spark, dim, C.table(C.GOLD_NS, "dim_users"))
    print(f"[gold] dim_users -> {dim.count()} clients")


def build_fact_cart_items(spark: SparkSession) -> None:
    ci = spark.table(C.table(C.SILVER_NS, "cart_items"))
    prod = spark.table(C.table(C.SILVER_NS, "products")).select(
        F.col("product_id"), F.col("snapshot_date"), F.col("price"),
        F.col("category"), F.col("title"),
    )
    dim_p = spark.table(C.table(C.GOLD_NS, "dim_products")).select(
        F.col("product_id"),
        F.col("price").alias("price_fallback"),
        F.col("category").alias("category_fallback"),
        F.col("title").alias("title_fallback"),
    )
    dim_u = spark.table(C.table(C.GOLD_NS, "dim_users")).select(
        "user_id", "us_region", "city",
    )

    fact = (
        ci.join(prod, ["product_id", "snapshot_date"], "left")
        .join(dim_p, "product_id", "left")
        .join(dim_u, "user_id", "left")
        .withColumn("unit_price", F.coalesce("price", "price_fallback"))
        .withColumn("category", F.coalesce("category", "category_fallback"))
        .withColumn("product_title", F.coalesce("title", "title_fallback"))
        .withColumn("line_revenue", F.round(F.col("quantity") * F.col("unit_price"), 2))
        # Axe temporel de l'analyse = timeline RECONSTITUEE (mois du snapshot),
        # pas la date native de l'API (figee en 2019-2020). cart_date est
        # conservee comme attribut descriptif.
        .withColumn("snapshot_month", F.date_format("snapshot_date", "yyyy-MM"))
        .withColumn("cart_month_native", F.date_format("cart_date", "yyyy-MM"))
        .select(
            "snapshot_date", "snapshot_month", "cart_id", "cart_date", "cart_month_native",
            "user_id", "us_region", "city",
            "product_id", "product_title", "category",
            "quantity", "unit_price", "line_revenue",
        )
    )
    overwrite_table(spark, fact, C.table(C.GOLD_NS, "fact_cart_items"),
                    partition_by=["snapshot_month"])
    print(f"[gold] fact_cart_items -> {fact.count()} lignes")


def build_agg_sales_by_category_month(spark: SparkSession) -> None:
    f = spark.table(C.table(C.GOLD_NS, "fact_cart_items"))
    agg = (
        f.groupBy("category", "snapshot_month")
        .agg(
            F.countDistinct("cart_id").alias("n_carts"),
            F.countDistinct("user_id").alias("n_customers"),
            F.sum("quantity").alias("units_sold"),
            F.round(F.sum("line_revenue"), 2).alias("revenue"),
            F.round(F.avg("line_revenue"), 2).alias("avg_line_revenue"),
        )
        .withColumn(
            "revenue_per_cart",
            F.round(F.col("revenue") / F.col("n_carts"), 2),
        )
        .orderBy("snapshot_month", "category")
    )
    overwrite_table(spark, agg, C.table(C.GOLD_NS, "agg_sales_by_category_month"),
                    partition_by=["snapshot_month"])
    print(f"[gold] agg_sales_by_category_month -> {agg.count()} lignes")


def build_agg_sales_by_region_month(spark: SparkSession) -> None:
    f = spark.table(C.table(C.GOLD_NS, "fact_cart_items"))
    agg = (
        f.groupBy("us_region", "snapshot_month")
        .agg(
            F.countDistinct("cart_id").alias("n_carts"),
            F.countDistinct("user_id").alias("n_customers"),
            F.sum("quantity").alias("units_sold"),
            F.round(F.sum("line_revenue"), 2).alias("revenue"),
        )
        .orderBy("snapshot_month", "us_region")
    )
    overwrite_table(spark, agg, C.table(C.GOLD_NS, "agg_sales_by_region_month"),
                    partition_by=["snapshot_month"])
    print(f"[gold] agg_sales_by_region_month -> {agg.count()} lignes")


def build_agg_product_price_history(spark: SparkSession) -> None:
    s = spark.table(C.table(C.SILVER_NS, "products"))
    hist = (
        s.withColumn("snapshot_month", F.date_format("snapshot_date", "yyyy-MM"))
        .groupBy("product_id", "snapshot_month")
        .agg(
            F.round(F.avg("price"), 2).alias("avg_price"),
            F.min("price").alias("min_price"),
            F.max("price").alias("max_price"),
            F.round(F.avg("rating_rate"), 2).alias("avg_rating"),
            F.max("rating_count").alias("rating_count"),
        )
    )
    # Variation de prix mois a mois (analyse de tendance).
    w = Window.partitionBy("product_id").orderBy("snapshot_month")
    hist = (
        hist.withColumn("prev_avg_price", F.lag("avg_price").over(w))
        .withColumn(
            "price_change_pct",
            F.when(
                F.col("prev_avg_price").isNotNull() & (F.col("prev_avg_price") != 0),
                F.round((F.col("avg_price") - F.col("prev_avg_price")) / F.col("prev_avg_price") * 100, 2),
            ),
        )
        .orderBy("product_id", "snapshot_month")
    )
    overwrite_table(spark, hist, C.table(C.GOLD_NS, "agg_product_price_history"),
                    partition_by=["snapshot_month"])
    print(f"[gold] agg_product_price_history -> {hist.count()} lignes")


def main() -> int:
    p = argparse.ArgumentParser(description="Gold - marts & agregats temporels")
    p.add_argument("--branch", default=C.NESSIE_REF)
    args = p.parse_args()

    spark = build_spark(
        "gold_build",
        extra_conf={f"spark.sql.catalog.{C.CATALOG}.ref": args.branch},
    )
    ensure_namespaces(spark)

    build_dim_products(spark)
    build_dim_users(spark)
    build_fact_cart_items(spark)
    build_agg_sales_by_category_month(spark)
    build_agg_sales_by_region_month(spark)
    build_agg_product_price_history(spark)

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
