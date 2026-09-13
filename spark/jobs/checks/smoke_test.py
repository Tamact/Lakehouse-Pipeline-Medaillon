"""
Controle qualite final du pipeline : verifie que les tables du medaillon
existent et ne sont pas vides, et affiche un apercu des Gold (pour les logs
Airflow et la demo video).
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common import config as C  # noqa: E402
from common.io_utils import table_exists  # noqa: E402
from common.spark_session import build_spark  # noqa: E402

EXPECTED = {
    C.BRONZE_NS: ["products_raw", "users_raw", "carts_raw"],
    C.SILVER_NS: ["products", "users", "carts", "cart_items"],
    C.GOLD_NS: [
        "dim_products", "dim_users", "fact_cart_items",
        "agg_sales_by_category_month", "agg_sales_by_region_month",
        "agg_product_price_history",
    ],
}


def main() -> int:
    spark = build_spark("smoke_test_medallion")
    errors: list[str] = []

    for ns, tables in EXPECTED.items():
        for t in tables:
            fqn = C.table(ns, t)
            if not table_exists(spark, fqn):
                errors.append(f"MANQUANTE : {fqn}")
                continue
            n = spark.table(fqn).count()
            flag = "OK " if n > 0 else "VIDE"
            print(f"[{flag}] {fqn:45s} {n:>10,} lignes")
            if n == 0 and ns != C.BRONZE_NS:
                errors.append(f"VIDE : {fqn}")

    print("\n--- Apercu : gold.agg_sales_by_category_month (tendance 6 mois) ---")
    spark.table(C.table(C.GOLD_NS, "agg_sales_by_category_month")) \
        .orderBy("snapshot_month", "category").show(24, truncate=False)

    spark.stop()
    if errors:
        print("\nECHEC du smoke test :\n  - " + "\n  - ".join(errors))
        return 1
    print("\nSmoke test : toutes les tables du medaillon sont presentes et peuplees.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
