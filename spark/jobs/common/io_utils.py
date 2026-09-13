"""
Helpers d'ecriture Iceberg (creation de table + upsert idempotent via MERGE).
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def table_exists(spark: SparkSession, table_fqn: str) -> bool:
    """Test d'existence robuste pour un catalogue v2 (Iceberg/Nessie)."""
    try:
        spark.sql(f"DESCRIBE TABLE {table_fqn}")
        return True
    except Exception:
        return False


def create_table_if_absent(
    spark: SparkSession,
    df: DataFrame,
    table_fqn: str,
    partition_by: list[str] | None = None,
) -> bool:
    """
    Cree la table Iceberg `table_fqn` avec le schema de `df` si elle n'existe pas.
    Retourne True si la table vient d'etre creee (donc deja peuplee par ce CTAS).
    """
    if table_exists(spark, table_fqn):
        return False

    df.createOrReplaceTempView("_schema_src")
    parts = f"PARTITIONED BY ({', '.join(partition_by)})" if partition_by else ""
    spark.sql(
        f"CREATE TABLE {table_fqn} USING iceberg {parts} "
        f"AS SELECT * FROM _schema_src"
    )
    return True


def merge_upsert(
    spark: SparkSession,
    df: DataFrame,
    table_fqn: str,
    key_cols: list[str],
    partition_by: list[str] | None = None,
    update: bool = True,
) -> None:
    """
    Upsert idempotent de `df` dans `table_fqn` :
      * cree la table au premier passage (CTAS) ;
      * sinon MERGE sur `key_cols` : INSERT si absent, UPDATE si present (optionnel).
    """
    just_created = create_table_if_absent(spark, df, table_fqn, partition_by)
    if just_created:
        return

    df.createOrReplaceTempView("_merge_src")
    on_clause = " AND ".join([f"t.{c} = s.{c}" for c in key_cols])
    matched = "WHEN MATCHED THEN UPDATE SET *" if update else ""
    spark.sql(
        f"""
        MERGE INTO {table_fqn} AS t
        USING _merge_src AS s
        ON {on_clause}
        {matched}
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def overwrite_table(
    spark: SparkSession,
    df: DataFrame,
    table_fqn: str,
    partition_by: list[str] | None = None,
) -> None:
    """Reconstruit entierement une table (usage : couches Gold recalculees)."""
    writer = df.writeTo(table_fqn).using("iceberg")
    if partition_by:
        from pyspark.sql.functions import col

        writer = writer.partitionedBy(*[col(c) for c in partition_by])
    writer.createOrReplace()
