"""
Construction de la SparkSession de la plateforme.

La session est configuree pour :
  * le catalogue Iceberg "nessie" adosse a Project Nessie (REST API v2) ;
  * le stockage du warehouse Iceberg dans MinIO via S3FileIO (s3://warehouse) ;
  * la lecture de la zone brute JSON dans MinIO via le connecteur s3a://.

Ces `spark.*` sont poses au niveau de la SparkSession : Spark les propage
automatiquement aux executors (contrairement a des variables d'environnement),
ce qui evite d'avoir a injecter les secrets dans le conteneur worker.
"""
from __future__ import annotations

from pyspark.sql import SparkSession

from . import config as C


def build_spark(app_name: str, extra_conf: dict[str, str] | None = None) -> SparkSession:
    catalog = C.CATALOG

    conf: dict[str, str] = {
        # ---- Extensions SQL Iceberg + Nessie -----------------------------
        "spark.sql.extensions": (
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "org.projectnessie.spark.extensions.NessieSparkSessionExtensions"
        ),
        # ---- Catalogue Iceberg adosse a Nessie -------------------------
        f"spark.sql.catalog.{catalog}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{catalog}.catalog-impl": "org.apache.iceberg.nessie.NessieCatalog",
        f"spark.sql.catalog.{catalog}.uri": C.NESSIE_URI,
        f"spark.sql.catalog.{catalog}.ref": C.NESSIE_REF,
        f"spark.sql.catalog.{catalog}.warehouse": f"s3://{C.WAREHOUSE_BUCKET}",
        f"spark.sql.catalog.{catalog}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        f"spark.sql.catalog.{catalog}.s3.endpoint": C.S3_ENDPOINT,
        f"spark.sql.catalog.{catalog}.s3.path-style-access": "true",
        f"spark.sql.catalog.{catalog}.s3.access-key-id": C.S3_ACCESS_KEY,
        f"spark.sql.catalog.{catalog}.s3.secret-access-key": C.S3_SECRET_KEY,
        f"spark.sql.catalog.{catalog}.client.region": C.S3_REGION,
        "spark.sql.defaultCatalog": catalog,
        # ---- Acces zone brute JSON en s3a:// ------------------------
        "spark.hadoop.fs.s3a.endpoint": C.S3_ENDPOINT,
        "spark.hadoop.fs.s3a.access.key": C.S3_ACCESS_KEY,
        "spark.hadoop.fs.s3a.secret.key": C.S3_SECRET_KEY,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        # ---- Confort / robustesse -------------------------------
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.sources.partitionOverwriteMode": "dynamic",
    }
    if extra_conf:
        conf.update(extra_conf)

    builder = SparkSession.builder.appName(app_name)
    for k, v in conf.items():
        builder = builder.config(k, v)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def ensure_namespaces(spark: SparkSession) -> None:
    """Cree les namespaces bronze / silver / gold dans le catalogue Nessie."""
    for ns in (C.BRONZE_NS, C.SILVER_NS, C.GOLD_NS):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {C.CATALOG}.{ns}")
