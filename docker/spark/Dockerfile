# =============================================================================
#  Image Spark de la plateforme  -  cluster standalone (master + worker)
#  Base : image officielle "spark" (Docker Official Image) 3.5.1 + Python 3
#  Ajouts : jars Iceberg / Nessie / AWS + libs Python pour les jobs medaillon
# =============================================================================
FROM spark:3.5.1-python3

USER root

# --- Outils systeme ---------------------------------------------------------
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends curl bash procps tini; \
    rm -rf /var/lib/apt/lists/*

# --- Dependances Python des jobs Spark ------------------------------------
RUN pip install --no-cache-dir \
      requests==2.32.3 \
      boto3==1.34.162

# --- Jars du classpath Spark (Iceberg + Nessie + S3A) --------------------
ENV ICEBERG_VERSION=1.6.1 \
    NESSIE_VERSION=0.99.0 \
    HADOOP_AWS_VERSION=3.3.4 \
    AWS_SDK_V1_VERSION=1.12.262
COPY download-jars.sh /tmp/download-jars.sh
RUN chmod +x /tmp/download-jars.sh && /tmp/download-jars.sh /opt/spark/jars

# --- Repertoire des event logs (Spark History / debug) -----------------
RUN mkdir -p /opt/spark/spark-events && chown -R spark:spark /opt/spark/spark-events /opt/spark/jobs 2>/dev/null || true

ENV PYSPARK_PYTHON=/usr/bin/python3 \
    PYSPARK_DRIVER_PYTHON=/usr/bin/python3

USER spark
WORKDIR /opt/spark
