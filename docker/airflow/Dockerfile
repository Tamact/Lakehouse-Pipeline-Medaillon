# =============================================================================
#  Image Airflow de la plateforme  -  orchestration du medaillon
#  Base : apache/airflow 2.9.3 (Python 3.11)
#  Ajouts : JRE 17 + client Apache Spark 3.5.1 + jars Iceberg/Nessie/AWS
#           => Airflow soumet les jobs en "client mode" vers spark://spark-master
# =============================================================================
FROM apache/airflow:2.9.3-python3.11

USER root

# --- JRE 17 + outils (requis par spark-submit / driver Spark) --------------
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless curl procps bash; \
    rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# --- Client Apache Spark 3.5.1 (meme version que le cluster) --------------
ENV SPARK_VERSION=3.5.1 \
    HADOOP_PROFILE=3 \
    SPARK_HOME=/opt/spark
RUN set -eux; \
    curl -fSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" \
        -o /tmp/spark.tgz; \
    mkdir -p "${SPARK_HOME}"; \
    tar -xzf /tmp/spark.tgz -C "${SPARK_HOME}" --strip-components=1; \
    rm /tmp/spark.tgz
ENV PATH="${SPARK_HOME}/bin:${PATH}" \
    PYSPARK_PYTHON=python3

# --- Memes jars que l'image Spark (classpath identique driver/executors) --
ENV ICEBERG_VERSION=1.6.1 \
    NESSIE_VERSION=0.99.0 \
    HADOOP_AWS_VERSION=3.3.4 \
    AWS_SDK_V1_VERSION=1.12.262
COPY download-jars.sh /tmp/download-jars.sh
RUN chmod +x /tmp/download-jars.sh && /tmp/download-jars.sh "${SPARK_HOME}/jars"

# --- Config Spark partagee (endpoints MinIO, catalogue Nessie...) --------
COPY --chown=airflow:0 airflow/spark-defaults.conf ${SPARK_HOME}/conf/spark-defaults.conf

USER airflow

# --- Providers & libs Python cote Airflow -------------------------------
# Contrainte officielle Airflow 2.9.3 : evite qu'un provider force une
# montee de version d'apache-airflow (ex. vers la 3.x, incompatible ici).
ARG AIRFLOW_VERSION=2.9.3
ARG PYTHON_VERSION=3.11
RUN pip install --no-cache-dir \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
      "apache-airflow==${AIRFLOW_VERSION}" \
      "apache-airflow-providers-apache-spark" \
      "apache-airflow-providers-amazon" \
      "requests" \
      "boto3"
