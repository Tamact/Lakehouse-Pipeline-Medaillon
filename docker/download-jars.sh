#!/usr/bin/env bash
# =============================================================================
#  Telecharge dans $JARS_DIR les dependances necessaires a Spark pour :
#    - le format de table Iceberg  (iceberg-spark-runtime)
#    - le catalogue Nessie         (nessie-spark-extensions + iceberg-nessie inclus)
#    - l'acces objet S3/MinIO      (iceberg-aws-bundle pour S3FileIO du warehouse
#                                   + hadoop-aws / aws-java-sdk-bundle pour lire
#                                   la zone brute JSON en s3a://)
#
#  Compatibilite : Spark 3.5.x / Scala 2.12 / Hadoop 3.3.4
#  Utilise a l'identique par l'image Spark ET l'image Airflow (meme classpath).
# =============================================================================
set -euo pipefail

JARS_DIR="${1:?Usage: download-jars.sh <jars_dir>}"
mkdir -p "${JARS_DIR}"

ICEBERG_VERSION="${ICEBERG_VERSION:-1.6.1}"
NESSIE_VERSION="${NESSIE_VERSION:-0.99.0}"
HADOOP_AWS_VERSION="${HADOOP_AWS_VERSION:-3.3.4}"
AWS_SDK_V1_VERSION="${AWS_SDK_V1_VERSION:-1.12.262}"

MAVEN="https://repo1.maven.org/maven2"

fetch() {
  local url="$1" ; local out="${JARS_DIR}/$(basename "$url")"
  if [ -f "$out" ]; then echo "  = deja present : $(basename "$url")"; return; fi
  echo "  + $(basename "$url")"
  curl -fSL --retry 4 --retry-delay 3 -o "$out" "$url"
}

echo "[jars] Telechargement vers ${JARS_DIR}"
fetch "${MAVEN}/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar"
fetch "${MAVEN}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"
fetch "${MAVEN}/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_2.12/${NESSIE_VERSION}/nessie-spark-extensions-3.5_2.12-${NESSIE_VERSION}.jar"
fetch "${MAVEN}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar"
fetch "${MAVEN}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_V1_VERSION}/aws-java-sdk-bundle-${AWS_SDK_V1_VERSION}.jar"
echo "[jars] Termine :"
ls -1 "${JARS_DIR}"
