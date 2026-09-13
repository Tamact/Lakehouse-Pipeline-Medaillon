#!/bin/bash
# =============================================================================
#  Initialisation PostgreSQL : cree les bases dediees Airflow et Nessie.
#  Execute automatiquement par l'image postgres (docker-entrypoint-initdb.d).
#  La base "lakehouse" (POSTGRES_DB) est deja creee par l'entrypoint standard.
# =============================================================================
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Base de metadonnees Airflow
    SELECT 'CREATE DATABASE ${POSTGRES_AIRFLOW_DB}'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${POSTGRES_AIRFLOW_DB}')\gexec

    -- Base du version store JDBC de Nessie
    SELECT 'CREATE DATABASE ${POSTGRES_NESSIE_DB}'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${POSTGRES_NESSIE_DB}')\gexec

    GRANT ALL PRIVILEGES ON DATABASE ${POSTGRES_AIRFLOW_DB} TO ${POSTGRES_USER};
    GRANT ALL PRIVILEGES ON DATABASE ${POSTGRES_NESSIE_DB}  TO ${POSTGRES_USER};
EOSQL

echo "[postgres-init] Bases '${POSTGRES_AIRFLOW_DB}' et '${POSTGRES_NESSIE_DB}' pretes."
