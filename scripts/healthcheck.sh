#!/usr/bin/env bash
# Verifie la disponibilite des interfaces / endpoints de la plateforme.
set -u

check() {
  local name="$1" url="$2" extra="${3:-}"
  if curl -k -s -o /dev/null -w '%{http_code}' ${extra} --max-time 5 "$url" | grep -qE '^(2|3|401|403)'; then
    printf "  [ OK ] %-12s %s\n" "$name" "$url"
  else
    printf "  [FAIL] %-12s %s\n" "$name" "$url"
  fi
}

echo "== Etat des endpoints =="
check "MinIO"      "http://localhost:9000/minio/health/live"
check "MinIO-UI"   "http://localhost:9001"
check "Nessie"     "http://localhost:19120/q/health/live"
check "Nessie-API" "http://localhost:19120/api/v2/config"
check "Spark"      "http://localhost:8081"
check "Airflow"    "http://localhost:8080/health"
check "NiFi"       "https://localhost:8443/nifi-api/access/config"
check "Dremio"     "http://localhost:9047"
check "Prometheus" "http://localhost:9090/-/ready"
check "Grafana"    "http://localhost:3000/api/health"

echo
echo "== Conteneurs =="
docker compose ps --format 'table {{.Name}}\t{{.Status}}'
