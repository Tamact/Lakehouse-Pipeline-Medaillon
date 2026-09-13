#!/usr/bin/env bash
# Recupere / (re)definit les identifiants de l'UI NiFi (Partie 2 - point d'attention).
set -euo pipefail
CONTAINER="${NIFI_CONTAINER:-lakehouse-nifi}"

echo "== 1) Identifiants generes automatiquement (si SINGLE_USER_CREDENTIALS_* absents) =="
docker logs "${CONTAINER}" 2>&1 | grep -E "Generated (Username|Password)" || \
  echo "   (aucun identifiant genere trouve dans les logs -> mode single-user fixe actif)"

echo
echo "== 2) Forcer un mot de passe connu (>= 12 caracteres) =="
echo "   docker exec -it ${CONTAINER} ./bin/nifi.sh set-single-user-credentials admin nifiAdminPass2026"

echo
echo "== 3) UI =="
echo "   https://localhost:${NIFI_WEB_HTTPS_PORT:-8443}/nifi   (certificat auto-signe : accepter l'exception)"
