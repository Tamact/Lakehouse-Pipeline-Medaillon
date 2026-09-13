# =============================================================================
#  Plateforme Data Lakehouse - raccourcis d'exploitation
#  Usage : make <cible>   (voir "make help")
# =============================================================================
SHELL := /bin/bash
COMPOSE := docker compose

.DEFAULT_GOAL := help

.PHONY: help
help: ## Affiche cette aide
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
	 awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------------------
#  Cycle de vie de la stack
# ---------------------------------------------------------------------------
.PHONY: build
build: ## Construit les images custom (spark, airflow)
	$(COMPOSE) build

.PHONY: up
up: ## Demarre toute la plateforme
	$(COMPOSE) up -d
	@echo "Interfaces :"
	@echo "  MinIO      http://localhost:9001   (minioadmin / minioadmin123)"
	@echo "  NiFi       https://localhost:8443/nifi   (admin / nifiAdminPass2026)"
	@echo "  Airflow    http://localhost:8080   (admin / admin)"
	@echo "  Spark      http://localhost:8081"
	@echo "  Dremio     http://localhost:9047"
	@echo "  Nessie     http://localhost:19120"
	@echo "  Prometheus http://localhost:9090"
	@echo "  Grafana    http://localhost:3000   (admin / admin)"

.PHONY: down
down: ## Arrete la plateforme (conserve les volumes)
	$(COMPOSE) down

.PHONY: destroy
destroy: ## Arrete ET supprime les volumes (remise a zero totale)
	$(COMPOSE) down -v --remove-orphans

.PHONY: ps
ps: ## Etat des conteneurs
	$(COMPOSE) ps

.PHONY: logs
logs: ## Suit les logs (SERVICE=nifi pour cibler)
	$(COMPOSE) logs -f $(SERVICE)

.PHONY: health
health: ## Verifie l'etat de sante de chaque service
	@bash scripts/healthcheck.sh

# ---------------------------------------------------------------------------
#  Provisioning applicatif
# ---------------------------------------------------------------------------
.PHONY: nifi-flow
nifi-flow: ## Amorce le Parameter Context + Process Group NiFi
	python nifi/scripts/build_flow.py

.PHONY: dremio-setup
dremio-setup: ## Cree l'utilisateur admin + la source Nessie dans Dremio
	python dremio/scripts/configure_dremio.py

.PHONY: backfill
backfill: ## Declenche le backfill 6 mois (via Airflow CLI)
	$(COMPOSE) exec airflow-scheduler airflow dags trigger lakehouse_backfill_history

.PHONY: run-pipeline
run-pipeline: ## Declenche un run complet du medaillon
	$(COMPOSE) exec airflow-scheduler airflow dags trigger lakehouse_medallion

.PHONY: ingest
ingest: ## Ingestion NiFi ponctuelle (SNAP=YYYY-MM-DD CART_LIMIT=N)
	bash nifi/scripts/trigger_ingest.sh $(SNAP) products,users,carts $(CART_LIMIT)

# ---------------------------------------------------------------------------
#  Spark en ligne de commande (debug hors Airflow)
# ---------------------------------------------------------------------------
.PHONY: spark-shell
spark-shell: ## Ouvre un shell dans le conteneur spark-master
	$(COMPOSE) exec spark-master bash

.PHONY: submit-%
submit-%: ## make submit-bronze DOMAIN=products | submit-silver | submit-gold
	$(COMPOSE) exec spark-master /opt/spark/bin/spark-submit \
	  --master spark://spark-master:7077 \
	  /opt/spark/jobs/$*/$(if $(filter gold,$*),gold_build.py,$(if $(filter bronze,$*),bronze_ingest.py,silver_build.py)) \
	  $(if $(DOMAIN),--domain $(DOMAIN),)

.PHONY: smoke
smoke: ## Lance le smoke test du medaillon
	$(COMPOSE) exec spark-master /opt/spark/bin/spark-submit \
	  --master spark://spark-master:7077 /opt/spark/jobs/checks/smoke_test.py

# ---------------------------------------------------------------------------
#  Qualite du code
# ---------------------------------------------------------------------------
.PHONY: lint
lint: ## Verifie la syntaxe Python de tous les jobs et DAGs
	python -m py_compile $$(find spark/jobs airflow/dags -name '*.py')
	@echo "Syntaxe OK"
