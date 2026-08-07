SHELL := /bin/bash
COMPOSE := docker compose

.PHONY: help init-env config preflight build up up-core up-lakehouse up-orchestration up-quality up-ai up-observability \
	down bootstrap bootstrap-core submit-flink dbt-run quality rag-index ai-models status logs test smoke clean

COMPOSE_TARGETS := config build up up-core up-lakehouse up-orchestration up-quality up-ai up-observability \
	down bootstrap bootstrap-core submit-flink dbt-run quality rag-index ai-models status logs smoke clean

$(COMPOSE_TARGETS): init-env

help:
	@sed -n 's/^## //p' Makefile

## init-env               Create .env and generate a unique local Airflow Fernet key.
init-env:
	bash scripts/init-env.sh

## config                 Render and validate the Compose model.
config:
	$(COMPOSE) --profile core --profile lakehouse --profile orchestration --profile quality --profile ai --profile observability --profile tools config --quiet

## preflight              Verify host and Docker storage before starting stateful services.
preflight:
	bash scripts/preflight.sh

## build                  Build all custom local images.
build: preflight
	$(COMPOSE) --profile core --profile lakehouse --profile orchestration --profile quality --profile ai --profile tools build

## up-core                Start sources, Kafka, Debezium, StarRocks, generators; then bootstrap them.
up-core: preflight
	$(COMPOSE) --profile core up -d --build
	$(MAKE) bootstrap-core

## up-lakehouse           Start the core plus SeaweedFS and Flink; submit streaming jobs.
up-lakehouse: preflight
	$(COMPOSE) --profile core --profile lakehouse up -d --build
	$(MAKE) bootstrap
	$(MAKE) submit-flink

## up-orchestration       Add Airflow and its local metadata database.
up-orchestration: preflight
	$(COMPOSE) --profile orchestration up -d --build

## up-quality             Run an on-demand Soda scan (Airflow also schedules it hourly).
up-quality:
	$(MAKE) quality

## up-ai                  Start Weaviate, Ollama, and the RAG API (models are a separate download).
up-ai: preflight
	$(COMPOSE) --profile ai up -d --build

## up-observability       Start Prometheus, Kafka exporter, and Grafana.
up-observability:
	$(COMPOSE) --profile observability up -d

## up                     Start every profile. This needs substantially more than the core profile.
up: preflight
	$(COMPOSE) --profile core --profile lakehouse --profile orchestration --profile ai --profile observability up -d --build
	$(MAKE) bootstrap
	$(MAKE) submit-flink

## bootstrap-core        Idempotently create Kafka topics, StarRocks tables, and CDC connectors.
bootstrap-core:
	$(COMPOSE) --profile tools run --rm -e BOOTSTRAP_LAKEHOUSE=false bootstrap

## bootstrap             Bootstrap core services plus the SeaweedFS Iceberg namespace.
bootstrap:
	$(COMPOSE) --profile tools run --rm -e BOOTSTRAP_LAKEHOUSE=true bootstrap

## submit-flink          Submit the SQL streaming jobs idempotently when none are running.
submit-flink:
	$(COMPOSE) --profile lakehouse --profile lakehouse-tools run --rm flink-submit

## dbt-run               Build and test the StarRocks analytical models.
dbt-run:
	$(COMPOSE) --profile orchestration run --rm airflow-cli bash -lc 'cd /opt/platform/dbt && dbt build --profiles-dir .'

## quality               Run Soda Core checks against StarRocks.
quality:
	$(COMPOSE) --profile quality run --rm quality

## ai-models             Download the configured open local embedding and chat models.
ai-models:
	$(COMPOSE) --profile ai up -d ollama weaviate
	$(COMPOSE) exec ollama ollama pull $${OLLAMA_EMBED_MODEL:-nomic-embed-text}
	$(COMPOSE) exec ollama ollama pull $${OLLAMA_CHAT_MODEL:-qwen3:1.7b}

## rag-index             Index local contracts, dbt metadata, checks, DAGs, and docs in Weaviate.
rag-index:
	$(COMPOSE) --profile ai run --rm rag-index

## status                Show service and Flink job status.
status:
	$(COMPOSE) ps
	@curl -fsS http://localhost:8081/jobs/overview 2>/dev/null || true

## logs                  Follow logs for running services.
logs:
	$(COMPOSE) logs -f --tail=150

## test                  Run static contracts and Compose validation.
test: config
	python3 -m unittest discover -s tests -v
	python3 -m compileall -q services airflow/dags quality

## smoke                 Verify live endpoints, topics, CDC, StarRocks, Iceberg, and optional services.
smoke:
	$(COMPOSE) --profile tools run --rm smoke

## down                  Stop services while preserving named volumes.
down:
	$(COMPOSE) --profile core --profile lakehouse --profile orchestration --profile quality --profile ai --profile observability --profile tools down

## clean                 Delete local containers and named volumes. DATA LOSS: generated local data is removed.
clean:
	$(COMPOSE) --profile core --profile lakehouse --profile orchestration --profile quality --profile ai --profile observability --profile tools down --volumes --remove-orphans
