SHELL := /bin/bash

DC := docker compose
TENANT ?=
NAME ?=
ADMIN_EMAIL ?=
PLAN ?=standard
SOURCE ?=external
API_BASE ?=http://localhost:8000
CLOUDON_TOKEN ?=
SERVICE ?=
DB ?=
FILE ?=
DROP_CREATE ?=false

.PHONY: up down logs migrate-control migrate-tenant create-tenant seed-admin check-migrations backup-nightly restore-db

up:
	$(DC) up --build -d

down:
	$(DC) down

logs:
ifeq ($(strip $(SERVICE)),)
	$(DC) logs -f --tail=200
else
	$(DC) logs -f --tail=200 $(SERVICE)
endif

migrate-control:
	$(DC) exec api alembic -c alembic.ini upgrade 20260228_0010_control

migrate-tenant:
ifndef TENANT
	$(error TENANT is required. Usage: make migrate-tenant TENANT=pharma-a)
endif
	$(DC) exec api python /app/scripts/run_tenant_migrations.py --tenant $(TENANT)

create-tenant:
ifndef NAME
	$(error NAME is required. Usage: make create-tenant NAME="Pharma A" TENANT=pharma-a ADMIN_EMAIL=admin@pharma.gr CLOUDON_TOKEN=<jwt>)
endif
ifndef TENANT
	$(error TENANT is required. Usage: make create-tenant NAME="Pharma A" TENANT=pharma-a ADMIN_EMAIL=admin@pharma.gr CLOUDON_TOKEN=<jwt>)
endif
ifndef ADMIN_EMAIL
	$(error ADMIN_EMAIL is required. Usage: make create-tenant NAME="Pharma A" TENANT=pharma-a ADMIN_EMAIL=admin@pharma.gr CLOUDON_TOKEN=<jwt>)
endif
ifndef CLOUDON_TOKEN
	$(error CLOUDON_TOKEN is required. Usage: make create-tenant ... CLOUDON_TOKEN=<jwt>)
endif
	curl -fsS -X POST $(API_BASE)/v1/admin/tenants \
	  -H "Authorization: Bearer $(CLOUDON_TOKEN)" \
	  -H "Content-Type: application/json" \
	  -d '{"name":"$(NAME)","slug":"$(TENANT)","admin_email":"$(ADMIN_EMAIL)","plan":"$(PLAN)","source":"$(SOURCE)"}'

seed-admin:
	$(DC) exec api python /app/scripts/seed_admin.py

check-migrations:
	$(DC) exec api sh -lc 'MIGRATION_TARGET=control alembic -c alembic.ini heads'
	$(DC) exec api sh -lc 'MIGRATION_TARGET=tenant alembic -c alembic.ini heads'

backup-nightly:
	./scripts/nightly_backup.sh

restore-db:
ifndef DB
	$(error DB is required. Usage: make restore-db DB=bi_control FILE=/path/to/backup.dump DROP_CREATE=true)
endif
ifndef FILE
	$(error FILE is required. Usage: make restore-db DB=bi_control FILE=/path/to/backup.dump DROP_CREATE=true)
endif
ifeq ($(DROP_CREATE),true)
	./scripts/restore_db.sh --db $(DB) --file $(FILE) --drop-create
else
	./scripts/restore_db.sh --db $(DB) --file $(FILE)
endif
