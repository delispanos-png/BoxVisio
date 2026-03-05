# How To Drain Queues

Drain one tenant queue immediately:
```bash
cd /opt/cloudon-bi
./scripts/drain_queue.sh <tenant_slug> 1000
```

Tune max jobs per drain loop globally (recommended for fairness):
```bash
# /opt/cloudon-bi/.env
INGEST_DRAIN_MAX_JOBS=100
docker compose -f docker-compose.yml -f infra/production/compose.prod.yml up -d --no-deps --force-recreate worker
```

Check queue depth before/after:
```bash
curl -fsS http://localhost:8000/metrics | grep "cloudon_ingest_queue_depth{tenant=\"<tenant_slug>\"}"
```

If backlog is large, temporarily scale workers:
```bash
docker compose -f docker-compose.yml -f infra/production/compose.prod.yml up -d --scale worker=4 worker
```

Return to normal scale after recovery:
```bash
docker compose -f docker-compose.yml -f infra/production/compose.prod.yml up -d --scale worker=1 worker
```

If retries are too aggressive, increase backoff:
```bash
# /opt/cloudon-bi/.env
INGEST_RETRY_BACKOFF_SECONDS=10
docker compose -f docker-compose.yml -f infra/production/compose.prod.yml up -d --no-deps --force-recreate worker
```
