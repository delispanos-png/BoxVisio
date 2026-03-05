# How To Replay DLQ

Replay all DLQ events for one tenant back to ingest queue:
```bash
cd /opt/cloudon-bi
./scripts/replay_dlq.sh <tenant_slug>
```

Replay only first N events:
```bash
./scripts/replay_dlq.sh <tenant_slug> 100
```

Then trigger queue drain:
```bash
./scripts/drain_queue.sh <tenant_slug> 1000
```

Verify DLQ/queue metrics:
```bash
curl -fsS http://localhost:8000/metrics | grep "cloudon_ingest_dlq_depth{tenant=\"<tenant_slug>\"}"
curl -fsS http://localhost:8000/metrics | grep "cloudon_ingest_queue_depth{tenant=\"<tenant_slug>\"}"
```

If replay causes repeated failures:
```bash
# increase retry backoff and reduce per-drain work
# /opt/cloudon-bi/.env
INGEST_RETRY_BACKOFF_SECONDS=15
INGEST_DRAIN_MAX_JOBS=50
docker compose -f docker-compose.yml -f infra/production/compose.prod.yml up -d --no-deps --force-recreate worker
```
