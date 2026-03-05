# Monitoring & Alerting Baseline

This folder provides a production baseline for observability:
- Prometheus scrape config + alert rules
- Grafana dashboard JSON
- Alertmanager sample config
- Optional monitoring stack compose file

## Files
- `prometheus/prometheus.yml`
- `prometheus/rules/cloudon_alerts.yml`
- `grafana/dashboards/cloudon-bi-overview.json`
- `alertmanager/alertmanager.yml`
- `compose.monitoring.yml`

## Metrics used
Application `/metrics` (FastAPI):
- `cloudon_http_request_duration_seconds_bucket`
- `cloudon_http_requests_total`
- `cloudon_app_errors_total`
- `cloudon_ingest_jobs_total`
- `cloudon_ingest_jobs_tenant_total`
- `cloudon_ingest_retries_total`
- `cloudon_ingest_dead_letters_total`
- `cloudon_ingest_queue_depth{tenant=...}`
- `cloudon_ingest_dlq_depth{tenant=...}`
- `cloudon_db_pool_*`

Exporter metrics:
- Postgres exporter: `pg_stat_activity_count`, `up{job="postgres_exporter"}`
- Node exporter: `node_filesystem_*`, plus textfile backup metrics

## Bring up monitoring stack (optional)
```bash
cd /opt/cloudon-bi/infra/monitoring
docker compose -f compose.monitoring.yml up -d
```

If your compose project/network name differs from `cloudon_bi_bi_net`, update `compose.monitoring.yml` -> `networks.bi_app.name`.

## Grafana import
1. Open Grafana (`http://<server>:3000`).
2. Add Prometheus datasource.
3. Import `grafana/dashboards/cloudon-bi-overview.json`.

## Backup metrics wiring
`nightly_backup.sh` can export Prometheus textfile metrics when `BACKUP_METRICS_FILE` is set.

Example `.env` value:
```env
BACKUP_METRICS_FILE=/opt/cloudon-bi/infra/monitoring/node-exporter-textfiles/cloudon_backup.prom
```

Then run backup script via cron/systemd timer as already defined.

## Alert rules covered
- high API error rate
- queue stuck (no progress)
- DLQ spike
- worker failures/retries high
- backup failure / stale backup
- low disk space
- postgres down
- DB pool saturation
