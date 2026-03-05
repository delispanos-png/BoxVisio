# Production Backup + Restore Verification

## Objective
Prove backups are valid and restores are executable in production-like conditions.

## A) Backup Verification

1. Trigger backup:
```bash
cd /opt/cloudon-bi
./scripts/nightly_backup.sh
```

2. Verify latest backup folder:
```bash
ls -1dt /opt/cloudon-bi/backups/20* | head -n 1
```

3. Check manifest contains control + tenant dumps:
```bash
LATEST=$(ls -1dt /opt/cloudon-bi/backups/20* | head -n 1)
cat "$LATEST/manifest.txt"
```

4. Validate metrics file (if enabled):
```bash
grep -E 'cloudon_backup_success|cloudon_backup_last_success_timestamp_seconds' /opt/cloudon-bi/infra/monitoring/node-exporter-textfiles/cloudon_backup.prom
```

## B) Restore Drill - Control DB

1. Restore into same DB (maintenance window):
```bash
cd /opt/cloudon-bi
LATEST=$(ls -1dt /opt/cloudon-bi/backups/20* | head -n 1)
./scripts/restore_db.sh --db bi_control --file "$LATEST/control/bi_control.dump" --drop-create
```

2. Validate critical tables exist:
```bash
docker compose -f docker-compose.yml -f infra/production/compose.prod.yml exec -T postgres \
  psql -U "$TENANT_DB_SUPERUSER" -d bi_control -c "\dt"
```

## C) Restore Drill - Tenant DB

1. Choose one tenant DB from control DB.
2. Restore selected tenant dump:
```bash
cd /opt/cloudon-bi
LATEST=$(ls -1dt /opt/cloudon-bi/backups/20* | head -n 1)
./scripts/restore_db.sh --db bi_tenant_<slug> --file "$LATEST/tenants/bi_tenant_<slug>.dump" --drop-create
```

3. Validate KPI query still works for tenant.

## D) Acceptance Criteria
- Backup script completes with exit code 0.
- Manifest includes all expected DB dumps.
- Control DB restore succeeds and app can read tenants.
- Tenant restore succeeds and KPI endpoints respond.
