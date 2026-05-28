# How To Refresh Pilot Tenant Data

This runbook refreshes the pilot tenant (`id=1`) with a snapshot of real customer data from tenant `id=5`.

Use this when UI work needs fresh, realistic data without touching the production tenant.

## Scope
- Source tenant: `id=5`
- Target tenant: `id=1`
- Target database is replaced.
- Target tenant record, users, login, and tenant id remain unchanged.
- This is a snapshot refresh, not a live mirror.

## Fast Path
From the project root:

```bash
cd /opt/cloudon-bi
make pilot-refresh-dry-run
make pilot-refresh-execute
```

The dry-run shows exactly which source and target databases will be used.

## Direct Script
```bash
cd /opt/cloudon-bi
scripts/refresh_pilot_tenant_db.sh
scripts/refresh_pilot_tenant_db.sh --execute
```

## What The Job Does
1. Reads source/target tenant metadata from the control database.
2. Refuses any target other than tenant `id=1`.
3. Backs up the current tenant `1` database.
4. Dumps the tenant `5` database.
5. Stops `api`, `worker`, `worker_beat`, and `worker_delete`.
6. Drops/recreates the tenant `1` database.
7. Restores tenant `5` data into tenant `1`.
8. Runs `ANALYZE`.
9. Starts services again.
10. Prints basic row-count validation.

## Backup Location
Each run creates:

```text
backups/pilot_refresh/<timestamp>/
```

Inside it:
- `target_1_<db>_before_refresh.dump`
- `source_5_<db>.dump`

## Rollback
Use the target backup produced by the run:

```bash
cd /opt/cloudon-bi
./scripts/restore_db.sh --db 'R&DDB' --file backups/pilot_refresh/<timestamp>/target_1_R&DDB_before_refresh.dump --drop-create
docker compose restart api worker worker_beat worker_delete
```

## Validation
After refresh:

```bash
docker compose ps
curl -kfsS -H 'Host: bi.boxvisio.com' https://127.0.0.1/ready
```

Then open tenant `1` and check the dashboard KPIs with the refreshed data.

## Safety Notes
- Do not run this against tenant `5` as target.
- Do not bypass the dry-run when unsure.
- The script intentionally refuses target ids other than `1`.
- The production tenant `5` is only read through `pg_dump`.
