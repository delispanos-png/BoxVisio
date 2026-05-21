# BoxVisio BI Operations Handover

Last updated: 2026-05-10

This file is the working handover for BoxVisio BI. It exists so a new session, a new server, or a new operator can quickly understand where the project stands and continue without losing context.

## Project Snapshot

BoxVisio BI is a multi-tenant BI/admin platform for ERP and e-shop operational reporting.

Main tenant currently under active work:

- `pharmacy295`

Important production domain:

- `bi.boxvisio.com`

Current live server after migration:

- Hostname: `BoxVisioSrv01`
- Public IP: `178.105.108.10`
- Project path: `/opt/cloudon-bi`
- Docker Compose project: `cloudon_bi`
- Compose file: `/opt/cloudon-bi/docker-compose.yml`

Previous server IP:

- `159.69.130.78`

## Current Runtime

Expected running services on the new server:

- `api`
- `nginx`
- `postgres`
- `redis`
- `worker`
- `worker_beat`
- `worker_delete`

Check status:

```bash
cd /opt/cloudon-bi
docker compose ps
```

Public health check:

```bash
curl -k -I https://bi.boxvisio.com/health
```

Expected result:

- HTTP `200`
- JSON body from `/health`: `{"status":"ok","service":"CloudOn BI"}`

## Migration Notes

The migration from the old server to the new server has been performed.

Completed:

- Installed Docker / Docker Compose / Python dependencies / Microsoft ODBC Driver 18.
- Synced the real production working tree from `/opt/cloudon-bi`, not only the Git repository.
- Transferred `.env`.
- Transferred TLS certificates from `/etc/letsencrypt`.
- Restored databases from backup:
  - `bi_control_recovery`
  - `bi_tenant_pharmacy295`
  - `bi_tenant_uat-b`
- Confirmed nginx/TLS works for `bi.boxvisio.com`.
- DNS for `bi.boxvisio.com` now points to `178.105.108.10`.
- Stopped workers on the old server after cutover.
- Started workers on the new server.

Important fix after DNS cutover:

- The dashboard initially returned `500`.
- Root cause: tenant DB roles/passwords from PostgreSQL globals were not applied after restore.
- Applied `globals.sql` from the backup and granted tenant DB permissions.
- Restarted API.
- Dashboard API stopped returning tenant DB authentication errors.

Second fix after cutover:

- The dashboard endpoint then hit nginx `504` on slow first KPI requests.
- Increased nginx proxy timeout in:
  - `infra/nginx/bi.boxvisio.com.conf`

The timeout fix was synced to the new server and nginx was restarted.

## Backup And Restore Notes

Backup script:

```bash
./scripts/nightly_backup.sh
```

Known issue:

- The `.env` file may not be shell-safe if values contain spaces.
- To run backup safely, use explicit env and bypass sourcing `.env`:

```bash
CONTROL_DB_NAME=bi_control_recovery TENANT_DB_SUPERUSER=postgres ENV_FILE=/dev/null ./scripts/nightly_backup.sh
```

Restore script:

```bash
./scripts/restore_db.sh
```

Example:

```bash
ENV_FILE=/dev/null TENANT_DB_SUPERUSER=postgres ./scripts/restore_db.sh \
  --db bi_tenant_pharmacy295 \
  --file backups/<backup-id>/tenants/bi_tenant_pharmacy295.dump \
  --drop-create
```

After restore, remember:

- Apply `control/globals.sql` to recreate tenant DB roles.
- Re-grant tenant DB permissions if restore used `--no-owner --no-privileges`.
- Restart API/workers so old connection pools do not keep stale auth.

## Operational Commands

New server shell:

```bash
ssh root@178.105.108.10
cd /opt/cloudon-bi
```

Service status:

```bash
docker compose ps
```

Logs:

```bash
docker compose logs --tail 120 api
docker compose logs --tail 120 worker worker_beat worker_delete
docker compose logs --tail 120 nginx
```

Restart API:

```bash
docker compose restart api
```

Restart nginx:

```bash
docker compose exec -T nginx nginx -t
docker compose restart nginx
```

Start workers:

```bash
docker compose up -d worker worker_beat worker_delete
```

Stop workers:

```bash
docker compose stop worker worker_beat worker_delete
```

Docker cache cleanup:

```bash
systemctl status cloudon-docker-cleanup.timer --no-pager
systemctl list-timers --all --no-pager | grep cloudon-docker-cleanup
DOCKER_CLEANUP_DRY_RUN=1 /opt/cloudon-bi/scripts/docker_cache_cleanup.sh
```

The cleanup is intentionally conservative:

- it prunes stopped containers older than 7 days
- it prunes unused/dangling images older than 7 days
- it prunes Docker builder cache when supported
- it never prunes Docker volumes
- it never removes running containers

The timer definition lives in:

- `infra/systemd/cloudon-docker-cleanup.service`
- `infra/systemd/cloudon-docker-cleanup.timer`

Check DNS:

```bash
dig +short bi.boxvisio.com
```

Expected:

```text
178.105.108.10
```

## Current Product Work Context

The active product direction is to make the tenant environment consistent, fast, and reliable across all business circuits.

Major active themes:

- Unified UI/UX across tenant modules.
- Faster dashboard loading from local BI database.
- Correct KPI comparisons against the same previous-year period, excluding the current incomplete day where appropriate.
- E-shop analysis with clear popup drilldowns.
- Sales, purchases, expenses, and other document-based circuits should follow the same interaction model where possible.
- Tenant-level sync profiles and advanced per-stream sync settings.
- Automatic sync should respect tenant-specific schedules and business operating hours.
- Sync should avoid unnecessary SQL calls when a business is closed.
- Reconciliation/audit process should compare SoftOne data against BI data per tenant and alert on differences.
- SoftOne JavaScript bridge must stay updated with the backend ingestion model.

## Known Recent Fixes

Recent fixes and decisions:

- E-shop branch filter had an issue where branches did not populate. A fix was made in:
  - `backend/app/templates/tenant/eshop_analysis_dashboard.html`
- Executive KPI cards/popup behavior had issues. Work was made in:
  - `backend/app/templates/tenant/dashboard.html`
- Nginx timeout was increased in:
  - `infra/nginx/bi.boxvisio.com.conf`
- Tenant DB roles/grants were fixed on the new server after restore.

## Open Issues / Next Priorities

1. Dashboard performance

   The executive dashboard still needs deeper optimization. Data is local, so slow KPI loading likely means query/index/cache inefficiency rather than SoftOne latency.

2. UI/UX audit

   The tenant environment needs a general audit so all circuits use the same:

   - form style
   - table density
   - buttons
   - typography
   - colors
   - modal/popup behavior

3. KPI drilldowns

   Executive dashboard cards should have professional popup drilldowns where meaningful.

4. KPI deltas

   Delta vs previous year must be verified carefully:

   - Compare current period to the exact equivalent previous-year period.
   - If the current day is incomplete, compare only through the previous completed day.

5. SoftOne bridge

   The latest ingestion assumptions must also be reflected in the SoftOne JavaScript bridge files under:

   - `integrations/softone/`

6. Reconciliation

   Add tenant-level scheduled validation:

   - configured time of day
   - all business circuits
   - compare ERP source totals vs BI totals
   - notify/report differences

7. Security hardening

   After migration stabilizes:

   - rotate root password
   - prefer SSH key login
   - consider disabling password login/root login if operationally acceptable

## Git / Deployment Reminder

There are often production changes in the working tree. Do not assume Git alone has every live change unless verified.

Before changing server or deploying:

```bash
git status --short
git diff --stat
```

When moving to another server:

- Use Git for tracked source.
- Use `rsync` for live `.env`, certificates, backups, and any uncommitted production files.
- Never rely only on a fresh clone if the production tree has uncommitted changes.

## Imported Codex History

The latest Codex session history from the previous server has been imported to the new server.

Quick context file:

```bash
cat /opt/cloudon-bi/docs/OLD_CODEX_CONTEXT.md
```

Readable transcript:

```text
/opt/cloudon-bi/forensics/old-server-codex-sessions/old_codex_user_assistant_transcript.md
```

User-message index:

```text
/opt/cloudon-bi/forensics/old-server-codex-sessions/old_codex_user_message_index.md
```

Raw imported JSONL:

```text
/opt/cloudon-bi/forensics/old-server-codex-sessions/rollout-2026-05-07T23-01-57-019e0408-47fe-7c11-b4f3-ff3b4fff62b7.jsonl
```

Do not paste the raw JSONL into prompts. Use `OLD_CODEX_CONTEXT.md` first, then inspect the transcript or index selectively.

## Prompt For New Session

Use this prompt when starting a new Codex/assistant session for this project:

```text
You are continuing work on the BoxVisio BI project.

Project path: /opt/cloudon-bi
Read first: docs/OPERATIONS_HANDOVER.md
Then read: docs/OLD_CODEX_CONTEXT.md

Important context:
- The live server is BoxVisioSrv01 at 178.105.108.10.
- The live domain bi.boxvisio.com points to 178.105.108.10.
- Docker Compose project is cloudon_bi.
- Main active tenant is pharmacy295.
- Always check current service status with:
  cd /opt/cloudon-bi && docker compose ps
- Do not assume Git contains every production change; check git status and the handover file.
- Be careful with workers: only one server should run worker/worker_beat/worker_delete for live sync.
- If dashboard gives 500, check tenant DB roles/passwords and API logs.
- If dashboard gives 504, check nginx proxy timeout and KPI query performance.

Current priorities:
1. Improve dashboard KPI loading speed.
2. Continue UI/UX audit for all tenant circuits.
3. Make document-based circuits follow the e-shop analysis style where appropriate.
4. Verify KPI deltas vs exact previous-year comparable periods.
5. Keep SoftOne JavaScript bridge aligned with backend ingestion changes.
6. Build tenant-level scheduled reconciliation/audit checks across all circuits.

Work style:
- Read the codebase before changing.
- Keep edits scoped.
- Run verification after changes.
- Use the existing design system and patterns.
- Do not overwrite user or production changes.
```
