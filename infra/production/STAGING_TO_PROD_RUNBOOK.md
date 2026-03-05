# Staging to Production Promotion Runbook

## 1) Preconditions
- Staging validation report is green (`STAGING_VALIDATION_REPORT.md`).
- Production DNS A records exist for `bi.boxvisio.com` and `adminpanel.boxvisio.com`.
- Production `.env` populated with non-placeholder secrets.
- Latest backups completed and verified restorable.

## 2) Freeze and Backup
```bash
cd /opt/cloudon-bi
./scripts/nightly_backup.sh
```

## 3) Pull/Sync Release
```bash
cd /opt/cloudon-bi
# If using git:
# git fetch --all
# git checkout <release-tag>
# git pull --ff-only
```

## 4) Deploy With Migrations (control + tenant)
```bash
cd /opt/cloudon-bi
bash infra/production/scripts/deploy_rolling.sh
```

## 5) Post-deploy Verification
```bash
docker compose -f docker-compose.yml -f infra/production/compose.prod.yml ps
curl -I http://bi.boxvisio.com/health
curl -I https://bi.boxvisio.com/health
curl -I https://adminpanel.boxvisio.com/health
```

## 6) Rollback Procedure
```bash
cd /opt/cloudon-bi
# rollback to previous release/tag
# git checkout <previous-release-tag>
bash infra/production/scripts/deploy_rolling.sh
```

If schema rollback is needed, restore from backup dumps:
```bash
./scripts/restore_db.sh --db bi_control --file <path_to_control_dump> --drop-create
./scripts/restore_db.sh --db bi_tenant_<slug> --file <path_to_tenant_dump> --drop-create
```
