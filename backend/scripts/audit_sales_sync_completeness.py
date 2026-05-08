#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from sqlalchemy import create_engine, select

SCRIPT_PATH = Path(__file__).resolve()
PROJECT_ROOT = SCRIPT_PATH.parents[2]
BACKEND_ROOT = PROJECT_ROOT / "backend"
sys.path.insert(0, str(BACKEND_ROOT))

from app.core.config import settings  # noqa: E402
from app.models.control import Tenant  # noqa: E402
from app.services.ingestion.completeness_audit import collect_tenant_sync_completeness  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit tenant sales/items sync completeness.")
    parser.add_argument("--tenant", required=True, help="Tenant slug, e.g. pharmacy295")
    args = parser.parse_args()

    control_engine = create_engine(settings.control_database_url_sync, future=True)
    try:
        with control_engine.begin() as db:
            tenant = db.execute(
                select(Tenant.db_name, Tenant.db_user, Tenant.db_password).where(Tenant.slug == args.tenant)
            ).one()
    finally:
        control_engine.dispose()

    tenant_db_url = settings.tenant_database_url_template_sync.format(
        user=tenant[1],
        password=tenant[2],
        db_name=tenant[0],
    )
    tenant_engine = create_engine(tenant_db_url, future=True)
    try:
        audit = collect_tenant_sync_completeness(tenant_engine)
    finally:
        tenant_engine.dispose()

    print(json.dumps({"tenant": args.tenant, "audit": audit}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
