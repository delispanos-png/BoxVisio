#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

from sqlalchemy import create_engine, select, text

SCRIPT_PATH = Path(__file__).resolve()
PROJECT_ROOT = SCRIPT_PATH.parents[2]
BACKEND_ROOT = PROJECT_ROOT / "backend"
sys.path.insert(0, str(BACKEND_ROOT))
sys.path.insert(0, str(PROJECT_ROOT))

from app.core.config import settings  # noqa: E402
from app.models.control import Tenant, TenantConnection  # noqa: E402
from app.services.ingestion.completeness_audit import collect_tenant_sync_completeness  # noqa: E402
from app.services.ingestion.progress import get_ingest_progress  # noqa: E402
from app.services.querypacks import apply_querypack_to_connection, load_querypack  # noqa: E402
from backend.scripts.backfill_softone_item_sotype import (  # noqa: E402
    _load_tenant_and_connections,
    _source_item_sotypes,
    _source_item_sotypes_via_api,
    _tenant_sync_url,
    _upsert_dim_items,
)
from worker.tasks import enqueue_sql_backfill  # noqa: E402
from app.services.connection_secrets import build_odbc_connection_string, decrypt_sqlserver_secret  # noqa: E402
from sqlalchemy.orm import Session  # noqa: E402


def _tenant_date_range(tenant_db_url: str) -> tuple[str, str]:
    tenant_engine = create_engine(tenant_db_url, future=True)
    try:
        with tenant_engine.begin() as db:
            row = db.execute(
                text(
                    """
                    SELECT
                      COALESCE(MIN(doc_date)::text, CURRENT_DATE::text) AS from_date,
                      COALESCE(MAX(doc_date)::text, CURRENT_DATE::text) AS to_date
                    FROM fact_sales
                    """
                )
            ).one()
            return str(row[0]), str(row[1])
    finally:
        tenant_engine.dispose()


def _apply_latest_querypack(tenant_slug: str) -> tuple[Tenant, TenantConnection]:
    control_engine = create_engine(settings.control_database_url_sync, future=True)
    try:
        with Session(control_engine) as control_session:
            tenant_id = control_session.execute(select(Tenant.id).where(Tenant.slug == tenant_slug)).scalar_one()
            tenant = control_session.execute(select(Tenant).where(Tenant.id == tenant_id)).scalar_one()
            conn = control_session.execute(
                select(TenantConnection).where(
                    TenantConnection.tenant_id == tenant_id,
                    TenantConnection.connector_type == "sql_connector",
                )
            ).scalar_one()
            pack = load_querypack("sql_connector")
            apply_querypack_to_connection(conn, pack)
            control_session.commit()
            control_session.refresh(conn)
            control_session.refresh(tenant)
            return tenant, conn
    finally:
        control_engine.dispose()


def _backfill_item_master(tenant_slug: str) -> dict[str, int | str | None]:
    control_engine = create_engine(settings.control_database_url_sync, future=True)
    try:
        with Session(control_engine) as control_session:
            tenant, connection, api_connection = _load_tenant_and_connections(control_session, tenant_slug)
            connection_string = None
            if connection is not None:
                secret = decrypt_sqlserver_secret(connection.enc_payload)
                connection_string = build_odbc_connection_string(secret)
            company = None
            raw_company = ((connection.connection_parameters if connection else None) or {}).get("company")
            try:
                company = int(raw_company) if raw_company is not None else None
            except Exception:
                company = None
            source_mode = "sql"
            try:
                if not connection_string:
                    raise RuntimeError("No SQL connection string available")
                source_rows = _source_item_sotypes(connection_string, company=company)
            except Exception:
                if api_connection is None:
                    raise
                source_rows = _source_item_sotypes_via_api(api_connection, tenant_slug=tenant.slug, company=company)
                source_mode = "external_api"
    finally:
        control_engine.dispose()

    tenant_engine = create_engine(
        _tenant_sync_url(db_name=tenant.db_name, db_user=tenant.db_user, db_password=tenant.db_password),
        future=True,
    )
    try:
        with Session(tenant_engine) as tenant_session:
            changed, fact_backfill = _upsert_dim_items(tenant_session, source_rows)
            tenant_session.commit()
            return {
                "source_mode": source_mode,
                "source_rows": len(source_rows),
                "dim_items_upserted": int(changed),
                "fact_inventory_item_id_backfilled": int(fact_backfill),
            }
    finally:
        tenant_engine.dispose()


def _wait_for_backfill(tenant_slug: str, timeout_seconds: int) -> dict:
    started = time.time()
    while True:
        progress = get_ingest_progress(tenant_slug)
        status = str(progress.get("status") or "")
        if status in {"completed", "idle"} and int(progress.get("current_queue_depth") or 0) == 0:
            return progress
        if status in {"failed", "stopped"}:
            raise RuntimeError(json.dumps(progress, ensure_ascii=False))
        if time.time() - started > timeout_seconds:
            raise TimeoutError(json.dumps(progress, ensure_ascii=False))
        time.sleep(5)


def main() -> int:
    parser = argparse.ArgumentParser(description="Remediate sales metadata/group completeness and wait for backfill.")
    parser.add_argument("--tenant", required=True, help="Tenant slug, e.g. pharmacy295")
    parser.add_argument("--chunk-days", type=int, default=1, help="Backfill chunk size in days")
    parser.add_argument("--timeout-seconds", type=int, default=7200, help="Wait timeout")
    args = parser.parse_args()

    tenant, _ = _apply_latest_querypack(args.tenant)
    item_backfill = _backfill_item_master(args.tenant)

    tenant_db_url = _tenant_sync_url(db_name=tenant.db_name, db_user=tenant.db_user, db_password=tenant.db_password)
    from_date, to_date = _tenant_date_range(tenant_db_url)

    enqueue_result = enqueue_sql_backfill(
        tenant_slug=args.tenant,
        from_date_str=from_date,
        to_date_str=to_date,
        chunk_days=max(1, int(args.chunk_days)),
        include_purchases=False,
        include_inventory=False,
        include_cashflows=False,
        include_supplier_balances=False,
        include_customer_balances=False,
        include_operating_expenses=False,
        operation="backfill",
    )
    progress = _wait_for_backfill(args.tenant, args.timeout_seconds)

    tenant_engine = create_engine(tenant_db_url, future=True)
    try:
        audit = collect_tenant_sync_completeness(tenant_engine)
    finally:
        tenant_engine.dispose()

    print(
        json.dumps(
            {
                "tenant": args.tenant,
                "date_from": from_date,
                "date_to": to_date,
                "item_backfill": item_backfill,
                "enqueue_result": enqueue_result,
                "progress": progress,
                "audit": audit,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
