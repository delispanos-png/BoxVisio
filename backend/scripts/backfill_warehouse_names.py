#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from sqlalchemy import create_engine, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

SCRIPT_PATH = Path(__file__).resolve()
PROJECT_ROOT: Path | None = None
BACKEND_ROOT: Path | None = None
for candidate in [SCRIPT_PATH.parent, *SCRIPT_PATH.parents]:
    if (candidate / "backend" / "app").exists():
        PROJECT_ROOT = candidate
        BACKEND_ROOT = candidate / "backend"
        break
    if (candidate / "app").exists() and (candidate / "requirements.txt").exists():
        BACKEND_ROOT = candidate
        PROJECT_ROOT = candidate.parent
        break
if BACKEND_ROOT is None or PROJECT_ROOT is None:
    raise RuntimeError("Could not locate backend root.")
sys.path.insert(0, str(BACKEND_ROOT))

from app.core.config import settings  # noqa: E402
from app.models.control import Tenant, TenantConnection  # noqa: E402
from app.models.tenant import DimWarehouse  # noqa: E402
from app.services.connection_secrets import build_odbc_connection_string, decrypt_sqlserver_secret  # noqa: E402
from app.services.sqlserver_connector import _connect  # noqa: E402


def _tenant_sync_url(*, db_name: str, db_user: str, db_password: str) -> str:
    return settings.tenant_database_url_template_sync.format(user=db_user, password=db_password, db_name=db_name)


def _load_tenant_and_connection(control_session: Session, tenant_slug: str) -> tuple[Tenant, TenantConnection]:
    tenant = control_session.execute(select(Tenant).where(Tenant.slug == tenant_slug)).scalar_one()
    sql_conn = (
        control_session.execute(
            select(TenantConnection).where(
                TenantConnection.tenant_id == tenant.id,
                TenantConnection.connector_type.in_(("sql_connector", "pharmacyone_sql")),
                TenantConnection.is_active.is_(True),
            )
        )
        .scalars()
        .first()
    )
    if sql_conn is None:
        raise RuntimeError(f"No active SQL connector found for tenant {tenant_slug}")
    return tenant, sql_conn


def _source_warehouses(connection_string: str, *, company: int | None = None) -> list[dict]:
    sql = """
    SELECT
      CAST(ISNULL(W.WHOUSE, 0) AS nvarchar(64)) AS warehouse_external_id,
      CAST(ISNULL(W.NAME, CAST(ISNULL(W.WHOUSE, 0) AS nvarchar(255))) AS nvarchar(255)) AS warehouse_name
    FROM WHOUSE W
    WHERE (? IS NULL OR W.COMPANY = ?)
    """
    with _connect(connection_string) as conn:
        cur = conn.cursor()
        cur.execute(sql, company, company)
        rows = cur.fetchall()
        out: list[dict] = []
        for row in rows:
            ext = str(row[0] or "").strip()
            if not ext:
                continue
            out.append(
                {
                    "external_id": ext[:64],
                    "name": (str(row[1] or ext).strip() or ext)[:255],
                }
            )
        return out


def _upsert_dim_warehouses(tenant_session: Session, rows: list[dict]) -> int:
    if not rows:
        return 0
    deduped: dict[str, dict] = {}
    for row in rows:
        ext = str(row.get("external_id") or "").strip()
        if not ext:
            continue
        name = str(row.get("name") or ext).strip() or ext
        current = deduped.get(ext)
        if current is None:
            deduped[ext] = {"external_id": ext[:64], "name": name[:255]}
            continue
        current_name = str(current.get("name") or ext).strip() or ext
        candidate_is_real = name.lower() != ext.lower()
        current_is_real = current_name.lower() != ext.lower()
        if candidate_is_real and (not current_is_real or len(name) > len(current_name)):
            deduped[ext] = {"external_id": ext[:64], "name": name[:255]}
    rows = list(deduped.values())
    if not rows:
        return 0
    table = DimWarehouse.__table__
    stmt = insert(table).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=["external_id"],
        set_={
            "name": stmt.excluded.name,
        },
    )
    tenant_session.execute(stmt)
    return len(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill warehouse names into tenant dim_warehouses without full resync.")
    parser.add_argument("--tenant", required=True, help="Tenant slug, e.g. pharmacy295")
    parser.add_argument("--company", type=int, default=None, help="Optional SoftOne company filter")
    args = parser.parse_args()

    control_engine = create_engine(settings.control_database_url_sync, future=True)
    with Session(control_engine) as control_session:
        tenant, connection = _load_tenant_and_connection(control_session, args.tenant)
        secret = decrypt_sqlserver_secret(connection.enc_payload)
        connection_string = build_odbc_connection_string(secret)
        company = args.company
        if company is None:
            raw_company = ((connection.connection_parameters if connection else None) or {}).get("company")
            try:
                company = int(raw_company) if raw_company is not None else None
            except Exception:
                company = None
        rows = _source_warehouses(connection_string, company=company)

    tenant_engine = create_engine(
        _tenant_sync_url(db_name=tenant.db_name, db_user=tenant.db_user, db_password=tenant.db_password),
        future=True,
    )
    with Session(tenant_engine) as tenant_session:
        changed = _upsert_dim_warehouses(tenant_session, rows)
        tenant_session.commit()

    print(
        {
            "tenant": tenant.slug,
            "company": company,
            "warehouses_upserted": changed,
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
