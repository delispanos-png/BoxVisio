#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
from collections import defaultdict
from datetime import date, datetime, time

from sqlalchemy import func, select, text

from app.db.control_session import ControlSessionLocal
from app.db.tenant_manager import get_tenant_db_session
from app.models.control import Tenant, TenantConnection
from app.models.tenant import FactSales
from app.services.connection_secrets import build_odbc_connection_string, decrypt_sqlserver_secret
from app.services.sqlserver_connector import fetch_incremental_rows


def _as_date(value) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    text_value = str(value).strip()
    if not text_value:
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(text_value[:19], fmt).date()
        except Exception:
            continue
    return None


async def _load_tenant_and_connection(tenant_slug: str) -> tuple[Tenant, TenantConnection]:
    async with ControlSessionLocal() as db:
        tenant = (await db.execute(select(Tenant).where(Tenant.slug == tenant_slug))).scalar_one()
        conn = (
            await db.execute(
                select(TenantConnection)
                .where(
                    TenantConnection.tenant_id == tenant.id,
                    TenantConnection.connector_type.in_(("sql_connector", "pharmacyone_sql")),
                    TenantConnection.is_active.is_(True),
                )
                .order_by(TenantConnection.id.desc())
                .limit(1)
            )
        ).scalar_one()
        return tenant, conn


async def _target_sales_snapshot(tenant: Tenant, from_date: date, to_date: date) -> dict:
    async for tenant_db in get_tenant_db_session(
        tenant_key=str(tenant.id),
        db_name=tenant.db_name,
        db_user=tenant.db_user,
        db_password=tenant.db_password,
    ):
        totals = (
            await tenant_db.execute(
                select(
                    func.count(FactSales.id),
                    func.coalesce(func.sum(FactSales.net_value), 0),
                    func.count(func.distinct(FactSales.external_id)),
                    func.count(func.distinct(FactSales.event_id)),
                ).where(FactSales.doc_date.between(from_date, to_date))
            )
        ).first()

        dup_external = (
            await tenant_db.execute(
                text(
                    """
                    SELECT COUNT(*) FROM (
                      SELECT external_id
                      FROM fact_sales
                      WHERE doc_date BETWEEN :from_date AND :to_date
                      GROUP BY external_id
                      HAVING COUNT(*) > 1
                    ) s
                    """
                ),
                {"from_date": from_date, "to_date": to_date},
            )
        ).scalar_one()
        dup_event = (
            await tenant_db.execute(
                text(
                    """
                    SELECT COUNT(*) FROM (
                      SELECT event_id
                      FROM fact_sales
                      WHERE doc_date BETWEEN :from_date AND :to_date
                      GROUP BY event_id
                      HAVING COUNT(*) > 1
                    ) s
                    """
                ),
                {"from_date": from_date, "to_date": to_date},
            )
        ).scalar_one()

        day_rows = (
            await tenant_db.execute(
                select(
                    FactSales.doc_date,
                    func.count(FactSales.id),
                    func.coalesce(func.sum(FactSales.net_value), 0),
                )
                .where(FactSales.doc_date.between(from_date, to_date))
                .group_by(FactSales.doc_date)
                .order_by(FactSales.doc_date)
            )
        ).all()

        key_rows = (
            await tenant_db.execute(
                select(FactSales.external_id).where(FactSales.doc_date.between(from_date, to_date))
            )
        ).all()
        keys = {str(r[0]) for r in key_rows if r[0]}

        by_day = {
            str(r[0]): {"rows": int(r[1] or 0), "net_value": float(r[2] or 0)}
            for r in day_rows
        }
        return {
            "rows": int(totals[0] or 0),
            "net_value": float(totals[1] or 0),
            "distinct_external_id": int(totals[2] or 0),
            "distinct_event_id": int(totals[3] or 0),
            "duplicate_external_ids": int(dup_external or 0),
            "duplicate_event_ids": int(dup_event or 0),
            "by_day": by_day,
            "keys": keys,
        }
    raise RuntimeError("tenant db session unavailable")


def _source_sales_snapshot(conn: TenantConnection, from_date: date, to_date: date) -> dict:
    secret = decrypt_sqlserver_secret(conn.enc_payload)
    connection_string = build_odbc_connection_string(secret)

    source_rows = 0
    source_net = 0.0
    source_keys: set[str] = set()
    by_day_rows: dict[str, int] = defaultdict(int)
    by_day_net: dict[str, float] = defaultdict(float)

    iterator = fetch_incremental_rows(
        connection_string=connection_string,
        query_template=str(conn.sales_query_template or ""),
        incremental_column=str(conn.incremental_column or "updated_at"),
        id_column=str(conn.id_column or "external_id"),
        date_column=str(conn.date_column or "doc_date"),
        last_sync_timestamp=None,
        last_sync_id=None,
        from_date=datetime.combine(from_date, time.min),
        to_date=datetime.combine(to_date, time.min),
        retries=1,
    )
    for row in iterator:
        ext_id = str(row.get("external_id") or "").strip()
        doc_date = _as_date(row.get("doc_date"))
        if not ext_id or doc_date is None:
            continue
        source_rows += 1
        net = float(row.get("net_amount") or 0)
        source_net += net
        source_keys.add(ext_id)
        day_key = doc_date.isoformat()
        by_day_rows[day_key] += 1
        by_day_net[day_key] += net

    by_day = {
        day: {"rows": by_day_rows[day], "net_value": by_day_net[day]}
        for day in sorted(by_day_rows.keys())
    }
    return {
        "rows": source_rows,
        "net_value": source_net,
        "distinct_external_id": len(source_keys),
        "by_day": by_day,
        "keys": source_keys,
    }


async def run(tenant_slug: str, from_date: date, to_date: date, sample_size: int) -> dict:
    tenant, conn = await _load_tenant_and_connection(tenant_slug)
    source = _source_sales_snapshot(conn, from_date, to_date)
    target = await _target_sales_snapshot(tenant, from_date, to_date)

    missing = sorted(source["keys"] - target["keys"])
    extra = sorted(target["keys"] - source["keys"])

    day_keys = sorted(set(source["by_day"].keys()) | set(target["by_day"].keys()))
    day_recon = []
    for day in day_keys:
        s = source["by_day"].get(day, {"rows": 0, "net_value": 0.0})
        t = target["by_day"].get(day, {"rows": 0, "net_value": 0.0})
        row_delta = int(s["rows"]) - int(t["rows"])
        net_delta = float(s["net_value"]) - float(t["net_value"])
        if row_delta != 0 or abs(net_delta) >= 0.01:
            day_recon.append(
                {
                    "date": day,
                    "source_rows": int(s["rows"]),
                    "target_rows": int(t["rows"]),
                    "row_delta": row_delta,
                    "source_net": round(float(s["net_value"]), 2),
                    "target_net": round(float(t["net_value"]), 2),
                    "net_delta": round(net_delta, 2),
                }
            )

    return {
        "tenant": tenant_slug,
        "period": {"from": from_date.isoformat(), "to": to_date.isoformat()},
        "source": {
            "rows": int(source["rows"]),
            "distinct_external_id": int(source["distinct_external_id"]),
            "net_value": round(float(source["net_value"]), 2),
        },
        "target": {
            "rows": int(target["rows"]),
            "distinct_external_id": int(target["distinct_external_id"]),
            "distinct_event_id": int(target["distinct_event_id"]),
            "net_value": round(float(target["net_value"]), 2),
            "duplicate_external_ids": int(target["duplicate_external_ids"]),
            "duplicate_event_ids": int(target["duplicate_event_ids"]),
        },
        "reconciliation": {
            "row_delta": int(source["rows"]) - int(target["rows"]),
            "net_delta": round(float(source["net_value"]) - float(target["net_value"]), 2),
            "missing_in_target_count": len(missing),
            "extra_in_target_count": len(extra),
            "missing_in_target_sample": missing[:sample_size],
            "extra_in_target_sample": extra[:sample_size],
            "day_mismatches": day_recon[: max(1, sample_size)],
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify sales ingest completeness and duplicates for a period.")
    parser.add_argument("--tenant", required=True, help="Tenant slug, e.g. pharmacy295")
    parser.add_argument("--from-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--to-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--sample-size", type=int, default=20, help="How many sample keys to print")
    args = parser.parse_args()

    from_date = date.fromisoformat(args.from_date)
    to_date = date.fromisoformat(args.to_date)
    if from_date > to_date:
        raise SystemExit("--from-date must be <= --to-date")

    result = asyncio.run(run(args.tenant.strip(), from_date, to_date, max(1, int(args.sample_size))))
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
