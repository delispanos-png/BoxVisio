#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time as dtime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import func, select, text


SCRIPT_PATH = Path(__file__).resolve()
PROJECT_ROOT = SCRIPT_PATH.parents[1]
BACKEND_ROOT = PROJECT_ROOT / "backend"
sys.path.insert(0, str(BACKEND_ROOT))
sys.path.insert(0, str(PROJECT_ROOT))

from app.db.control_session import ControlSessionLocal  # noqa: E402
from app.db.tenant_manager import get_tenant_db_session  # noqa: E402
from app.models.control import Tenant, TenantConnection  # noqa: E402
from app.models.tenant import (  # noqa: E402
    FactCashflow,
    FactCustomerBalance,
    FactExpense,
    FactInventory,
    FactPurchases,
    FactSales,
    FactSupplierBalance,
    FactSupplierOrder,
)
from app.services.connection_secrets import (  # noqa: E402
    build_odbc_connection_string,
    decrypt_sqlserver_secret,
)
from app.services.ingestion.progress import get_ingest_progress  # noqa: E402
from app.services.sqlserver_connector import fetch_incremental_rows  # noqa: E402
from worker.tasks import enqueue_sql_backfill  # noqa: E402


@dataclass(frozen=True)
class StreamCfg:
    stream: str
    query_attr: str
    model: Any
    table_name: str
    date_field: str
    amount_field: str
    source_amount_keys: tuple[str, ...]


STREAMS: dict[str, StreamCfg] = {
    "sales_documents": StreamCfg(
        stream="sales_documents",
        query_attr="sales_query_template",
        model=FactSales,
        table_name="fact_sales",
        date_field="doc_date",
        amount_field="net_value",
        source_amount_keys=("net_value", "net_amount"),
    ),
    "purchase_documents": StreamCfg(
        stream="purchase_documents",
        query_attr="purchases_query_template",
        model=FactPurchases,
        table_name="fact_purchases",
        date_field="doc_date",
        amount_field="net_value",
        source_amount_keys=("net_amount", "net_value"),
    ),
    "inventory_documents": StreamCfg(
        stream="inventory_documents",
        query_attr="inventory_query_template",
        model=FactInventory,
        table_name="fact_inventory",
        date_field="doc_date",
        amount_field="value_amount",
        source_amount_keys=("value_amount", "cost_amount"),
    ),
    "cash_transactions": StreamCfg(
        stream="cash_transactions",
        query_attr="cashflow_query_template",
        model=FactCashflow,
        table_name="fact_cashflows",
        date_field="doc_date",
        amount_field="amount",
        source_amount_keys=("amount",),
    ),
    "operating_expenses": StreamCfg(
        stream="operating_expenses",
        query_attr="expenses_query_template",
        model=FactExpense,
        table_name="fact_expenses",
        date_field="expense_date",
        amount_field="amount_gross",
        source_amount_keys=("amount_gross", "amount_net"),
    ),
    "supplier_balances": StreamCfg(
        stream="supplier_balances",
        query_attr="supplier_balances_query_template",
        model=FactSupplierBalance,
        table_name="fact_supplier_balances",
        date_field="balance_date",
        amount_field="open_balance",
        source_amount_keys=("open_balance",),
    ),
    "customer_balances": StreamCfg(
        stream="customer_balances",
        query_attr="customer_balances_query_template",
        model=FactCustomerBalance,
        table_name="fact_customer_balances",
        date_field="balance_date",
        amount_field="open_balance",
        source_amount_keys=("open_balance",),
    ),
    "supplier_orders": StreamCfg(
        stream="supplier_orders",
        query_attr="stream_query_mapping",
        model=FactSupplierOrder,
        table_name="fact_supplier_orders",
        date_field="doc_date",
        amount_field="line_value",
        source_amount_keys=("line_value",),
    ),
}


def _as_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    raw = str(value).strip()
    if not raw:
        return None
    for candidate in (raw, raw[:10], raw[:19]):
        for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(candidate, fmt).date()
            except ValueError:
                continue
    return None


def _as_float(value: Any) -> float:
    try:
        return float(value or 0)
    except Exception:
        return 0.0


def _group_contiguous_days(days: list[date]) -> list[tuple[date, date]]:
    if not days:
        return []
    ordered = sorted(set(days))
    out: list[tuple[date, date]] = []
    start = ordered[0]
    end = ordered[0]
    for current in ordered[1:]:
        if current == end + timedelta(days=1):
            end = current
            continue
        out.append((start, end))
        start = current
        end = current
    out.append((start, end))
    return out


def _extract_mismatch_days(recon: dict[str, Any], fallback_from: date, fallback_to: date) -> list[date]:
    out: list[date] = []
    for row in recon.get("day_mismatches") or []:
        raw = str(row.get("date") or "").strip()
        if not raw:
            continue
        try:
            out.append(date.fromisoformat(raw))
        except ValueError:
            continue
    if out:
        return sorted(set(out))
    return [fallback_from + timedelta(days=i) for i in range((fallback_to - fallback_from).days + 1)]


def _stream_flags(stream: str) -> dict[str, bool]:
    return {
        "include_sales": stream == "sales_documents",
        "include_purchases": stream == "purchase_documents",
        "include_inventory": stream == "inventory_documents",
        "include_cashflows": stream == "cash_transactions",
        "include_supplier_balances": stream == "supplier_balances",
        "include_customer_balances": stream == "customer_balances",
        "include_operating_expenses": stream == "operating_expenses",
        "include_supplier_orders": stream == "supplier_orders",
    }


def _wait_for_operation(tenant_slug: str, timeout_seconds: int) -> dict[str, Any]:
    started = time.time()
    latest = get_ingest_progress(tenant_slug)
    while time.time() - started <= timeout_seconds:
        latest = get_ingest_progress(tenant_slug)
        status = str(latest.get("status") or "")
        queue_depth = int(latest.get("current_queue_depth") or 0)
        target_depth = int(latest.get("target_queue_depth") or 0)
        lock_active = bool(latest.get("lock_active"))
        if status in {"completed", "idle"} and (
            queue_depth <= max(0, target_depth)
            or not lock_active
        ):
            return latest
        if status in {"failed", "stopped"}:
            raise RuntimeError(json.dumps(latest, ensure_ascii=False))
        time.sleep(5)
    raise TimeoutError(json.dumps(latest, ensure_ascii=False))


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


def _resolve_query_template(conn: TenantConnection, cfg: StreamCfg) -> str:
    mapping = conn.stream_query_mapping if isinstance(conn.stream_query_mapping, dict) else {}
    mapped = str(mapping.get(cfg.stream) or "").strip()
    if mapped:
        return mapped
    direct = str(getattr(conn, cfg.query_attr, "") or "").strip()
    if direct:
        return direct
    raise RuntimeError(f"Missing query template for stream: {cfg.stream}")


def _source_snapshot(
    conn: TenantConnection,
    cfg: StreamCfg,
    from_date: date,
    to_date: date,
    limit: int,
) -> dict[str, Any]:
    secret = decrypt_sqlserver_secret(conn.enc_payload)
    connection_string = build_odbc_connection_string(secret)
    query_template = _resolve_query_template(conn, cfg)

    source_rows = 0
    source_amount = 0.0
    source_keys: set[str] = set()
    by_day_rows: dict[str, int] = defaultdict(int)
    by_day_amount: dict[str, float] = defaultdict(float)

    iterator = fetch_incremental_rows(
        connection_string=connection_string,
        query_template=query_template,
        incremental_column=str(conn.incremental_column or "updated_at"),
        id_column=str(conn.id_column or "external_id"),
        date_column=str(conn.date_column or "doc_date"),
        last_sync_timestamp=None,
        last_sync_id=None,
        from_date=datetime.combine(from_date, dtime.min),
        to_date=datetime.combine(to_date, dtime.min),
        limit=max(100, int(limit)),
        exhaustive=True,
        retries=3,
        retry_sleep_sec=3,
    )
    for row in iterator:
        ext_id = str(row.get("external_id") or "").strip()
        doc_dt = _as_date(row.get("doc_date") or row.get(cfg.date_field))
        if not ext_id or doc_dt is None:
            continue
        amount = 0.0
        for key in cfg.source_amount_keys:
            if key in row:
                amount = _as_float(row.get(key))
                break
        source_rows += 1
        source_amount += amount
        source_keys.add(ext_id)
        day_key = doc_dt.isoformat()
        by_day_rows[day_key] += 1
        by_day_amount[day_key] += amount

    by_day = {
        day: {"rows": by_day_rows[day], "amount": by_day_amount[day]}
        for day in sorted(by_day_rows.keys())
    }
    return {
        "rows": source_rows,
        "amount": source_amount,
        "distinct_external_id": len(source_keys),
        "by_day": by_day,
        "keys": source_keys,
    }


async def _target_snapshot(tenant: Tenant, cfg: StreamCfg, from_date: date, to_date: date) -> dict[str, Any]:
    model = cfg.model
    date_col = getattr(model, cfg.date_field)
    amount_col = getattr(model, cfg.amount_field)

    async for tenant_db in get_tenant_db_session(
        tenant_key=str(tenant.id),
        db_name=tenant.db_name,
        db_user=tenant.db_user,
        db_password=tenant.db_password,
    ):
        totals = (
            await tenant_db.execute(
                select(
                    func.count(model.id),
                    func.coalesce(func.sum(amount_col), 0),
                    func.count(func.distinct(model.external_id)),
                ).where(date_col.between(from_date, to_date))
            )
        ).first()

        dup_external = (
            await tenant_db.execute(
                text(
                    f"""
                    SELECT COUNT(*) FROM (
                      SELECT external_id
                      FROM {cfg.table_name}
                      WHERE {cfg.date_field} BETWEEN :from_date AND :to_date
                      GROUP BY external_id
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
                    date_col,
                    func.count(model.id),
                    func.coalesce(func.sum(amount_col), 0),
                )
                .where(date_col.between(from_date, to_date))
                .group_by(date_col)
                .order_by(date_col)
            )
        ).all()

        key_rows = (
            await tenant_db.execute(select(model.external_id).where(date_col.between(from_date, to_date)))
        ).all()
        keys = {str(r[0]) for r in key_rows if r[0]}

        by_day = {
            str(r[0]): {"rows": int(r[1] or 0), "amount": float(r[2] or 0)}
            for r in day_rows
        }
        return {
            "rows": int(totals[0] or 0),
            "amount": float(totals[1] or 0),
            "distinct_external_id": int(totals[2] or 0),
            "duplicate_external_ids": int(dup_external or 0),
            "by_day": by_day,
            "keys": keys,
        }
    raise RuntimeError("tenant db session unavailable")


async def verify_stream(
    tenant: Tenant,
    conn: TenantConnection,
    cfg: StreamCfg,
    from_date: date,
    to_date: date,
    sample_size: int,
    limit: int,
) -> dict[str, Any]:
    source = _source_snapshot(conn, cfg, from_date, to_date, limit)
    target = await _target_snapshot(tenant, cfg, from_date, to_date)

    missing = sorted(source["keys"] - target["keys"])
    extra = sorted(target["keys"] - source["keys"])

    day_keys = sorted(set(source["by_day"].keys()) | set(target["by_day"].keys()))
    day_recon = []
    for day in day_keys:
        s = source["by_day"].get(day, {"rows": 0, "amount": 0.0})
        t = target["by_day"].get(day, {"rows": 0, "amount": 0.0})
        row_delta = int(s["rows"]) - int(t["rows"])
        amount_delta = float(s["amount"]) - float(t["amount"])
        if row_delta != 0 or abs(amount_delta) >= 0.01:
            day_recon.append(
                {
                    "date": day,
                    "source_rows": int(s["rows"]),
                    "target_rows": int(t["rows"]),
                    "row_delta": row_delta,
                    "source_amount": round(float(s["amount"]), 2),
                    "target_amount": round(float(t["amount"]), 2),
                    "amount_delta": round(amount_delta, 2),
                }
            )

    return {
        "stream": cfg.stream,
        "source": {
            "rows": int(source["rows"]),
            "distinct_external_id": int(source["distinct_external_id"]),
            "amount": round(float(source["amount"]), 2),
        },
        "target": {
            "rows": int(target["rows"]),
            "distinct_external_id": int(target["distinct_external_id"]),
            "duplicate_external_ids": int(target["duplicate_external_ids"]),
            "amount": round(float(target["amount"]), 2),
        },
        "reconciliation": {
            "row_delta": int(source["rows"]) - int(target["rows"]),
            "amount_delta": round(float(source["amount"]) - float(target["amount"]), 2),
            "missing_in_target_count": len(missing),
            "extra_in_target_count": len(extra),
            "missing_in_target_sample": missing[:sample_size],
            "extra_in_target_sample": extra[:sample_size],
            "day_mismatches": day_recon[: max(1, sample_size)],
        },
    }


async def verify_many(
    tenant_slug: str,
    streams: list[str],
    from_date: date,
    to_date: date,
    sample_size: int,
    limit: int,
) -> dict[str, Any]:
    tenant, conn = await _load_tenant_and_connection(tenant_slug)
    results: dict[str, Any] = {}
    for stream in streams:
        cfg = STREAMS[stream]
        results[stream] = await verify_stream(tenant, conn, cfg, from_date, to_date, sample_size, limit)
    return {
        "tenant": tenant_slug,
        "period": {"from": from_date.isoformat(), "to": to_date.isoformat()},
        "streams": results,
    }


def _stream_is_clean(stream_result: dict[str, Any]) -> bool:
    recon = dict(stream_result.get("reconciliation") or {})
    row_delta = int(recon.get("row_delta") or 0)
    amount_delta = float(recon.get("amount_delta") or 0.0)
    return row_delta == 0 and abs(amount_delta) < 0.01


async def _run_workflow(
    *,
    tenant_slug: str,
    streams: list[str],
    from_date: date,
    to_date: date,
    sample_size: int,
    limit: int,
    recover: bool,
    passes: int,
    chunk_days: int,
    wait_timeout: int,
) -> int:
    history: list[dict[str, Any]] = []

    for pass_no in range(1, passes + 1):
        snapshot = await verify_many(tenant_slug, streams, from_date, to_date, sample_size, limit)
        stream_results = snapshot.get("streams") or {}
        pass_brief = {}
        all_clean = True
        for stream in streams:
            result = dict(stream_results.get(stream) or {})
            recon = dict(result.get("reconciliation") or {})
            row_delta = int(recon.get("row_delta") or 0)
            amount_delta = round(float(recon.get("amount_delta") or 0.0), 2)
            pass_brief[stream] = {"row_delta": row_delta, "amount_delta": amount_delta}
            if not _stream_is_clean(result):
                all_clean = False
        history.append({"pass": pass_no, "streams": pass_brief})
        print(json.dumps({"event": "verify", "pass": pass_no, "streams": pass_brief}, ensure_ascii=False))

        if all_clean:
            print(
                json.dumps(
                    {
                        "status": "ok",
                        "tenant": tenant_slug,
                        "period": {"from": from_date.isoformat(), "to": to_date.isoformat()},
                        "passes": pass_no,
                        "streams": stream_results,
                        "history": history,
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return 0

        if not recover:
            print(
                json.dumps(
                    {
                        "status": "mismatch",
                        "tenant": tenant_slug,
                        "period": {"from": from_date.isoformat(), "to": to_date.isoformat()},
                        "passes": pass_no,
                        "streams": stream_results,
                        "history": history,
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return 2

        for stream in streams:
            result = dict(stream_results.get(stream) or {})
            if _stream_is_clean(result):
                continue
            recon = dict(result.get("reconciliation") or {})
            mismatch_days = _extract_mismatch_days(recon, from_date, to_date)
            windows = _group_contiguous_days(mismatch_days)
            flags = _stream_flags(stream)
            for window_from, window_to in windows:
                enqueue_sql_backfill(
                    tenant_slug=tenant_slug,
                    from_date_str=window_from.isoformat(),
                    to_date_str=window_to.isoformat(),
                    chunk_days=chunk_days,
                    limit=limit,
                    operation=f"auto_recovery_{stream}",
                    **flags,
                )

        _wait_for_operation(tenant_slug, int(wait_timeout))

    final_snapshot = await verify_many(tenant_slug, streams, from_date, to_date, sample_size, limit)
    print(
        json.dumps(
            {
                "status": "incomplete",
                "tenant": tenant_slug,
                "period": {"from": from_date.isoformat(), "to": to_date.isoformat()},
                "passes": passes,
                "streams": final_snapshot.get("streams"),
                "history": history,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 2


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Verify source-vs-target completeness for operational streams in a period. "
            "Optional recover mode triggers targeted stream backfills for mismatch days."
        )
    )
    parser.add_argument("--tenant", required=True, help="Tenant slug, e.g. pharmacy295")
    parser.add_argument("--from-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--to-date", required=True, help="YYYY-MM-DD")
    parser.add_argument(
        "--streams",
        default="sales_documents,purchase_documents,inventory_documents,cash_transactions,operating_expenses",
        help="Comma list of streams",
    )
    parser.add_argument("--sample-size", type=int, default=200, help="How many sample keys/day mismatches to keep")
    parser.add_argument("--limit", type=int, default=10000, help="SQL page limit per query")
    parser.add_argument("--recover", action="store_true", help="Auto enqueue recover backfill for mismatch windows")
    parser.add_argument("--max-passes", type=int, default=3, help="Max verify/recover loops")
    parser.add_argument("--chunk-days", type=int, default=1, help="Backfill chunk days for mismatch windows")
    parser.add_argument("--wait-timeout", type=int, default=7200, help="Seconds to wait per recovery pass")
    args = parser.parse_args()

    from_date = date.fromisoformat(args.from_date)
    to_date = date.fromisoformat(args.to_date)
    if from_date > to_date:
        raise SystemExit("--from-date must be <= --to-date")

    requested_streams = [s.strip() for s in str(args.streams).split(",") if s.strip()]
    streams = [s for s in requested_streams if s in STREAMS]
    if not streams:
        raise SystemExit("No valid streams selected")

    return asyncio.run(
        _run_workflow(
            tenant_slug=args.tenant,
            streams=streams,
            from_date=from_date,
            to_date=to_date,
            sample_size=max(20, int(args.sample_size)),
            limit=max(100, int(args.limit)),
            recover=bool(args.recover),
            passes=max(1, int(args.max_passes)),
            chunk_days=max(1, int(args.chunk_days)),
            wait_timeout=max(30, int(args.wait_timeout)),
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())
