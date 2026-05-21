#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import select


SCRIPT_PATH = Path(__file__).resolve()
PROJECT_ROOT = SCRIPT_PATH.parents[1]
BACKEND_ROOT = PROJECT_ROOT / "backend"
sys.path.insert(0, str(BACKEND_ROOT))
sys.path.insert(0, str(PROJECT_ROOT))

from app.db.control_session import ControlSessionLocal  # noqa: E402
from app.db.tenant_manager import get_tenant_db_session  # noqa: E402
from app.models.control import Tenant, TenantConnection  # noqa: E402
from app.services.ingestion.base import normalize_stream_name  # noqa: E402
from app.services.ingestion.reconciliation import (  # noqa: E402
    STREAM_CONFIGS,
    reconcile_tenant_streams,
)


DEFAULT_STREAMS = [
    "sales_documents",
    "purchase_documents",
    "inventory_documents",
    "cash_transactions",
    "operating_expenses",
    "supplier_balances",
    "customer_balances",
    "supplier_orders",
]

STREAM_LABELS = {
    "sales_documents": "Πωλήσεις",
    "purchase_documents": "Αγορές",
    "inventory_documents": "Αποθήκη",
    "cash_transactions": "Ταμειακά",
    "operating_expenses": "Έξοδα",
    "supplier_balances": "Υπόλοιπα προμηθευτών",
    "customer_balances": "Υπόλοιπα πελατών",
    "supplier_orders": "Παραγγελίες προμηθευτών",
}


async def _load_context(tenant_slug: str) -> tuple[Tenant, TenantConnection]:
    async with ControlSessionLocal() as db:
        tenant = (await db.execute(select(Tenant).where(Tenant.slug == tenant_slug))).scalar_one()
        conn = (
            await db.execute(
                select(TenantConnection)
                .where(
                    TenantConnection.tenant_id == tenant.id,
                    TenantConnection.is_active.is_(True),
                    TenantConnection.connector_type.in_(("sql_connector", "pharmacyone_sql")),
                )
                .order_by(TenantConnection.id.desc())
                .limit(1)
            )
        ).scalar_one()
        return tenant, conn


def _clean_streams(raw: str) -> list[str]:
    streams: list[str] = []
    for value in str(raw or "").split(","):
        stream = normalize_stream_name(value.strip())
        if stream in STREAM_CONFIGS and stream not in streams:
            streams.append(stream)
    return streams or list(DEFAULT_STREAMS)


def _format_amount(value: Any) -> str:
    try:
        return f"{float(value or 0):,.2f}".replace(",", "_").replace(".", ",").replace("_", ".")
    except Exception:
        return str(value or "0")


def _status_for_stream(result: dict[str, Any]) -> str:
    mismatches = int(result.get("mismatch_count") or 0)
    if mismatches == 0:
        return "OK"
    return "ΔΙΑΦΟΡΑ"


def _render_markdown(result: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append(f"# SoftOne Reconciliation - {result['tenant']}")
    lines.append("")
    lines.append(f"- Περίοδος: `{result['from']}` έως `{result['to']}`")
    lines.append(f"- Δημιουργήθηκε: `{datetime.utcnow().isoformat(timespec='seconds')}Z`")
    lines.append(f"- Συνολικές αποκλίσεις buckets: `{result['mismatch_count']}`")
    lines.append("")
    lines.append("## Σύνοψη")
    lines.append("")
    lines.append("| Κύκλωμα | Status | Buckets | Αποκλίσεις |")
    lines.append("| --- | --- | ---: | ---: |")
    for stream, stream_result in result.get("streams", {}).items():
        lines.append(
            "| "
            + " | ".join(
                [
                    STREAM_LABELS.get(stream, stream),
                    _status_for_stream(stream_result),
                    str(stream_result.get("bucket_count") or 0),
                    str(stream_result.get("mismatch_count") or 0),
                ]
            )
            + " |"
        )
    lines.append("")
    for stream, stream_result in result.get("streams", {}).items():
        lines.append(f"## {STREAM_LABELS.get(stream, stream)}")
        lines.append("")
        mismatches = list(stream_result.get("mismatches") or [])
        if not mismatches:
            lines.append("OK, δεν βρέθηκαν αποκλίσεις.")
            lines.append("")
            continue
        lines.append("| Bucket | SoftOne γραμμές | BI γραμμές | Δ γραμμών | SoftOne ποσό | BI ποσό | Δ ποσού | SoftOne ποσό 2 | BI ποσό 2 | Δ ποσού 2 |")
        lines.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
        for row in mismatches:
            lines.append(
                "| "
                + " | ".join(
                    [
                        str(row.get("bucket") or ""),
                        str(row.get("source_rows") or 0),
                        str(row.get("bi_rows") or 0),
                        str(row.get("rows_delta") or 0),
                        _format_amount(row.get("source_amount")),
                        _format_amount(row.get("bi_amount")),
                        _format_amount(row.get("amount_delta")),
                        "-" if row.get("source_amount2") is None else _format_amount(row.get("source_amount2")),
                        "-" if row.get("bi_amount2") is None else _format_amount(row.get("bi_amount2")),
                        "-" if row.get("amount2_delta") is None else _format_amount(row.get("amount2_delta")),
                    ]
                )
                + " |"
            )
        lines.append("")
    return "\n".join(lines).strip() + "\n"


async def _run(args: argparse.Namespace) -> int:
    tenant, conn = await _load_context(args.tenant)
    from_date = date.fromisoformat(args.from_date)
    to_date = date.fromisoformat(args.to_date)
    streams = _clean_streams(args.streams)
    async for tenant_db in get_tenant_db_session(
        tenant_key=str(tenant.id),
        db_name=tenant.db_name,
        db_user=tenant.db_user,
        db_password=tenant.db_password,
    ):
        result = await reconcile_tenant_streams(
            tenant,
            tenant_db,
            conn,
            from_date=from_date,
            to_date=to_date,
            streams=streams,
        )
        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        base = output_dir / f"softone_reconciliation_{args.tenant}_{from_date}_{to_date}_{stamp}"
        json_path = base.with_suffix(".json")
        md_path = base.with_suffix(".md")
        json_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        md_path.write_text(_render_markdown(result), encoding="utf-8")
        print(json.dumps({"status": "ok", "json": str(json_path), "markdown": str(md_path), "mismatch_count": result.get("mismatch_count")}, ensure_ascii=False))
        return 0 if int(result.get("mismatch_count") or 0) == 0 else 2
    raise RuntimeError("tenant db session unavailable")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run SoftOne-vs-BI reconciliation report for operational streams.")
    parser.add_argument("--tenant", required=True)
    parser.add_argument("--from-date", required=True)
    parser.add_argument("--to-date", required=True)
    parser.add_argument("--streams", default=",".join(DEFAULT_STREAMS))
    parser.add_argument("--output-dir", default="artifacts/reconciliation")
    args = parser.parse_args()
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
