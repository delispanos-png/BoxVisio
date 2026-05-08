#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from datetime import date, timedelta
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve()
PROJECT_ROOT = SCRIPT_PATH.parents[1]
BACKEND_ROOT = PROJECT_ROOT / "backend"
sys.path.insert(0, str(BACKEND_ROOT))
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(SCRIPT_PATH.parent))

from app.services.ingestion.progress import get_ingest_progress  # noqa: E402
from verify_sales_ingest_period import run as run_verify  # noqa: E402
from worker.tasks import enqueue_sql_backfill  # noqa: E402


def _group_contiguous_days(days: list[date]) -> list[tuple[date, date]]:
    if not days:
        return []
    ordered = sorted(set(days))
    groups: list[tuple[date, date]] = []
    start = ordered[0]
    end = ordered[0]
    for current in ordered[1:]:
        if current == end + timedelta(days=1):
            end = current
            continue
        groups.append((start, end))
        start = current
        end = current
    groups.append((start, end))
    return groups


def _extract_mismatch_days(recon: dict, fallback_from: date, fallback_to: date) -> list[date]:
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


def _wait_for_operation(tenant_slug: str, timeout_seconds: int) -> dict:
    started = time.time()
    latest = get_ingest_progress(tenant_slug)
    while time.time() - started <= timeout_seconds:
        latest = get_ingest_progress(tenant_slug)
        status = str(latest.get("status") or "")
        queue_depth = int(latest.get("current_queue_depth") or 0)
        if status in {"completed", "idle"} and queue_depth <= 0:
            return latest
        if status in {"failed", "stopped"}:
            raise RuntimeError(json.dumps(latest, ensure_ascii=False))
        time.sleep(5)
    raise TimeoutError(json.dumps(latest, ensure_ascii=False))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Reconcile source-vs-target sales period and auto-recover missing days with targeted backfill."
    )
    parser.add_argument("--tenant", required=True, help="Tenant slug, e.g. pharmacy295")
    parser.add_argument("--from-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--to-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--max-passes", type=int, default=3, help="Max reconcile/recover loops")
    parser.add_argument("--sample-size", type=int, default=5000, help="Reconciliation sample size")
    parser.add_argument("--chunk-days", type=int, default=1, help="Backfill chunk days for mismatch windows")
    parser.add_argument("--limit", type=int, default=10000, help="SQL page size limit per query")
    parser.add_argument("--wait-timeout", type=int, default=7200, help="Seconds to wait per recovery pass")
    args = parser.parse_args()

    from_date = date.fromisoformat(args.from_date)
    to_date = date.fromisoformat(args.to_date)
    if from_date > to_date:
        raise SystemExit("--from-date must be <= --to-date")

    max_passes = max(1, int(args.max_passes))
    sample_size = max(100, int(args.sample_size))
    chunk_days = max(1, int(args.chunk_days))
    limit = max(100, int(args.limit))

    history: list[dict] = []
    for pass_no in range(1, max_passes + 1):
        snapshot = asyncio.run(run_verify(args.tenant, from_date, to_date, sample_size))
        recon = dict(snapshot.get("reconciliation") or {})
        row_delta = int(recon.get("row_delta") or 0)
        net_delta = float(recon.get("net_delta") or 0.0)
        history.append(
            {
                "pass": pass_no,
                "row_delta": row_delta,
                "net_delta": round(net_delta, 2),
                "missing_in_target_count": int(recon.get("missing_in_target_count") or 0),
                "extra_in_target_count": int(recon.get("extra_in_target_count") or 0),
            }
        )
        print(
            json.dumps(
                {
                    "event": "reconcile",
                    "pass": pass_no,
                    "row_delta": row_delta,
                    "net_delta": round(net_delta, 2),
                },
                ensure_ascii=False,
            )
        )

        if row_delta == 0 and abs(net_delta) < 0.01:
            print(
                json.dumps(
                    {
                        "status": "ok",
                        "tenant": args.tenant,
                        "from_date": from_date.isoformat(),
                        "to_date": to_date.isoformat(),
                        "passes": pass_no,
                        "history": history,
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return 0

        mismatch_days = _extract_mismatch_days(recon, from_date, to_date)
        windows = _group_contiguous_days(mismatch_days)
        for window_from, window_to in windows:
            enqueue_sql_backfill(
                tenant_slug=args.tenant,
                from_date_str=window_from.isoformat(),
                to_date_str=window_to.isoformat(),
                chunk_days=chunk_days,
                limit=limit,
                include_sales=True,
                include_purchases=False,
                include_inventory=False,
                include_cashflows=False,
                include_supplier_balances=False,
                include_customer_balances=False,
                include_operating_expenses=False,
                operation="auto_sales_period_recovery",
            )
        _wait_for_operation(args.tenant, args.wait_timeout)

    final_snapshot = asyncio.run(run_verify(args.tenant, from_date, to_date, sample_size))
    print(
        json.dumps(
            {
                "status": "incomplete",
                "tenant": args.tenant,
                "from_date": from_date.isoformat(),
                "to_date": to_date.isoformat(),
                "passes": max_passes,
                "history": history,
                "final_reconciliation": final_snapshot.get("reconciliation"),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
