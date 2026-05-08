#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import date, timedelta
from pathlib import Path

SCRIPT_PATH = Path(__file__).resolve()
PROJECT_ROOT = SCRIPT_PATH.parents[2]
BACKEND_ROOT = PROJECT_ROOT / "backend"
sys.path.insert(0, str(BACKEND_ROOT))
sys.path.insert(0, str(PROJECT_ROOT))

from app.services.ingestion import engine as ingest_engine  # noqa: E402
from app.services.ingestion.engine import process_job  # noqa: E402


def _iter_chunks(from_date: date, to_date: date, chunk_days: int):
    current = from_date
    while current <= to_date:
        chunk_to = min(current + timedelta(days=chunk_days - 1), to_date)
        yield current, chunk_to
        current = chunk_to + timedelta(days=1)


async def _run(
    tenant: str,
    from_date: date,
    to_date: date,
    chunk_days: int,
    limit: int,
    skip_dim_upserts: bool,
) -> None:
    original_upsert_dims = ingest_engine._upsert_dims_from_row
    if skip_dim_upserts:
        async def _noop_upsert_dims(*args, **kwargs):
            return None
        ingest_engine._upsert_dims_from_row = _noop_upsert_dims
    total = 0
    try:
        for chunk_from, chunk_to in _iter_chunks(from_date, to_date, chunk_days):
            job = {
                "tenant_slug": tenant,
                "connector": "sql_connector",
                "stream": "sales_documents",
                "entity": "sales",
                "payload": {
                    "from_date": chunk_from.isoformat(),
                    "to_date": chunk_to.isoformat(),
                    "ignore_sync_state": True,
                    "backfill": True,
                    "limit": limit,
                },
                "attempt": 0,
                "max_retries": 0,
            }
            result = await process_job(job)
            total += int(result.get("processed") or 0)
            print(
                json.dumps(
                    {
                        "chunk_from": chunk_from.isoformat(),
                        "chunk_to": chunk_to.isoformat(),
                        "processed": result.get("processed"),
                        "min_doc_date": result.get("min_doc_date"),
                        "max_doc_date": result.get("max_doc_date"),
                    },
                    ensure_ascii=False,
                ),
                flush=True,
            )
        print(json.dumps({"tenant": tenant, "total_processed": total}, ensure_ascii=False), flush=True)
    finally:
        ingest_engine._upsert_dims_from_row = original_upsert_dims


def main() -> int:
    parser = argparse.ArgumentParser(description="Run sales backfill serially for one tenant.")
    parser.add_argument("--tenant", required=True)
    parser.add_argument("--from-date", required=True)
    parser.add_argument("--to-date", required=True)
    parser.add_argument("--chunk-days", type=int, default=7)
    parser.add_argument("--limit", type=int, default=10000)
    parser.add_argument("--skip-dim-upserts", action="store_true")
    args = parser.parse_args()

    asyncio.run(
        _run(
            tenant=args.tenant,
            from_date=date.fromisoformat(args.from_date),
            to_date=date.fromisoformat(args.to_date),
            chunk_days=max(1, int(args.chunk_days)),
            limit=max(1, int(args.limit)),
            skip_dim_upserts=bool(args.skip_dim_upserts),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
