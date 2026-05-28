from __future__ import annotations

from app.core.config import settings


DEFAULT_STREAM_CHUNK_DAYS: dict[str, int] = {
    # Heavy SoftOne document streams stay small to reduce timeouts, locks and
    # duplicate upsert pressure during production backfills.
    'sales_documents': 1,
    'purchase_documents': 1,
    'inventory_documents': 1,
    'item_master': 31,
    # Operational streams are lighter and can move in wider windows.
    'supplier_orders': 7,
    'cash_transactions': 14,
    'operating_expenses': 14,
    # Balance streams are snapshots; a wider window is fine when used as dated
    # periods, while several call sites enqueue only the final snapshot.
    'supplier_balances': 31,
    'customer_balances': 31,
}


def stream_chunk_days(
    stream: str | None,
    requested_chunk_days: int | None = None,
    *,
    profile: str = 'safe',
) -> int:
    requested = max(1, int(requested_chunk_days or 1))
    normalized = str(stream or '').strip().lower()
    normalized_profile = str(profile or 'safe').strip().lower()
    if normalized_profile in {'bulk', 'full', 'initial'}:
        # Initial/full tenant backfills run outside the live incremental path.
        # Let them use the requested wider windows; the ingest job timeout and
        # per-stream retries still protect the worker from runaway chunks.
        return requested
    if normalized == 'sales_documents':
        configured = int(getattr(settings, 'ingest_backfill_sales_chunk_days', 1) or 1)
        return max(1, min(requested, configured, DEFAULT_STREAM_CHUNK_DAYS[normalized]))
    if normalized == 'purchase_documents':
        configured = int(getattr(settings, 'ingest_backfill_purchases_chunk_days', 1) or 1)
        return max(1, min(requested, configured, DEFAULT_STREAM_CHUNK_DAYS[normalized]))
    if normalized == 'inventory_documents':
        configured = int(getattr(settings, 'ingest_backfill_inventory_chunk_days', 1) or 1)
        return max(1, min(requested, configured, DEFAULT_STREAM_CHUNK_DAYS[normalized]))
    default_days = DEFAULT_STREAM_CHUNK_DAYS.get(normalized, requested)
    return max(1, min(requested, int(default_days)))


def stream_chunk_policy(requested_chunk_days: int | None = None, *, profile: str = 'safe') -> dict[str, int]:
    requested = max(1, int(requested_chunk_days or 1))
    return {
        stream: stream_chunk_days(stream, requested, profile=profile)
        for stream in DEFAULT_STREAM_CHUNK_DAYS
    }
