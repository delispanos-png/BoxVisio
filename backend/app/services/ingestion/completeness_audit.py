from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

# (circuit_name, table, date_column, stream_name)
_VERIFY_CIRCUITS: list[tuple[str, str, str, str]] = [
    ('sales', 'fact_sales', 'doc_date', 'sales_documents'),
    ('purchases', 'fact_purchases', 'doc_date', 'purchase_documents'),
    ('inventory', 'fact_inventory', 'doc_date', 'inventory_documents'),
    ('cashflows', 'fact_cashflows', 'doc_date', 'cash_transactions'),
    ('operating_expenses', 'fact_expenses', 'expense_date', 'operating_expenses'),
    ('supplier_balances', 'fact_supplier_balances', 'balance_date', 'supplier_balances'),
    ('customer_balances', 'fact_customer_balances', 'balance_date', 'customer_balances'),
]


def _pct(filled: int, total: int) -> float:
    if total <= 0:
        return 100.0
    return round((max(0, filled) / total) * 100.0, 2)


def collect_tenant_sync_completeness(engine: Engine) -> dict[str, Any]:
    with engine.begin() as conn:
        item_row = conn.execute(
            text(
                """
                SELECT
                  COUNT(*)::int AS total_items,
                  COUNT(softone_sotype)::int AS items_with_sotype,
                  COUNT(group_id)::int AS items_with_group_id
                FROM dim_items
                """
            )
        ).mappings().one()
        sales_row = conn.execute(
            text(
                """
                SELECT
                  COUNT(*)::int AS total_sales,
                  COUNT(document_type)::int AS sales_with_document_type,
                  COUNT(document_status)::int AS sales_with_document_status,
                  COUNT(payment_method)::int AS sales_with_payment_method,
                  COUNT(source_created_at)::int AS sales_with_source_created_at,
                  COUNT(channel_ext_id)::int AS sales_with_channel_ext_id,
                  COUNT(group_ext_id)::int AS sales_with_group_ext_id
                FROM fact_sales
                """
            )
        ).mappings().one()
        sales_dates = conn.execute(
            text(
                """
                SELECT
                  MIN(doc_date)::text AS min_doc_date,
                  MAX(doc_date)::text AS max_doc_date
                FROM fact_sales
                """
            )
        ).mappings().one()
        purchases_row = conn.execute(
            text(
                """
                SELECT
                  COUNT(*)::int AS total_purchases,
                  MIN(doc_date)::text AS min_doc_date,
                  MAX(doc_date)::text AS max_doc_date
                FROM fact_purchases
                """
            )
        ).mappings().one()
        inventory_row = conn.execute(
            text(
                """
                SELECT
                  COUNT(*)::int AS total_inventory,
                  MIN(doc_date)::text AS min_doc_date,
                  MAX(doc_date)::text AS max_doc_date
                FROM fact_inventory
                """
            )
        ).mappings().one()
        cashflow_row = conn.execute(
            text(
                """
                SELECT
                  COUNT(*)::int AS total_cashflows,
                  MIN(doc_date)::text AS min_doc_date,
                  MAX(doc_date)::text AS max_doc_date
                FROM fact_cashflows
                """
            )
        ).mappings().one()
        expenses_row = conn.execute(
            text(
                """
                SELECT
                  COUNT(*)::int AS total_expenses,
                  MIN(expense_date)::text AS min_expense_date,
                  MAX(expense_date)::text AS max_expense_date
                FROM fact_expenses
                """
            )
        ).mappings().one()

    total_items = int(item_row["total_items"] or 0)
    total_sales = int(sales_row["total_sales"] or 0)

    return {
        "items": {
            "total": total_items,
            "with_sotype": int(item_row["items_with_sotype"] or 0),
            "with_group_id": int(item_row["items_with_group_id"] or 0),
            "sotype_fill_pct": _pct(int(item_row["items_with_sotype"] or 0), total_items),
            "group_id_fill_pct": _pct(int(item_row["items_with_group_id"] or 0), total_items),
        },
        "sales": {
            "total": total_sales,
            "with_document_type": int(sales_row["sales_with_document_type"] or 0),
            "with_document_status": int(sales_row["sales_with_document_status"] or 0),
            "with_payment_method": int(sales_row["sales_with_payment_method"] or 0),
            "with_source_created_at": int(sales_row["sales_with_source_created_at"] or 0),
            "with_channel_ext_id": int(sales_row["sales_with_channel_ext_id"] or 0),
            "with_group_ext_id": int(sales_row["sales_with_group_ext_id"] or 0),
            "document_type_fill_pct": _pct(int(sales_row["sales_with_document_type"] or 0), total_sales),
            "document_status_fill_pct": _pct(int(sales_row["sales_with_document_status"] or 0), total_sales),
            "payment_method_fill_pct": _pct(int(sales_row["sales_with_payment_method"] or 0), total_sales),
            "source_created_at_fill_pct": _pct(int(sales_row["sales_with_source_created_at"] or 0), total_sales),
            "channel_ext_id_fill_pct": _pct(int(sales_row["sales_with_channel_ext_id"] or 0), total_sales),
            "group_ext_id_fill_pct": _pct(int(sales_row["sales_with_group_ext_id"] or 0), total_sales),
            "min_doc_date": sales_dates["min_doc_date"],
            "max_doc_date": sales_dates["max_doc_date"],
        },
        "purchases": {
            "total": int(purchases_row["total_purchases"] or 0),
            "min_doc_date": purchases_row["min_doc_date"],
            "max_doc_date": purchases_row["max_doc_date"],
        },
        "inventory": {
            "total": int(inventory_row["total_inventory"] or 0),
            "min_doc_date": inventory_row["min_doc_date"],
            "max_doc_date": inventory_row["max_doc_date"],
        },
        "cashflows": {
            "total": int(cashflow_row["total_cashflows"] or 0),
            "min_doc_date": cashflow_row["min_doc_date"],
            "max_doc_date": cashflow_row["max_doc_date"],
        },
        "operating_expenses": {
            "total": int(expenses_row["total_expenses"] or 0),
            "min_expense_date": expenses_row["min_expense_date"],
            "max_expense_date": expenses_row["max_expense_date"],
        },
    }


def verify_tenant_sync_window(
    engine: Engine,
    from_date: date,
    to_date: date,
    gap_tolerance_days: int = 3,
) -> dict[str, Any]:
    """
    Check actual row counts, date coverage, and data-quality signals for a sync window.
    Returns per-circuit verification results including any detected date gaps.
    Queries are read-only and do not modify any data.
    """
    circuits: dict[str, Any] = {}
    failed_batches: list[dict] = []
    dead_letters: list[dict] = []
    items_total: int = 0

    with engine.begin() as conn:
        # dim_items count (dimension — no date filter)
        try:
            items_total = int(conn.execute(text('SELECT COUNT(*)::int FROM dim_items')).scalar() or 0)
        except Exception as exc:
            items_total = -1

        for circuit, table, date_col, stream in _VERIFY_CIRCUITS:
            try:
                row = conn.execute(
                    text(
                        f'SELECT'
                        f'  COUNT(*)::int AS total,'
                        f'  MIN({date_col})::text AS min_date,'
                        f'  MAX({date_col})::text AS max_date,'
                        f'  COUNT(CASE WHEN external_id IS NULL THEN 1 END)::int AS null_ext_id'
                        f' FROM {table}'
                        f' WHERE {date_col} >= :fd AND {date_col} <= :td'
                    ),
                    {'fd': from_date, 'td': to_date},
                ).mappings().one()

                total = int(row['total'] or 0)
                min_date_str: str | None = row['min_date']
                max_date_str: str | None = row['max_date']
                null_ext_id = int(row['null_ext_id'] or 0)

                gap_at_end = False
                gap_at_start = False
                suggested_gap_from: str | None = None
                suggested_gap_to: str | None = None

                if total > 0 and max_date_str:
                    try:
                        max_d = date.fromisoformat(max_date_str[:10])
                        if (to_date - max_d).days > gap_tolerance_days:
                            gap_at_end = True
                            suggested_gap_from = max(max_d + timedelta(days=1), from_date).isoformat()
                            suggested_gap_to = to_date.isoformat()
                    except Exception:
                        pass

                if total > 0 and min_date_str:
                    try:
                        min_d = date.fromisoformat(min_date_str[:10])
                        if (min_d - from_date).days > gap_tolerance_days:
                            gap_at_start = True
                    except Exception:
                        pass

                circuits[circuit] = {
                    'stream': stream,
                    'table': table,
                    'total': total,
                    'min_date': min_date_str,
                    'max_date': max_date_str,
                    'null_ext_id_count': null_ext_id,
                    'gap_at_end': gap_at_end,
                    'gap_at_start': gap_at_start,
                    'suggested_gap_from': suggested_gap_from,
                    'suggested_gap_to': suggested_gap_to,
                    'ok': True,
                }
            except Exception as exc:
                circuits[circuit] = {'stream': stream, 'table': table, 'ok': False, 'error': str(exc)[:300]}

        # IngestBatch failures for the window
        batch_from = datetime(from_date.year, from_date.month, from_date.day)
        try:
            batch_rows = conn.execute(
                text(
                    'SELECT stream,'
                    '  COUNT(*)::int AS total_batches,'
                    '  SUM(CASE WHEN status = \'failed\' THEN 1 ELSE 0 END)::int AS failed_batches,'
                    '  SUM(rows_read)::int AS total_rows_read,'
                    '  SUM(rows_loaded)::int AS total_rows_loaded,'
                    '  SUM(rows_failed)::int AS total_rows_failed,'
                    '  MAX(finished_at)::text AS last_batch_at'
                    ' FROM ingest_batches'
                    ' WHERE started_at >= :bf'
                    ' GROUP BY stream ORDER BY stream'
                ),
                {'bf': batch_from},
            ).mappings().all()
            failed_batches = [
                dict(r)
                for r in batch_rows
                if int(r.get('failed_batches') or 0) > 0 or int(r.get('total_rows_failed') or 0) > 0
            ]
        except Exception:
            failed_batches = []

        # Pending dead letters for the window
        try:
            dl_rows = conn.execute(
                text(
                    'SELECT entity,'
                    '  COUNT(*)::int AS total,'
                    '  COUNT(CASE WHEN status = \'new\' THEN 1 END)::int AS pending,'
                    '  MAX(created_at)::text AS last_failure_at'
                    ' FROM ingest_dead_letter'
                    ' WHERE created_at >= :dl_from'
                    ' GROUP BY entity ORDER BY entity'
                ),
                {'dl_from': batch_from},
            ).mappings().all()
            dead_letters = [dict(r) for r in dl_rows if int(r.get('pending') or 0) > 0]
        except Exception:
            dead_letters = []

    return {
        'from_date': from_date.isoformat(),
        'to_date': to_date.isoformat(),
        'circuits': circuits,
        'items_total': items_total,
        'failed_batches': failed_batches,
        'dead_letters': dead_letters,
    }


def check_recent_batch_health(engine: Engine, since: datetime) -> dict[str, Any]:
    """
    Lightweight check for the incremental verification path.

    Queries only the most recent ingest_batches and ingest_dead_letter rows —
    no date-range scans of fact tables, no historical gap detection.
    Safe to call after every 5-minute incremental drain.
    """
    failed_batches: list[dict] = []
    pending_dead_letters = 0

    with engine.begin() as conn:
        try:
            rows = conn.execute(
                text(
                    'SELECT stream,'
                    '  COUNT(*)::int AS total_batches,'
                    '  SUM(CASE WHEN status = \'failed\' THEN 1 ELSE 0 END)::int AS failed,'
                    '  SUM(rows_read)::int AS rows_read,'
                    '  SUM(rows_loaded)::int AS rows_loaded,'
                    '  SUM(rows_failed)::int AS rows_failed,'
                    '  MAX(finished_at)::text AS last_batch_at'
                    ' FROM ingest_batches'
                    ' WHERE started_at >= :since'
                    ' GROUP BY stream ORDER BY stream'
                ),
                {'since': since},
            ).mappings().all()
            failed_batches = [
                dict(r)
                for r in rows
                if int(r.get('failed') or 0) > 0 or int(r.get('rows_failed') or 0) > 0
            ]
        except Exception:
            failed_batches = []

        try:
            pending_dead_letters = int(
                conn.execute(
                    text(
                        'SELECT COUNT(*)::int'
                        ' FROM ingest_dead_letter'
                        ' WHERE created_at >= :since AND status = \'new\''
                    ),
                    {'since': since},
                ).scalar()
                or 0
            )
        except Exception:
            pending_dead_letters = 0

    return {
        'since': since.isoformat(),
        'failed_batches': failed_batches,
        'pending_dead_letters': pending_dead_letters,
        'has_failures': bool(failed_batches) or pending_dead_letters > 0,
    }
