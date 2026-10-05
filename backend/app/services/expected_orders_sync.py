"""Light refresh of supplier-order line quantities (ordered / covered / cancelled).

FnR's «Αναμενόμενα» are the still-open quantities of supplier orders. A line closes
when it is received or cancelled, and SoftOne records that on the order line without
touching the order's change stamp, so the incremental supplier_orders stream cannot
see it. The FnR «Συγχρονισμός αναμενόμενων» button used to answer that by re-ingesting
every order line of the last 400 days through the full pipeline: 92k lines one by one
on pharmacy295, 50-70 minutes per click with the tenant's ingest lock held.

This reads just the three quantities for the same lines with the tenant's own
supplier_orders query (same filters, same expressions) and updates only the lines
that differ, in a few seconds. Lines SoftOne no longer returns (order cancelled or
deleted) are closed. New orders still arrive through the regular stream.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import date, timedelta

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.control import TenantConnection
from app.services.connection_secrets import build_odbc_connection_string, decrypt_sqlserver_secret
from app.services.ingestion.reconciliation import _bind, _company_id, _templates
from app.services.sqlserver_connector import _connect

logger = logging.getLogger(__name__)

_SQL_CONNECTORS = ('sql_connector', 'pharmacyone_sql', 'generic_sql')
_QTY_EPS = 1e-4


def _fetch_line_quantities(conn: TenantConnection, from_date: date) -> dict[str, tuple[float, float, float]]:
    template = _templates(conn)['supplier_orders'].strip().rstrip(';')
    sql = (
        'SELECT CAST(src.external_id AS nvarchar(128)), src.order_qty, src.covered_qty, src.cancelled_qty '
        f'FROM ({template}) src'
    )
    bound, params = _bind(sql, from_date=from_date, to_date=date.today(), company_id=_company_id(conn))
    secret = decrypt_sqlserver_secret(conn.enc_payload)
    out: dict[str, tuple[float, float, float]] = {}
    with _connect(build_odbc_connection_string(secret), query_timeout=180) as db:
        cur = db.cursor()
        cur.execute(bound, *params)
        for ext_id, order_qty, covered_qty, cancelled_qty in cur.fetchall():
            if ext_id:
                out[str(ext_id)] = (float(order_qty or 0), float(covered_qty or 0), float(cancelled_qty or 0))
    return out


async def refresh_expected_orders(
    control_db: AsyncSession,
    tenant_db: AsyncSession,
    *,
    tenant_id: int,
    lookback_days: int = 400,
) -> dict:
    """Bring ordered/covered/cancelled quantities of supplier-order lines in line with
    SoftOne. Returns {'status', 'checked', 'updated', 'closed', 'new'}."""
    conn = (
        await control_db.execute(
            select(TenantConnection)
            .where(
                TenantConnection.tenant_id == tenant_id,
                TenantConnection.is_active.is_(True),
                TenantConnection.connector_type.in_(_SQL_CONNECTORS),
            )
            .order_by(TenantConnection.id.desc())
            .limit(1)
        )
    ).scalar_one_or_none()
    if conn is None or not conn.enc_payload:
        return {'status': 'skipped', 'reason': 'no_sql_connector', 'checked': 0, 'updated': 0, 'closed': 0, 'new': 0}
    if not (_templates(conn).get('supplier_orders') or '').strip():
        return {'status': 'skipped', 'reason': 'no_template', 'checked': 0, 'updated': 0, 'closed': 0, 'new': 0}

    from_date = date.today() - timedelta(days=max(1, int(lookback_days)))
    source = await asyncio.to_thread(_fetch_line_quantities, conn, from_date)

    current = (
        await tenant_db.execute(
            text(
                'SELECT external_id, order_qty, covered_qty, cancelled_qty '
                'FROM fact_supplier_orders WHERE doc_date >= :f'
            ),
            {'f': from_date},
        )
    ).all()

    updates: list[dict] = []
    missing_open: list[str] = []
    known: set[str] = set()
    for ext_id, order_qty, covered_qty, cancelled_qty in current:
        key = str(ext_id)
        known.add(key)
        have = (float(order_qty or 0), float(covered_qty or 0), float(cancelled_qty or 0))
        want = source.get(key)
        if want is None:
            if have[0] - have[1] - have[2] > _QTY_EPS:
                missing_open.append(key)
            continue
        if any(abs(a - b) > _QTY_EPS for a, b in zip(have, want)):
            updates.append({'e': key, 'o': want[0], 'c': want[1], 'x': want[2]})

    # Closing lines SoftOne did not return is only safe when the read was plainly whole.
    closed = 0
    if missing_open and len(source) >= 0.5 * max(1, len(current)):
        for i in range(0, len(missing_open), 5000):
            result = await tenant_db.execute(
                text(
                    'UPDATE fact_supplier_orders '
                    'SET cancelled_qty = GREATEST(COALESCE(order_qty, 0) - COALESCE(covered_qty, 0), 0), updated_at = now() '
                    'WHERE external_id = ANY(CAST(:ids AS text[]))'
                ),
                {'ids': missing_open[i : i + 5000]},
            )
            closed += int(result.rowcount or 0)
    elif missing_open:
        logger.warning(
            'expected_orders_close_skipped tenant_id=%s missing_open=%s source=%s current=%s',
            tenant_id, len(missing_open), len(source), len(current),
        )

    for i in range(0, len(updates), 1000):
        await tenant_db.execute(
            text(
                'UPDATE fact_supplier_orders SET order_qty = :o, covered_qty = :c, cancelled_qty = :x, '
                'updated_at = now() WHERE external_id = :e'
            ),
            updates[i : i + 1000],
        )
    await tenant_db.commit()
    return {
        'status': 'ok',
        'checked': len(source),
        'updated': len(updates),
        'closed': closed,
        'new': sum(1 for key in source if key not in known),
    }
