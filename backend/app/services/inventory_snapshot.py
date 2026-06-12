"""Reliable stock-balance snapshot refresh, straight from SoftOne MTRBALSHEET.

The normal inventory ingest only refreshes the stock *balance* on a FULL sync
(`@last_sync_ts IS NULL`); incremental syncs pull movement documents but not the
balance, so today's `fact_inventory` snapshot can be stale. This service does a
light, fast, deterministic re-pull of the current net balance per item *and
warehouse* (IMPQTY1-EXPQTY1 at the latest fiscal period, PERIOD=0) and writes
today's `fact_inventory` snapshot directly. Callers then refresh the inventory
aggregates so every stock-dependent circuit (FnR, Availability, Destocking,
Αποθέματα, Είδη) updates together. Used by the nightly 22:00 job and the
pre-FnR refresh action.
"""

from __future__ import annotations

from datetime import date

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.models.control import TenantConnection
from app.services.connection_secrets import decrypt_sqlserver_secret, build_odbc_connection_string


# Net on-hand per (item, warehouse, branch) at the latest fiscal period (PERIOD=0).
# warehouse_ext_id = WHOUSE, branch_ext_id = 'COMPANY:branch' — exactly the formats the
# normal inventory_facts.sql produces, so the aggregates dedup/group correctly.
_BALANCE_SQL = """
WITH sp AS (
    SELECT COMPANY, MAX(FISCPRD) AS FISCPRD
    FROM MTRBALSHEET WITH (NOLOCK)
    WHERE COMPANY = ? AND PERIOD = 0
    GROUP BY COMPANY
),
mac AS (
    -- Moving-average unit cost = lifetime purchase value / purchase qty (all periods, all
    -- years). The on-hand cost is qty * this — matching SoftOne's «Κόστος υπολ.». Summing
    -- IMPVAL-EXPVAL is wrong (EXPVAL is valued at the cost at time of sale, and the year
    -- opening value is not in IMPVAL) — it went negative per warehouse and overstated totals.
    SELECT COMPANY, MTRL,
           CASE WHEN SUM(ISNULL(PURQTY, 0)) > 0 THEN SUM(ISNULL(PURVAL, 0)) / SUM(ISNULL(PURQTY, 0)) ELSE 0 END AS unit_cost
    FROM MTRBALSHEET WITH (NOLOCK)
    WHERE COMPANY = ?
    GROUP BY COMPANY, MTRL
)
SELECT
    CAST(ISNULL(I.CODE, I.MTRL) AS VARCHAR(128)) AS code,
    CAST(ISNULL(S.WHOUSE, 0) AS VARCHAR(64)) AS warehouse_ext_id,
    CAST(CAST(ISNULL(S.COMPANY, 0) AS VARCHAR(32)) + ':' +
         CAST(ISNULL(BR.BRANCH, ISNULL(NULLIF(W.WHOUSEG, 0), ISNULL(S.WHOUSE, 0))) AS VARCHAR(32))
         AS VARCHAR(64)) AS branch_ext_id,
    SUM(ISNULL(S.IMPQTY1, 0) - ISNULL(S.EXPQTY1, 0)) AS qty,
    SUM((ISNULL(S.IMPQTY1, 0) - ISNULL(S.EXPQTY1, 0)) * ISNULL(I.PRICEW, 0)) AS val,
    SUM(ISNULL(S.IMPQTY1, 0) - ISNULL(S.EXPQTY1, 0)) * MAX(ISNULL(mac.unit_cost, 0)) AS cost,
    SUM((ISNULL(S.IMPQTY1, 0) - ISNULL(S.EXPQTY1, 0)) * ISNULL(I.PRICER, 0)) AS retail
FROM MTRBALSHEET S WITH (NOLOCK)
-- The on-hand balance is the cumulative net over ALL periods of the current fiscal
-- year (PERIOD 0 = year opening + PERIOD 1..12 = monthly movements). Filtering PERIOD=0
-- captures ONLY the year-opening balance and misses every movement since — e.g. C006091
-- shows 10 at PERIOD=0 vs the true 48 (Logika 34, Κηφισιά 6, ...). Sum all periods.
JOIN sp ON sp.COMPANY = S.COMPANY AND sp.FISCPRD = S.FISCPRD
JOIN MTRL I WITH (NOLOCK) ON I.MTRL = S.MTRL AND I.COMPANY = S.COMPANY
LEFT JOIN mac ON mac.COMPANY = S.COMPANY AND mac.MTRL = S.MTRL
LEFT JOIN WHOUSE W WITH (NOLOCK) ON W.WHOUSE = S.WHOUSE AND W.COMPANY = S.COMPANY
LEFT JOIN BRANCH BR WITH (NOLOCK)
    ON BR.BRANCH = ISNULL(NULLIF(W.WHOUSEG, 0), ISNULL(S.WHOUSE, 0)) AND BR.COMPANY = S.COMPANY
WHERE S.COMPANY = ? AND NULLIF(ISNULL(I.CODE, ''), '') IS NOT NULL
GROUP BY
    CAST(ISNULL(I.CODE, I.MTRL) AS VARCHAR(128)),
    CAST(ISNULL(S.WHOUSE, 0) AS VARCHAR(64)),
    CAST(CAST(ISNULL(S.COMPANY, 0) AS VARCHAR(32)) + ':' +
         CAST(ISNULL(BR.BRANCH, ISNULL(NULLIF(W.WHOUSEG, 0), ISNULL(S.WHOUSE, 0))) AS VARCHAR(32)) AS VARCHAR(64))
HAVING ABS(SUM(ISNULL(S.IMPQTY1, 0) - ISNULL(S.EXPQTY1, 0))) > 0.0001
"""

_SQL_CONNECTOR_TYPES = ('sql_connector', 'pharmacyone_sql', 'generic_sql')


async def _sql_connection_for_tenant(control_db: AsyncSession, tenant_id: int) -> tuple[str | None, object]:
    conn = (
        await control_db.execute(
            select(TenantConnection).where(
                TenantConnection.tenant_id == tenant_id,
                TenantConnection.is_active.is_(True),
                TenantConnection.connector_type.in_(_SQL_CONNECTOR_TYPES),
            )
        )
    ).scalars().first()
    if conn is None or not conn.enc_payload:
        return None, None
    try:
        secret = decrypt_sqlserver_secret(conn.enc_payload)
    except (KeyError, ValueError, TypeError):
        return None, None
    cs = build_odbc_connection_string(secret).replace('{ODBC_DRIVER}', settings.odbc_driver)
    params = conn.connection_parameters if isinstance(conn.connection_parameters, dict) else {}
    auth = params.get('auth_config') if isinstance(params.get('auth_config'), dict) else {}
    company = params.get('company_id') or params.get('company') or auth.get('company') or auth.get('COMPANY')
    return cs, company


def _fetch_balance_rows(connection_string: str, company) -> list[tuple]:
    import pyodbc

    cn = pyodbc.connect(connection_string, timeout=30)
    try:
        cn.timeout = 120
        cur = cn.cursor()
        cur.execute(_BALANCE_SQL, company, company, company)
        return cur.fetchall()
    finally:
        cn.close()


async def refresh_inventory_snapshot(
    control_db: AsyncSession,
    tenant_db: AsyncSession,
    *,
    tenant_id: int,
    snapshot_day: date | None = None,
) -> dict:
    """Re-pull SoftOne net stock per warehouse and rewrite today's fact_inventory snapshot.

    Read-only on SoftOne; on the BI side it replaces today's snapshot rows only. The caller is
    expected to refresh the inventory aggregates afterwards. Returns
    {'status', 'items', 'rows', 'value', 'snapshot_date'}.
    """
    snapshot_day = snapshot_day or date.today()
    connection_string, company = await _sql_connection_for_tenant(control_db, tenant_id)
    if not connection_string:
        return {'status': 'skipped', 'reason': 'no_sql_connector', 'items': 0, 'rows': 0}

    rows = _fetch_balance_rows(connection_string, company)
    if not rows:
        return {'status': 'skipped', 'reason': 'no_rows', 'items': 0, 'rows': 0}

    batch = [
        {
            'eid': f'STKSNAP|{snapshot_day.isoformat()}|{str(r[1])}|{str(r[0])}',
            'd': snapshot_day,
            'code': str(r[0]),
            'wh': str(r[1]) if r[1] is not None else None,
            'br': str(r[2]) if r[2] is not None else None,
            'q': float(r[3] or 0),
            'v': float(r[5] or 0),       # cost_amount (IMPVAL-EXPVAL)
            'val': float(r[4] or 0),     # value_amount (qty * PRICEW, wholesale)
            'retail': float(r[6] or 0),  # retail valuation (qty * PRICER) -> source_payload_json
        }
        for r in rows
        if r[0]
    ]

    # Match SoftOne's per-item view exactly: an item whose warehouse balances net to ~0
    # (e.g. +1 in one warehouse and -1 in another from an unreconciled transfer) carries no
    # real stock. SoftOne's stock list filters these out (ABS(SUM) > 0); we mirror that so the
    # item counts and "has-stock" scope agree with SoftOne and no phantom items are counted.
    net_by_code: dict[str, float] = {}
    for b in batch:
        net_by_code[b['code']] = net_by_code.get(b['code'], 0.0) + b['q']
    batch = [b for b in batch if abs(net_by_code.get(b['code'], 0.0)) > 1e-4]

    # Replace today's snapshot only (movement docs and other days untouched).
    await tenant_db.execute(
        text("DELETE FROM fact_inventory WHERE doc_date = :d AND COALESCE(movement_type, 'snapshot') = 'snapshot'"),
        {'d': snapshot_day},
    )
    for i in range(0, len(batch), 1000):
        await tenant_db.execute(
            text(
                """
                INSERT INTO fact_inventory
                    (external_id, doc_date, item_code, branch_ext_id, warehouse_ext_id,
                     qty_on_hand, qty_available, qty_reserved, qty_expected,
                     value_amount, cost_amount, source_payload_json, movement_type, updated_at, created_at)
                VALUES
                    (:eid, :d, :code, :br, :wh,
                     :q, :q, 0, 0,
                     :val, :v, jsonb_build_object('retail_value_amount', CAST(:retail AS double precision)),
                     'snapshot', now(), now())
                """
            ),
            batch[i : i + 1000],
        )
    await tenant_db.commit()

    distinct_items = len({b['code'] for b in batch})
    total_value = sum(b['val'] for b in batch)
    return {
        'status': 'ok',
        'items': distinct_items,
        'rows': len(batch),
        'value': round(total_value, 2),
        'snapshot_date': snapshot_day.isoformat(),
    }
