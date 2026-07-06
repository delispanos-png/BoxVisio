"""Fast, on-demand sync of SoftOne item status & categories into dim_items.

The full item_master pull (192k items × many joins) is heavy and only runs every
few hours. This is a light, targeted refresh of just the descriptive fields the
FnR/dashboards read as status — SoftOne is authoritative, so a changed OR cleared
value is written verbatim. Reuses the SQL-connector connection (worker/pyodbc).
"""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.services.inventory_snapshot import _sql_connection_for_tenant

# Only the light lookup joins — no MTRBALSHEET / sales, so it runs in seconds.
_STATUS_SQL = """
SELECT
    CAST(I.CODE AS VARCHAR(128)) AS code,
    ISNULL(UT4.NAME, '') AS manual_order,        -- status_1 (UTBL04)
    ISNULL(UT5.NAME, '') AS commercial_status,   -- status_2 (UTBL05)
    ISNULL(CG.NAME, '')  AS commercial_category,
    ISNULL(PC1.NAME, '') AS category_1,
    ISNULL(PC2.NAME, '') AS category_2,
    ISNULL(PC3.NAME, '') AS category_3
FROM MTRL I WITH (NOLOCK)
LEFT JOIN MTREXTRA IX WITH (NOLOCK) ON IX.MTRL = I.MTRL AND IX.COMPANY = I.COMPANY
LEFT JOIN UTBL04 UT4 WITH (NOLOCK) ON UT4.UTBL04 = IX.UTBL04 AND UT4.COMPANY = IX.COMPANY AND UT4.SODTYPE = I.SODTYPE
LEFT JOIN UTBL05 UT5 WITH (NOLOCK) ON UT5.UTBL05 = IX.UTBL05 AND UT5.COMPANY = IX.COMPANY AND UT5.SODTYPE = I.SODTYPE
LEFT JOIN MTRPCATEGORY CG WITH (NOLOCK) ON CG.MTRPCATEGORY = I.MTRPCATEGORY AND CG.COMPANY = I.COMPANY
LEFT JOIN CCC88POCAT1 PC1 WITH (NOLOCK) ON PC1.CCC88POCAT1 = I.CCC88POCAT1
LEFT JOIN CCC88POCAT2 PC2 WITH (NOLOCK) ON PC2.CCC88POCAT2 = I.CCC88POCAT2
LEFT JOIN CCC88POCAT3 PC3 WITH (NOLOCK) ON PC3.CCC88POCAT3 = I.CCC88POCAT3
WHERE I.COMPANY = ? AND NULLIF(ISNULL(I.CODE, ''), '') IS NOT NULL
"""


def _fetch_status_rows(connection_string: str, company) -> list[tuple]:
    import pyodbc

    cn = pyodbc.connect(connection_string, timeout=30)
    try:
        cn.timeout = 180
        cur = cn.cursor()
        cur.execute(_STATUS_SQL, company)
        return cur.fetchall()
    finally:
        cn.close()


async def refresh_item_status(control_db: AsyncSession, tenant_db: AsyncSession, *, tenant_id: int) -> dict:
    """Sync manual_order_category / commercial_status / categories from SoftOne into
    dim_items for every item, updating only the rows that differ. Returns
    {'status', 'checked', 'updated'}.
    """
    connection_string, company = await _sql_connection_for_tenant(control_db, tenant_id)
    if not connection_string:
        return {'status': 'skipped', 'reason': 'no_sql_connector', 'checked': 0, 'updated': 0}

    rows = _fetch_status_rows(connection_string, company)
    soft = {
        str(r[0]): (r[1] or None, r[2] or None, r[3] or None, r[4] or None, r[5] or None, r[6] or None)
        for r in rows
        if r[0]
    }
    if not soft:
        return {'status': 'skipped', 'reason': 'no_rows', 'checked': 0, 'updated': 0}

    current = (
        await tenant_db.execute(
            text(
                'SELECT external_id, manual_order_category, commercial_status, commercial_category, '
                'category_1, category_2, category_3 FROM dim_items'
            )
        )
    ).all()

    diffs: list[dict] = []
    for code, mo, cs, cc, c1, c2, c3 in current:
        source = soft.get(str(code))
        if source is None:  # item not in SoftOne right now — leave untouched
            continue
        if (mo or None, cs or None, cc or None, c1 or None, c2 or None, c3 or None) != source:
            diffs.append(
                {
                    'c': str(code),
                    'mo': source[0],
                    'cs': source[1],
                    'cc': source[2],
                    'c1': source[3],
                    'c2': source[4],
                    'c3': source[5],
                }
            )

    for i in range(0, len(diffs), 1000):
        await tenant_db.execute(
            text(
                'UPDATE dim_items SET manual_order_category = :mo, commercial_status = :cs, '
                'commercial_category = :cc, category_1 = :c1, category_2 = :c2, category_3 = :c3, '
                'updated_at = now() WHERE external_id = :c'
            ),
            diffs[i : i + 1000],
        )
    await tenant_db.commit()
    return {'status': 'ok', 'checked': len(soft), 'updated': len(diffs)}
