"""Fast, on-demand sync of SoftOne item status & categories into dim_items.

The full item_master pull (192k items × many joins) is heavy and only runs every
few hours. This is a light, targeted refresh of just the descriptive fields the
FnR/dashboards read as status — SoftOne is authoritative, so a changed OR cleared
value is written verbatim. Reuses the SQL-connector connection (worker/pyodbc).

It writes the same dim_items columns as the item_master stream, so its query has to
stay a strict subset of that stream's querypack — see the PARITY RULE on _STATUS_SQL.
"""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.services.inventory_snapshot import _sql_connection_for_tenant

# Only the light lookup joins — no MTRBALSHEET / sales, so it runs in seconds.
#
# PARITY RULE: this query is a strict subset of the tenant's `item_master`
# querypack and MUST stay so — same joins, same value expressions, same WHERE.
# Both write the SAME dim_items columns, so any divergence means the button and
# the 4-hourly stream fight each other and the last one to run wins. Two such
# divergences were shipped and corrected on 2026-09-16 (pharmacy295):
#   * no SODTYPE filter -> item CODEs that also exist under another SODTYPE
#     returned several rows; the dict below kept whichever came last, which was
#     usually the blank one, and the sync blanked 86 correct statuses.
#   * categories 1-3 were read from CCC88POCAT1/2/3 while the querypack reads
#     CCCCATEGORY01/02/03 — a different tree entirely, rewriting 13.5k items.
# The category tree is a per-tenant customization and is hardcoded here, which holds
# only because pharmacy295 is the single SQL-connector tenant (every other tenant runs
# the API/bridge connector, where _sql_connection_for_tenant returns no_sql_connector
# and this never executes). When onboarding a second SQL tenant, diff its item_master
# querypack against this block first — the bridge, for one, reads CCC88POCAT1/2/3.
_STATUS_SQL = """
SELECT
    CAST(I.CODE AS VARCHAR(128)) AS code,
    -- status_1 (UTBL04) / status_2 (UTBL05): NAME, else CODE, else the raw id.
    CAST(COALESCE(NULLIF(UT4.NAME, ''), NULLIF(UT4.CODE, ''),
                  NULLIF(CAST(IX.UTBL04 AS nvarchar(128)), '0'), '') AS nvarchar(128)) AS manual_order,
    CAST(COALESCE(NULLIF(UT5.NAME, ''), NULLIF(UT5.CODE, ''),
                  NULLIF(CAST(IX.UTBL05 AS nvarchar(128)), '0'), '') AS nvarchar(128)) AS commercial_status,
    CAST(ISNULL(CG.NAME, '')  AS nvarchar(255)) AS commercial_category,
    CAST(ISNULL(CT1.NAME, '') AS nvarchar(255)) AS category_1,
    CAST(ISNULL(CT2.NAME, '') AS nvarchar(255)) AS category_2,
    CAST(ISNULL(CT3.NAME, '') AS nvarchar(255)) AS category_3
FROM MTRL I WITH (NOLOCK)
LEFT JOIN MTREXTRA IX WITH (NOLOCK) ON IX.MTRL = I.MTRL AND IX.COMPANY = I.COMPANY
LEFT JOIN UTBL04 UT4 WITH (NOLOCK) ON UT4.UTBL04 = IX.UTBL04 AND UT4.COMPANY = IX.COMPANY AND UT4.SODTYPE = I.SODTYPE
LEFT JOIN UTBL05 UT5 WITH (NOLOCK) ON UT5.UTBL05 = IX.UTBL05 AND UT5.COMPANY = IX.COMPANY AND UT5.SODTYPE = I.SODTYPE
LEFT JOIN MTRPCATEGORY CG WITH (NOLOCK) ON CG.MTRPCATEGORY = I.MTRPCATEGORY AND CG.COMPANY = I.COMPANY
LEFT JOIN CCCCATEGORY01 CT1 WITH (NOLOCK) ON CT1.CATEGORY01 = I.CCCCATEGORY01
LEFT JOIN CCCCATEGORY02 CT2 WITH (NOLOCK) ON CT2.CATEGORY02 = I.CCCCATEGORY02
LEFT JOIN CCCCATEGORY03 CT3 WITH (NOLOCK) ON CT3.CATEGORY03 = I.CCCCATEGORY03
WHERE I.COMPANY = ?
  AND ISNULL(I.SODTYPE, 0) = 51
  AND NULLIF(ISNULL(I.CODE, ''), '') IS NOT NULL
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

    #  Same '0'/'-'/'null'/'n/a' -> NULL normalization the item_master stream applies,
    #  so pressing the button can never write a value the 4-hourly pull would reject.
    from app.services.ingestion.engine import _as_optional_softone_text as _norm

    rows = _fetch_status_rows(connection_string, company)
    soft: dict[str, tuple] = {}
    ambiguous: set[str] = set()
    for r in rows:
        if not r[0]:
            continue
        code = str(r[0])
        value = (
            _norm(r[1], 128), _norm(r[2], 128), _norm(r[3], 255),
            _norm(r[4], 255), _norm(r[5], 255), _norm(r[6], 255),
        )
        previous = soft.get(code)
        #  SODTYPE=51 should make CODE unique, but a catalog that still returns the
        #  same CODE twice with conflicting values must not be resolved by row order —
        #  that is exactly how 86 statuses got blanked. Leave those rows untouched.
        if previous is not None and previous != value:
            ambiguous.add(code)
            continue
        soft[code] = value
    for code in ambiguous:
        soft.pop(code, None)
    if not soft:
        return {'status': 'skipped', 'reason': 'no_rows', 'checked': 0, 'updated': 0,
                'ambiguous': len(ambiguous)}

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
    return {'status': 'ok', 'checked': len(soft), 'updated': len(diffs), 'ambiguous': len(ambiguous)}
