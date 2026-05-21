#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from sqlalchemy import create_engine, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

SCRIPT_PATH = Path(__file__).resolve()
PROJECT_ROOT: Path | None = None
BACKEND_ROOT: Path | None = None
for candidate in [SCRIPT_PATH.parent, *SCRIPT_PATH.parents]:
    if (candidate / "backend" / "app").exists():
        PROJECT_ROOT = candidate
        BACKEND_ROOT = candidate / "backend"
        break
    if (candidate / "app").exists() and (candidate / "requirements.txt").exists():
        BACKEND_ROOT = candidate
        PROJECT_ROOT = candidate.parent
        break
if BACKEND_ROOT is None or PROJECT_ROOT is None:
    raise RuntimeError("Could not locate backend root.")
sys.path.insert(0, str(BACKEND_ROOT))

from app.core.config import settings  # noqa: E402
from app.models.control import Tenant, TenantConnection  # noqa: E402
from app.models.tenant import DimBrand, DimGroup, DimItem, FactInventory  # noqa: E402
from app.services.connection_secrets import build_odbc_connection_string, decrypt_sqlserver_secret  # noqa: E402
from app.services.ingestion.base import ConnectorContext  # noqa: E402
from app.services.ingestion.external_api_connector import ExternalApiIngestConnector  # noqa: E402
from app.services.sqlserver_connector import _connect  # noqa: E402

_BATCH_SIZE = 2000


def _normalize_softone_text(value: object | None) -> str | None:
    txt = str(value or "").strip()
    if not txt:
        return None
    if txt.lower() in {"0", "-", "null", "n/a", "na"}:
        return None
    return txt


def _normalize_optional_bool(value: object | None) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(int(value))
    txt = str(value).strip().lower()
    if not txt:
        return None
    if txt in {"1", "true", "yes", "y", "on", "ναι"}:
        return True
    if txt in {"0", "false", "no", "n", "off", "οχι", "όχι"}:
        return False
    return None


def _normalize_optional_float(value: object | None, default: float | None = None) -> float | None:
    if value is None:
        return default
    try:
        text = str(value).strip().replace(',', '.')
        if not text:
            return default
        return float(text)
    except Exception:
        return default


def _tenant_sync_url(*, db_name: str, db_user: str, db_password: str) -> str:
    return settings.tenant_database_url_template_sync.format(user=db_user, password=db_password, db_name=db_name)


def _load_tenant_and_connections(
    control_session: Session, tenant_slug: str
) -> tuple[Tenant, TenantConnection | None, TenantConnection | None]:
    tenant = control_session.execute(select(Tenant).where(Tenant.slug == tenant_slug)).scalar_one()
    sql_conn = (
        control_session.execute(
            select(TenantConnection).where(
                TenantConnection.tenant_id == tenant.id,
                TenantConnection.connector_type.in_(("sql_connector", "pharmacyone_sql")),
                TenantConnection.is_active.is_(True),
            )
        )
        .scalars()
        .first()
    )
    api_conn = (
        control_session.execute(
            select(TenantConnection).where(
                TenantConnection.tenant_id == tenant.id,
                TenantConnection.connector_type == 'external_api',
            )
        )
        .scalars()
        .first()
    )
    if sql_conn is None and api_conn is None:
        raise RuntimeError(f"No SQL or external API connector found for tenant {tenant_slug}")
    return tenant, sql_conn, api_conn


def _source_item_sotypes(connection_string: str, *, company: int | None = None) -> list[dict]:
    sql = """
    SELECT
      CAST(ISNULL(M.CODE, M.MTRL) AS nvarchar(128)) AS item_code,
      CAST(ISNULL(M.SODTYPE, 0) AS int) AS softone_sotype,
      CAST(ISNULL(M.NAME, '') AS nvarchar(255)) AS item_name,
      CAST(NULLIF(ISNULL(M.CODE1, ''), '') AS nvarchar(128)) AS barcode,
      CAST(
        NULLIF(
          STUFF(
            (
              SELECT ',' + CAST(MS.CODE AS nvarchar(128))
              FROM MTRSUBSTITUTE MS
              WHERE MS.COMPANY = M.COMPANY
                AND MS.MTRL = M.MTRL
                AND NULLIF(ISNULL(MS.CODE, ''), '') IS NOT NULL
              FOR XML PATH(''), TYPE
            ).value('.', 'nvarchar(max)'),
            1,
            1,
            ''
          ),
          ''
        ) AS nvarchar(1024)
      ) AS alternate_barcodes,
      CAST(NULLIF(CAST(ISNULL(M.MTRMARK, 0) AS nvarchar(64)), '0') AS nvarchar(64)) AS brand_external_id,
      CAST(ISNULL(MK.NAME, '') AS nvarchar(255)) AS brand_name,
      TRY_CAST(M.VAT AS decimal(18,4)) AS vat_rate,
      CAST(ISNULL(VT.NAME, '') AS nvarchar(255)) AS vat_label,
      CAST(ISNULL(C1.NAME, '') AS nvarchar(255)) AS category_1,
      CAST(ISNULL(C2.NAME, '') AS nvarchar(255)) AS category_2,
      CAST(ISNULL(C3.NAME, '') AS nvarchar(255)) AS category_3,
      CAST(ISNULL(CG.NAME, '') AS nvarchar(255)) AS commercial_category,
      CAST(
        COALESCE(
          NULLIF(UT4.NAME, ''),
          NULLIF(UT4.CODE, ''),
          NULLIF(CAST(IX.UTBL04 AS nvarchar(128)), '0'),
          ''
        ) AS nvarchar(128)
      ) AS manual_order_category,
      CAST(
        COALESCE(
          NULLIF(UT5.NAME, ''),
          NULLIF(UT5.CODE, ''),
          NULLIF(CAST(IX.UTBL05 AS nvarchar(128)), '0'),
          ''
        ) AS nvarchar(128)
      ) AS commercial_status,
      COALESCE(TRY_CAST(VMQ.vendor_moq AS decimal(18,4)), CAST(1 AS decimal(18,4))) AS vendor_moq,
      CAST(NULLIF(CAST(ISNULL(M.MTRGROUP, 0) AS nvarchar(64)), '0') AS nvarchar(64)) AS group_ext_id,
      CAST(ISNULL(MG.NAME, '') AS nvarchar(255)) AS group_name,
      CASE
        WHEN COL_LENGTH('MTRL', 'ISACTIVE') IS NOT NULL THEN CAST(ISNULL(M.ISACTIVE, 1) AS int)
        ELSE 1
      END AS is_active_source
    FROM MTRL M
    LEFT JOIN MTRMARK MK
      ON MK.MTRMARK = M.MTRMARK
     AND MK.COMPANY = M.COMPANY
    LEFT JOIN MTRGROUP MG
      ON MG.MTRGROUP = M.MTRGROUP
     AND MG.COMPANY = M.COMPANY
    LEFT JOIN VAT VT
      ON VT.VAT = M.VAT
    LEFT JOIN CCC88POCAT1 C1
      ON C1.CCC88POCAT1 = M.CCC88POCAT1
    LEFT JOIN CCC88POCAT2 C2
      ON C2.CCC88POCAT2 = M.CCC88POCAT2
    LEFT JOIN CCC88POCAT3 C3
      ON C3.CCC88POCAT3 = M.CCC88POCAT3
    LEFT JOIN MTRPCATEGORY CG
      ON CG.MTRPCATEGORY = M.MTRPCATEGORY
     AND CG.COMPANY = M.COMPANY
    LEFT JOIN MTREXTRA IX
      ON IX.MTRL = M.MTRL
     AND IX.COMPANY = M.COMPANY
    OUTER APPLY (
      SELECT MIN(NULLIF(TRY_CAST(MSC.CCC88MOQ AS decimal(18,4)), 0)) AS vendor_moq
      FROM MTRSUPCODE MSC
      WHERE MSC.MTRL = M.MTRL
        AND MSC.COMPANY = M.COMPANY
    ) VMQ
    LEFT JOIN UTBL04 UT4
      ON UT4.UTBL04 = IX.UTBL04
     AND UT4.COMPANY = IX.COMPANY
     AND UT4.SODTYPE = M.SODTYPE
    LEFT JOIN UTBL05 UT5
      ON UT5.UTBL05 = IX.UTBL05
     AND UT5.COMPANY = IX.COMPANY
     AND UT5.SODTYPE = M.SODTYPE
    WHERE (? IS NULL OR M.COMPANY = ?)
      AND ISNULL(M.CODE, '') <> ''
    """
    with _connect(connection_string) as conn:
        cur = conn.cursor()
        cur.execute(sql, company, company)
        rows = cur.fetchall()
        out: list[dict] = []
        for row in rows:
            out.append(
                {
                    "external_id": str(row[0] or "").strip(),
                    "softone_sotype": int(row[1] or 0),
                    "name": str(row[2] or "").strip() or None,
                    "barcode": _normalize_softone_text(row[3]),
                    "alternate_barcodes": _normalize_softone_text(row[4]),
                    "brand_external_id": _normalize_softone_text(row[5]),
                    "brand_name": _normalize_softone_text(row[6]),
                    "vat_rate": float(row[7]) if row[7] is not None else None,
                    "vat_label": _normalize_softone_text(row[8]),
                    "category_1": _normalize_softone_text(row[9]),
                    "category_2": _normalize_softone_text(row[10]),
                    "category_3": _normalize_softone_text(row[11]),
                    "commercial_category": _normalize_softone_text(row[12]),
                    "manual_order_category": _normalize_softone_text(row[13]),
                    "commercial_status": _normalize_softone_text(row[14]),
                    "vendor_moq": _normalize_optional_float(row[15], 1),
                    "group_ext_id": _normalize_softone_text(row[16]),
                    "group_name": _normalize_softone_text(row[17]),
                    "is_active_source": None if row[18] is None else bool(int(row[18])),
                }
            )
        return out


def _source_item_sotypes_via_api(api_connection: TenantConnection, *, tenant_slug: str, company: int | None = None) -> list[dict]:
    params = dict(api_connection.connection_parameters or {})
    base_url = str(params.get("base_url") or "").strip()
    if not base_url:
        raise RuntimeError("external_api connection has no base_url")

    connector = ExternalApiIngestConnector()
    context = ConnectorContext(
        tenant_slug=tenant_slug,
        incremental_column="updated_at",
        id_column="item_code",
        date_column="updated_at",
        branch_column="branch_ext_id",
        item_column="item_code",
        amount_column="value_amount",
        cost_column="cost_amount",
        qty_column="qty",
        source_type="api",
        connection_parameters=params,
        stream_api_endpoint={},
    )
    body: dict[str, object] = {
        "company": company,
        "limit": 20000,
        "debug": False,
    }
    connector._inject_softone_auth(body=body, context=context, payload={})
    response = connector._call_endpoint(
        endpoint=f"{base_url.rstrip('/')}/GetItemMasterForBI",
        context=context,
        body=body,
    )
    records = response.get("records")
    if not isinstance(records, list):
        return []
    out: list[dict] = []
    for record in records:
        if not isinstance(record, dict):
            continue
        out.append(
            {
                "external_id": str(record.get("item_code") or "").strip(),
                "softone_sotype": int(record.get("softone_sotype") or 0),
                "name": _normalize_softone_text(record.get("item_name")),
                "barcode": _normalize_softone_text(record.get("barcode")),
                "alternate_barcodes": _normalize_softone_text(record.get("alternate_barcodes")),
                "brand_external_id": _normalize_softone_text(record.get("brand_external_id")),
                "brand_name": _normalize_softone_text(record.get("brand_name")),
                "category_1": _normalize_softone_text(record.get("category_1")),
                "category_2": _normalize_softone_text(record.get("category_2")),
                "category_3": _normalize_softone_text(record.get("category_3")),
                "commercial_category": _normalize_softone_text(record.get("commercial_category")),
                "manual_order_category": _normalize_softone_text(record.get("manual_order_category")),
                "commercial_status": _normalize_softone_text(record.get("commercial_status")),
                "vendor_moq": _normalize_optional_float(record.get("vendor_moq"), 1),
                "group_ext_id": _normalize_softone_text(record.get("group_ext_id")),
                "group_name": _normalize_softone_text(record.get("group_name")),
                "is_active_source": _normalize_optional_bool(
                    record.get("is_active_source")
                    if record.get("is_active_source") is not None
                    else record.get("is_active")
                ),
            }
        )
    return [row for row in out if row["external_id"]]


def _upsert_dim_items(tenant_session: Session, rows: list[dict]) -> tuple[int, int]:
    inserted_or_updated = 0
    if not rows:
        return 0, 0

    def _batched(seq: list[dict], size: int = _BATCH_SIZE):
        for start in range(0, len(seq), size):
            yield seq[start:start + size]

    def _dedupe(rows_in: list[dict], key: str) -> list[dict]:
        deduped: dict[str, dict] = {}
        for row in rows_in:
            raw_key = str(row.get(key) or "").strip()
            if not raw_key:
                continue
            deduped[raw_key] = row
        return list(deduped.values())

    brand_rows = _dedupe([
        {"external_id": row["brand_external_id"], "name": row.get("brand_name") or row["brand_external_id"]}
        for row in rows
        if row.get("brand_external_id")
    ], "external_id")
    if brand_rows:
        for batch in _batched(brand_rows):
            brand_stmt = insert(DimBrand.__table__).values(batch)
            tenant_session.execute(
                brand_stmt.on_conflict_do_update(
                    index_elements=["external_id"],
                    set_={
                        "name": text("COALESCE(NULLIF(EXCLUDED.name, ''), dim_brands.name)"),
                        "updated_at": text("NOW()"),
                    },
                )
            )

    group_rows = _dedupe([
        {"external_id": row["group_ext_id"], "name": row.get("group_name") or row["group_ext_id"]}
        for row in rows
        if row.get("group_ext_id")
    ], "external_id")
    if group_rows:
        for batch in _batched(group_rows):
            group_stmt = insert(DimGroup.__table__).values(batch)
            tenant_session.execute(
                group_stmt.on_conflict_do_update(
                    index_elements=["external_id"],
                    set_={
                        "name": text("COALESCE(NULLIF(EXCLUDED.name, ''), dim_groups.name)"),
                        "updated_at": text("NOW()"),
                    },
                )
            )

    brand_map = {
        str(ext): brand_id
        for ext, brand_id in tenant_session.execute(select(DimBrand.external_id, DimBrand.id)).all()
        if ext and brand_id
    }
    group_map = {
        str(ext): group_id
        for ext, group_id in tenant_session.execute(select(DimGroup.external_id, DimGroup.id)).all()
        if ext and group_id
    }

    table = DimItem.__table__
    table_columns = {col.name for col in table.columns}
    materialized_rows = []
    for row in rows:
        materialized = dict(row)
        materialized["brand_id"] = brand_map.get(str(row.get("brand_external_id") or ""))
        materialized["group_id"] = group_map.get(str(row.get("group_ext_id") or ""))
        materialized_rows.append({k: v for k, v in materialized.items() if k in table_columns})
    materialized_rows = _dedupe(materialized_rows, "external_id")
    for batch in _batched(materialized_rows):
        stmt = insert(table).values(batch)
        stmt = stmt.on_conflict_do_update(
            index_elements=["external_id"],
            set_={
                "softone_sotype": stmt.excluded.softone_sotype,
                "name": text("COALESCE(NULLIF(EXCLUDED.name, ''), dim_items.name)"),
                "barcode": text("COALESCE(NULLIF(EXCLUDED.barcode, ''), dim_items.barcode)"),
                "alternate_barcodes": text("COALESCE(NULLIF(EXCLUDED.alternate_barcodes, ''), dim_items.alternate_barcodes)"),
                "category_1": text("COALESCE(NULLIF(EXCLUDED.category_1, ''), dim_items.category_1)"),
                "category_2": text("COALESCE(NULLIF(EXCLUDED.category_2, ''), dim_items.category_2)"),
                "category_3": text("COALESCE(NULLIF(EXCLUDED.category_3, ''), dim_items.category_3)"),
                "commercial_category": text("COALESCE(NULLIF(EXCLUDED.commercial_category, ''), dim_items.commercial_category)"),
                "manual_order_category": text("COALESCE(NULLIF(EXCLUDED.manual_order_category, ''), dim_items.manual_order_category)"),
                "commercial_status": text("COALESCE(NULLIF(EXCLUDED.commercial_status, ''), dim_items.commercial_status)"),
                "vendor_moq": text("COALESCE(EXCLUDED.vendor_moq, dim_items.vendor_moq, 1)"),
                "brand_id": text("COALESCE(EXCLUDED.brand_id, dim_items.brand_id)"),
                "group_id": text("COALESCE(EXCLUDED.group_id, dim_items.group_id)"),
                "is_active_source": text("COALESCE(EXCLUDED.is_active_source, dim_items.is_active_source)"),
                "updated_at": text("NOW()"),
            },
        )
        tenant_session.execute(stmt)
    inserted_or_updated = len(rows)

    backfilled_item_ids = tenant_session.execute(
        text(
            """
            UPDATE fact_inventory fi
            SET item_id = di.id
            FROM dim_items di
            WHERE fi.item_id IS NULL
              AND COALESCE(fi.item_code, '') <> ''
              AND di.external_id = fi.item_code
            """
        )
    ).rowcount or 0
    return inserted_or_updated, int(backfilled_item_ids)


def _summarize_tenant_inventory(tenant_session: Session) -> dict[str, int]:
    total_items = tenant_session.execute(text("SELECT COUNT(*) FROM dim_items")).scalar_one()
    sotype_51 = tenant_session.execute(text("SELECT COUNT(*) FROM dim_items WHERE softone_sotype = 51")).scalar_one()
    inventory_distinct = tenant_session.execute(
        text("SELECT COUNT(DISTINCT item_code) FROM fact_inventory WHERE COALESCE(item_code, '') <> ''")
    ).scalar_one()
    inventory_distinct_51 = tenant_session.execute(
        text(
            """
            SELECT COUNT(DISTINCT fi.item_code)
            FROM fact_inventory fi
            JOIN dim_items di ON di.id = fi.item_id
            WHERE COALESCE(fi.item_code, '') <> ''
              AND di.softone_sotype = 51
            """
        )
    ).scalar_one()
    return {
        "dim_items_total": int(total_items or 0),
        "dim_items_sotype_51": int(sotype_51 or 0),
        "inventory_distinct_items": int(inventory_distinct or 0),
        "inventory_distinct_items_sotype_51": int(inventory_distinct_51 or 0),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill SoftOne SOTYPE into tenant dim_items without full resync.")
    parser.add_argument("--tenant", required=True, help="Tenant slug, e.g. pharmacy295")
    parser.add_argument("--company", type=int, default=None, help="Optional SoftOne company filter")
    args = parser.parse_args()

    control_engine = create_engine(settings.control_database_url_sync, future=True)

    with Session(control_engine) as control_session:
        tenant, connection, api_connection = _load_tenant_and_connections(control_session, args.tenant)
        connection_string = None
        if connection is not None:
            secret = decrypt_sqlserver_secret(connection.enc_payload)
            connection_string = build_odbc_connection_string(secret)
        company = args.company
        if company is None:
            raw_company = ((connection.connection_parameters if connection else None) or {}).get("company")
            try:
                company = int(raw_company) if raw_company is not None else None
            except Exception:
                company = None

        source_mode = "sql"
        try:
            if not connection_string:
                raise RuntimeError("No SQL connection string available")
            source_rows = _source_item_sotypes(connection_string, company=company)
        except Exception:
            if api_connection is None:
                raise
            source_rows = _source_item_sotypes_via_api(api_connection, tenant_slug=tenant.slug, company=company)
            source_mode = "external_api"

    tenant_engine = create_engine(
        _tenant_sync_url(db_name=tenant.db_name, db_user=tenant.db_user, db_password=tenant.db_password),
        future=True,
    )
    with Session(tenant_engine) as tenant_session:
        changed, fact_backfill = _upsert_dim_items(tenant_session, source_rows)
        tenant_session.commit()
        summary = _summarize_tenant_inventory(tenant_session)

    print(
        {
            "tenant": tenant.slug,
            "company": company,
            "source_mode": source_mode,
            "source_rows": len(source_rows),
            "dim_items_upserted": changed,
            "fact_inventory_item_id_backfilled": fact_backfill,
            **summary,
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
