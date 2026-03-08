#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import os
import tempfile
import xml.etree.ElementTree as ET
import zipfile
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path

import psycopg


NS_MAIN = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
CELL_TAG = f"{{{NS_MAIN}}}c"
VALUE_TAG = f"{{{NS_MAIN}}}v"
INLINE_TAG = f"{{{NS_MAIN}}}is"
TEXT_TAG = f"{{{NS_MAIN}}}t"
ROW_TAG = f"{{{NS_MAIN}}}row"


@dataclass
class ImportStats:
    db_name: str
    staged_rows: int
    items_affected: int
    inventory_affected: int
    total_items: int
    snapshot_rows: int


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Import items XLSX into tenant dimensions (dim_items) and inventory snapshot (fact_inventory)."
    )
    p.add_argument("--xlsx", required=True, help="Path to XLSX file")
    p.add_argument("--doc-date", default=date.today().isoformat(), help="Inventory snapshot date (YYYY-MM-DD)")
    p.add_argument(
        "--tenant-db",
        action="append",
        default=[],
        help="Tenant DB name (repeatable). If omitted, imports into all active tenants from control DB.",
    )
    p.add_argument("--limit", type=int, default=0, help="Optional max item rows to stage (0 = all)")
    p.add_argument("--db-host", default=os.getenv("TENANT_DB_HOST", "postgres"))
    p.add_argument("--db-port", type=int, default=int(os.getenv("TENANT_DB_PORT", "5432")))
    p.add_argument("--db-user", default=os.getenv("TENANT_DB_SUPERUSER", "postgres"))
    p.add_argument("--db-pass", default=os.getenv("TENANT_DB_SUPERPASS", "postgres"))
    p.add_argument("--control-db", default=os.getenv("CONTROL_DB_NAME", "bi_control"))
    return p.parse_args()


def _col_to_index(cell_ref: str) -> int:
    col = "".join(ch for ch in cell_ref if ch.isalpha())
    idx = 0
    for ch in col:
        idx = idx * 26 + (ord(ch.upper()) - 64)
    return max(idx - 1, 0)


def _read_shared_strings(zf: zipfile.ZipFile) -> list[str]:
    if "xl/sharedStrings.xml" not in zf.namelist():
        return []
    root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
    out: list[str] = []
    for si in root.findall(f".//{{{NS_MAIN}}}si"):
        txt = "".join((node.text or "") for node in si.findall(f".//{{{NS_MAIN}}}t"))
        out.append(txt)
    return out


def _cell_value(cell: ET.Element, shared_strings: list[str]) -> str:
    cell_type = cell.attrib.get("t")
    if cell_type == "s":
        v = cell.find(VALUE_TAG)
        if v is None or v.text is None:
            return ""
        idx = int(v.text)
        if idx < 0 or idx >= len(shared_strings):
            return ""
        return shared_strings[idx]
    if cell_type == "inlineStr":
        i = cell.find(INLINE_TAG)
        if i is None:
            return ""
        return "".join((t.text or "") for t in i.findall(f".//{TEXT_TAG}"))
    v = cell.find(VALUE_TAG)
    return (v.text or "") if v is not None else ""


def iter_xlsx_rows(xlsx_path: Path):
    with zipfile.ZipFile(xlsx_path) as zf:
        shared_strings = _read_shared_strings(zf)
        with zf.open("xl/worksheets/sheet1.xml") as fh:
            context = ET.iterparse(fh, events=("end",))
            for _, elem in context:
                if elem.tag != ROW_TAG:
                    continue
                values: dict[int, str] = {}
                max_idx = -1
                for cell in elem.findall(CELL_TAG):
                    ref = cell.attrib.get("r", "")
                    idx = _col_to_index(ref) if ref else (max_idx + 1)
                    max_idx = max(max_idx, idx)
                    values[idx] = _cell_value(cell, shared_strings)
                if values:
                    row = [""] * (max_idx + 1)
                    for idx, val in values.items():
                        row[idx] = val
                    yield row
                elem.clear()


def clean_text(value: str) -> str:
    return " ".join(str(value or "").replace("\xa0", " ").replace("\t", " ").strip().split())


def parse_decimal(value: str) -> Decimal:
    raw = clean_text(value)
    if not raw:
        return Decimal("0")
    txt = raw.replace(" ", "")
    if "," in txt and "." in txt:
        if txt.rfind(",") > txt.rfind("."):
            txt = txt.replace(".", "").replace(",", ".")
        else:
            txt = txt.replace(",", "")
    elif "," in txt:
        txt = txt.replace(".", "").replace(",", ".")
    try:
        return Decimal(txt)
    except InvalidOperation:
        return Decimal("0")


def _safe_slice(value: str, max_len: int) -> str:
    return value[:max_len] if len(value) > max_len else value


def build_staging_csv(xlsx_path: Path, limit: int = 0) -> tuple[Path, int]:
    with tempfile.NamedTemporaryFile(prefix="items_import_", suffix=".csv", delete=False, mode="w", encoding="utf-8", newline="") as tmp:
        writer = csv.writer(tmp)
        out_path = Path(tmp.name)
        rows_written = 0
        header: list[str] | None = None
        index: dict[str, int] = {}
        for row_idx, row in enumerate(iter_xlsx_rows(xlsx_path), start=1):
            if row_idx == 1:
                header = [clean_text(v) for v in row]
                index = {name: i for i, name in enumerate(header)}
                continue
            if header is None:
                continue

            def val(*cols: str) -> str:
                for col in cols:
                    pos = index.get(col, -1)
                    if 0 <= pos < len(row):
                        txt = clean_text(row[pos])
                        if txt:
                            return txt
                return ""

            item_code = _safe_slice(val("Κωδικός"), 128)
            item_name = _safe_slice(val("Περιγραφή"), 255)
            image_url = _safe_slice(val("Εικόνα"), 1024)
            main_unit = _safe_slice(val("Κύρια Μ.Μ.", "Κύρια Μ.Μ"), 64)
            group_name = _safe_slice(val("Ομάδα είδους"), 255)
            brand_name = _safe_slice(val("Brand"), 255)
            category_1 = _safe_slice(val("Κατηγορία 1"), 255)
            category_2 = _safe_slice(val("Κατηγορία 2"), 255)
            category_3 = _safe_slice(val("Κατηγορία 3"), 255)
            eof_code = _safe_slice(val("Κωδικός Ε.Ο.Φ."), 128)
            wholesale_price = parse_decimal(val("Τιμή χονδρικής"))
            retail_price = parse_decimal(val("Τιμή λιανικής"))
            stock_qty = parse_decimal(val("Υπόλοιπο είδους"))

            if not item_code:
                continue
            if not item_name:
                item_name = item_code

            writer.writerow(
                [
                    rows_written + 1,
                    item_code,
                    item_name,
                    image_url,
                    main_unit,
                    group_name,
                    brand_name,
                    category_1,
                    category_2,
                    category_3,
                    eof_code,
                    str(wholesale_price.quantize(Decimal("0.0001"))),
                    str(retail_price.quantize(Decimal("0.0001"))),
                    str(stock_qty.quantize(Decimal("0.0001"))),
                ]
            )
            rows_written += 1
            if limit > 0 and rows_written >= limit:
                break
    return out_path, rows_written


def conninfo(host: str, port: int, user: str, password: str, dbname: str) -> str:
    return f"host={host} port={port} user={user} password={password} dbname={dbname}"


def fetch_active_tenant_dbs(conn: psycopg.Connection) -> list[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT db_name FROM tenants WHERE status = 'active' ORDER BY id")
        return [str(r[0]) for r in cur.fetchall() if r and r[0]]


def import_into_tenant(
    db_name: str,
    csv_path: Path,
    staged_rows: int,
    doc_date: date,
    host: str,
    port: int,
    user: str,
    password: str,
) -> ImportStats:
    with psycopg.connect(conninfo(host, port, user, password, db_name)) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TEMP TABLE stg_items_import (
                    row_num integer,
                    item_code text,
                    item_name text,
                    image_url text,
                    main_unit text,
                    group_name text,
                    brand_name text,
                    category_1 text,
                    category_2 text,
                    category_3 text,
                    eof_code text,
                    wholesale_price numeric(14,4),
                    retail_price numeric(14,4),
                    stock_qty numeric(18,4)
                ) ON COMMIT DROP
                """
            )
            with csv_path.open("r", encoding="utf-8", newline="") as fh:
                with cur.copy(
                    """
                    COPY stg_items_import (
                        row_num, item_code, item_name, image_url, main_unit, group_name, brand_name,
                        category_1, category_2, category_3, eof_code, wholesale_price, retail_price, stock_qty
                    ) FROM STDIN WITH (FORMAT CSV)
                    """
                ) as cp:
                    while True:
                        chunk = fh.read(1024 * 1024)
                        if not chunk:
                            break
                        cp.write(chunk)

            cur.execute(
                """
                CREATE TEMP TABLE stg_items_dedup ON COMMIT DROP AS
                WITH normalized AS (
                    SELECT
                        row_num,
                        NULLIF(BTRIM(item_code), '') AS item_code,
                        NULLIF(BTRIM(item_name), '') AS item_name,
                        NULLIF(BTRIM(image_url), '') AS image_url,
                        NULLIF(BTRIM(main_unit), '') AS main_unit,
                        NULLIF(BTRIM(group_name), '') AS group_name,
                        NULLIF(BTRIM(brand_name), '') AS brand_name,
                        NULLIF(BTRIM(category_1), '') AS category_1,
                        NULLIF(BTRIM(category_2), '') AS category_2,
                        NULLIF(BTRIM(category_3), '') AS category_3,
                        NULLIF(BTRIM(eof_code), '') AS eof_code,
                        COALESCE(wholesale_price, 0)::numeric(14,4) AS wholesale_price,
                        COALESCE(retail_price, 0)::numeric(14,4) AS retail_price,
                        COALESCE(stock_qty, 0)::numeric(18,4) AS stock_qty
                    FROM stg_items_import
                ),
                dedup AS (
                    SELECT DISTINCT ON (item_code)
                        item_code,
                        item_name,
                        image_url,
                        main_unit,
                        group_name,
                        brand_name,
                        category_1,
                        category_2,
                        category_3,
                        eof_code,
                        wholesale_price,
                        retail_price,
                        stock_qty
                    FROM normalized
                    WHERE item_code IS NOT NULL
                    ORDER BY item_code, row_num DESC
                )
                SELECT *
                FROM dedup
                """
            )

            cur.execute(
                """
                INSERT INTO dim_brands (external_id, name, created_at, updated_at)
                SELECT DISTINCT
                    LEFT('BRX_' || md5(lower(brand_name)), 64) AS external_id,
                    LEFT(brand_name, 255) AS name,
                    NOW(),
                    NOW()
                FROM stg_items_dedup
                WHERE brand_name IS NOT NULL
                ON CONFLICT (external_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    updated_at = NOW()
                """
            )

            cur.execute(
                """
                INSERT INTO dim_categories (external_id, name, parent_id, level, created_at, updated_at)
                SELECT DISTINCT
                    LEFT('CATX_' || md5(lower(category_1)), 64) AS external_id,
                    LEFT(category_1, 255) AS name,
                    NULL::uuid,
                    1,
                    NOW(),
                    NOW()
                FROM stg_items_dedup
                WHERE category_1 IS NOT NULL
                ON CONFLICT (external_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    updated_at = NOW()
                """
            )

            cur.execute(
                """
                INSERT INTO dim_groups (external_id, name, created_at, updated_at)
                SELECT DISTINCT
                    LEFT('GRPX_' || md5(lower(group_name)), 64) AS external_id,
                    LEFT(group_name, 255) AS name,
                    NOW(),
                    NOW()
                FROM stg_items_dedup
                WHERE group_name IS NOT NULL
                ON CONFLICT (external_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    updated_at = NOW()
                """
            )

            cur.execute(
                """
                INSERT INTO dim_items (
                    external_id,
                    sku,
                    barcode,
                    name,
                    main_unit,
                    commercial_category,
                    category_1,
                    category_2,
                    category_3,
                    unit2,
                    purchase_unit,
                    sales_unit,
                    rel_2_to_1,
                    rel_purchase_to_1,
                    rel_sale_to_1,
                    image_url,
                    discount_pct,
                    is_active_source,
                    brand_id,
                    category_id,
                    group_id,
                    created_at,
                    updated_at
                )
                SELECT
                    d.item_code,
                    LEFT('SKU-' || d.item_code, 128),
                    LEFT(COALESCE(d.eof_code, d.item_code), 128),
                    LEFT(COALESCE(d.item_name, d.item_code), 255),
                    LEFT(COALESCE(d.main_unit, 'Τεμ.'), 64),
                    LEFT(COALESCE(d.category_1, ''), 255),
                    LEFT(COALESCE(d.category_1, ''), 255),
                    LEFT(COALESCE(d.category_2, ''), 255),
                    LEFT(COALESCE(d.category_3, ''), 255),
                    LEFT(COALESCE(d.main_unit, 'Τεμ.'), 64),
                    LEFT(COALESCE(d.main_unit, 'Τεμ.'), 64),
                    LEFT(COALESCE(d.main_unit, 'Τεμ.'), 64),
                    1::numeric(18,6),
                    1::numeric(18,6),
                    1::numeric(18,6),
                    LEFT(COALESCE(d.image_url, ''), 1024),
                    0::numeric(6,2),
                    TRUE,
                    b.id,
                    c.id,
                    g.id,
                    NOW(),
                    NOW()
                FROM stg_items_dedup d
                LEFT JOIN dim_brands b
                    ON b.external_id = LEFT('BRX_' || md5(lower(d.brand_name)), 64)
                LEFT JOIN dim_categories c
                    ON c.external_id = LEFT('CATX_' || md5(lower(d.category_1)), 64)
                LEFT JOIN dim_groups g
                    ON g.external_id = LEFT('GRPX_' || md5(lower(d.group_name)), 64)
                ON CONFLICT (external_id) DO UPDATE SET
                    sku = EXCLUDED.sku,
                    barcode = EXCLUDED.barcode,
                    name = EXCLUDED.name,
                    main_unit = EXCLUDED.main_unit,
                    commercial_category = EXCLUDED.commercial_category,
                    category_1 = EXCLUDED.category_1,
                    category_2 = EXCLUDED.category_2,
                    category_3 = EXCLUDED.category_3,
                    unit2 = EXCLUDED.unit2,
                    purchase_unit = EXCLUDED.purchase_unit,
                    sales_unit = EXCLUDED.sales_unit,
                    rel_2_to_1 = EXCLUDED.rel_2_to_1,
                    rel_purchase_to_1 = EXCLUDED.rel_purchase_to_1,
                    rel_sale_to_1 = EXCLUDED.rel_sale_to_1,
                    image_url = EXCLUDED.image_url,
                    discount_pct = EXCLUDED.discount_pct,
                    is_active_source = EXCLUDED.is_active_source,
                    brand_id = EXCLUDED.brand_id,
                    category_id = EXCLUDED.category_id,
                    group_id = EXCLUDED.group_id,
                    updated_at = NOW()
                """
            )
            items_affected = max(cur.rowcount, 0)

            cur.execute(
                """
                INSERT INTO dim_branches (external_id, branch_code, name, branch_name, created_at, updated_at)
                SELECT 'BR_IMPORT', 'BR_IMPORT', 'Εισαγωγή XLSX', 'Εισαγωγή XLSX', NOW(), NOW()
                WHERE NOT EXISTS (SELECT 1 FROM dim_branches)
                ON CONFLICT (external_id) DO NOTHING
                """
            )
            cur.execute(
                """
                INSERT INTO dim_warehouses (external_id, name, created_at, updated_at)
                SELECT 'WH_IMPORT', 'Αποθήκη Εισαγωγής', NOW(), NOW()
                WHERE NOT EXISTS (SELECT 1 FROM dim_warehouses)
                ON CONFLICT (external_id) DO NOTHING
                """
            )

            cur.execute(
                """
                WITH branch_choice AS (
                    SELECT id
                    FROM dim_branches
                    ORDER BY created_at ASC, external_id ASC
                    LIMIT 1
                ),
                warehouse_choice AS (
                    SELECT id
                    FROM dim_warehouses
                    ORDER BY created_at ASC, external_id ASC
                    LIMIT 1
                )
                INSERT INTO fact_inventory (
                    external_id,
                    doc_date,
                    branch_id,
                    item_id,
                    warehouse_id,
                    qty_on_hand,
                    qty_reserved,
                    cost_amount,
                    value_amount,
                    created_at,
                    updated_at
                )
                SELECT
                    LEFT('INVXLS_' || d.item_code || '_' || to_char(%s::date, 'YYYYMMDD'), 128),
                    %s::date,
                    (SELECT id FROM branch_choice),
                    di.id,
                    (SELECT id FROM warehouse_choice),
                    COALESCE(d.stock_qty, 0),
                    0::numeric(18,4),
                    (COALESCE(d.wholesale_price, 0) * COALESCE(d.stock_qty, 0))::numeric(14,2),
                    (COALESCE(NULLIF(d.retail_price, 0), d.wholesale_price, 0) * COALESCE(d.stock_qty, 0))::numeric(14,2),
                    NOW(),
                    NOW()
                FROM stg_items_dedup d
                JOIN dim_items di ON di.external_id = d.item_code
                ON CONFLICT (external_id) DO UPDATE SET
                    doc_date = EXCLUDED.doc_date,
                    branch_id = EXCLUDED.branch_id,
                    item_id = EXCLUDED.item_id,
                    warehouse_id = EXCLUDED.warehouse_id,
                    qty_on_hand = EXCLUDED.qty_on_hand,
                    qty_reserved = EXCLUDED.qty_reserved,
                    cost_amount = EXCLUDED.cost_amount,
                    value_amount = EXCLUDED.value_amount,
                    updated_at = NOW()
                """,
                (doc_date.isoformat(), doc_date.isoformat()),
            )
            inventory_affected = max(cur.rowcount, 0)

            cur.execute("SELECT count(*) FROM dim_items")
            total_items = int(cur.fetchone()[0] or 0)
            cur.execute("SELECT count(*) FROM fact_inventory WHERE doc_date = %s::date", (doc_date.isoformat(),))
            snapshot_rows = int(cur.fetchone()[0] or 0)

        conn.commit()

    return ImportStats(
        db_name=db_name,
        staged_rows=staged_rows,
        items_affected=items_affected,
        inventory_affected=inventory_affected,
        total_items=total_items,
        snapshot_rows=snapshot_rows,
    )


def main() -> None:
    args = parse_args()
    xlsx_path = Path(args.xlsx)
    if not xlsx_path.exists():
        raise SystemExit(f"XLSX not found: {xlsx_path}")
    doc_date = date.fromisoformat(str(args.doc_date))

    csv_path, staged_rows = build_staging_csv(xlsx_path=xlsx_path, limit=max(0, int(args.limit or 0)))
    print(f"[info] staged_rows={staged_rows} csv={csv_path}")
    if staged_rows == 0:
        print("[warn] No item rows found in spreadsheet.")
        csv_path.unlink(missing_ok=True)
        return

    tenant_dbs = [str(x).strip() for x in (args.tenant_db or []) if str(x).strip()]
    if not tenant_dbs:
        with psycopg.connect(
            conninfo(
                host=args.db_host,
                port=args.db_port,
                user=args.db_user,
                password=args.db_pass,
                dbname=args.control_db,
            )
        ) as control_conn:
            tenant_dbs = fetch_active_tenant_dbs(control_conn)

    if not tenant_dbs:
        print("[warn] No tenant DBs found.")
        csv_path.unlink(missing_ok=True)
        return

    print(f"[info] target_tenant_dbs={tenant_dbs}")
    stats: list[ImportStats] = []
    try:
        for db_name in tenant_dbs:
            print(f"[info] importing -> {db_name}")
            st = import_into_tenant(
                db_name=db_name,
                csv_path=csv_path,
                staged_rows=staged_rows,
                doc_date=doc_date,
                host=args.db_host,
                port=args.db_port,
                user=args.db_user,
                password=args.db_pass,
            )
            stats.append(st)
            print(
                f"[ok] {db_name}: items_affected={st.items_affected} inventory_affected={st.inventory_affected} "
                f"total_items={st.total_items} snapshot_rows={st.snapshot_rows}"
            )
    finally:
        csv_path.unlink(missing_ok=True)

    print("\n=== IMPORT SUMMARY ===")
    for st in stats:
        print(
            f"{st.db_name}: staged={st.staged_rows}, items_affected={st.items_affected}, "
            f"inventory_affected={st.inventory_affected}, total_items={st.total_items}, "
            f"snapshot_rows={st.snapshot_rows}"
        )


if __name__ == "__main__":
    main()
