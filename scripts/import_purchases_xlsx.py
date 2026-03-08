#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import os
import tempfile
import xml.etree.ElementTree as ET
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path

import psycopg

NS_MAIN = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
CELL_TAG = f"{{{NS_MAIN}}}c"
VALUE_TAG = f"{{{NS_MAIN}}}v"
INLINE_TAG = f"{{{NS_MAIN}}}is"
TEXT_TAG = f"{{{NS_MAIN}}}t"
ROW_TAG = f"{{{NS_MAIN}}}row"
EXCEL_EPOCH = date(1899, 12, 30)


@dataclass
class ImportStats:
    db_name: str
    staged_rows: int
    dedup_docs: int
    inserted_rows: int
    total_fact_rows: int


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Replace/import purchase documents XLSX into tenant fact_purchases as one-line docs."
    )
    p.add_argument("--xlsx", required=True, help="Path to XLSX file")
    p.add_argument(
        "--fallback-doc-date",
        default=date.today().isoformat(),
        help="Fallback document date when row date cannot be parsed (YYYY-MM-DD)",
    )
    p.add_argument(
        "--tenant-db",
        action="append",
        default=[],
        help="Tenant DB name (repeatable). If omitted, imports into all active tenants from control DB.",
    )
    p.add_argument("--limit", type=int, default=0, help="Optional max rows to stage (0 = all)")
    p.add_argument("--replace-all", action="store_true", help="Delete existing purchases and purchase aggregates before import")
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


def parse_doc_date(value: str, fallback: date) -> date:
    raw = clean_text(value)
    if not raw:
        return fallback
    normalized = raw.replace(",", ".")
    try:
        serial = int(float(normalized))
        if 20000 <= serial <= 70000:
            return EXCEL_EPOCH + timedelta(days=serial)
    except ValueError:
        pass

    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%d.%m.%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return fallback


def build_staging_csv(xlsx_path: Path, fallback_doc_date: date, limit: int = 0) -> tuple[Path, int]:
    with tempfile.NamedTemporaryFile(prefix="purchases_import_", suffix=".csv", delete=False, mode="w", encoding="utf-8", newline="") as tmp:
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
                    if pos < 0 or pos >= len(row):
                        continue
                    txt = clean_text(row[pos])
                    if txt:
                        return txt
                return ""

            doc_date = parse_doc_date(val("Ημερομηνία"), fallback_doc_date)
            branch_name = val("Υποκατάστημα")[:255]
            warehouse_name = val("Αποθ. χώρος", "Αποθ.χώρος")[:255]
            series_label = val("Σειρά")[:128]
            document_no = val("Κωδ. παραστ.", "Κωδ.παραστ.")[:128]
            supplier_name = val("Προμηθευτής")[:255]
            reason = val("Αιτιολογία")[:255]
            net_value = parse_decimal(val("Συν.Αξία", "Συν. Αξία")).quantize(Decimal("0.01"))

            if not document_no:
                continue

            writer.writerow(
                [
                    rows_written + 1,
                    doc_date.isoformat(),
                    branch_name,
                    warehouse_name,
                    series_label,
                    document_no,
                    supplier_name,
                    str(net_value),
                    reason,
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
    *,
    db_name: str,
    csv_path: Path,
    staged_rows: int,
    host: str,
    port: int,
    user: str,
    password: str,
    replace_all: bool,
) -> ImportStats:
    with psycopg.connect(conninfo(host, port, user, password, db_name)) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TEMP TABLE stg_purchases_import (
                    row_num integer,
                    doc_date date,
                    branch_name text,
                    warehouse_name text,
                    series_label text,
                    document_no text,
                    supplier_name text,
                    net_value numeric(14,2),
                    reason text
                ) ON COMMIT DROP
                """
            )
            with csv_path.open("r", encoding="utf-8", newline="") as fh:
                with cur.copy(
                    """
                    COPY stg_purchases_import (
                        row_num, doc_date, branch_name, warehouse_name, series_label,
                        document_no, supplier_name, net_value, reason
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
                CREATE TEMP TABLE stg_purchases_dedup ON COMMIT DROP AS
                WITH normalized AS (
                    SELECT
                        row_num,
                        doc_date,
                        NULLIF(BTRIM(branch_name), '') AS branch_name,
                        NULLIF(BTRIM(warehouse_name), '') AS warehouse_name,
                        NULLIF(BTRIM(series_label), '') AS series_label,
                        NULLIF(BTRIM(document_no), '') AS document_no,
                        NULLIF(BTRIM(supplier_name), '') AS supplier_name,
                        COALESCE(net_value, 0)::numeric(14,2) AS net_value,
                        NULLIF(BTRIM(reason), '') AS reason
                    FROM stg_purchases_import
                ),
                dedup AS (
                    SELECT DISTINCT ON (
                        doc_date,
                        document_no,
                        COALESCE(series_label, ''),
                        COALESCE(branch_name, ''),
                        COALESCE(supplier_name, '')
                    )
                        doc_date,
                        branch_name,
                        warehouse_name,
                        series_label,
                        document_no,
                        supplier_name,
                        net_value,
                        reason
                    FROM normalized
                    WHERE doc_date IS NOT NULL
                      AND document_no IS NOT NULL
                    ORDER BY
                        doc_date,
                        document_no,
                        COALESCE(series_label, ''),
                        COALESCE(branch_name, ''),
                        COALESCE(supplier_name, ''),
                        row_num DESC
                )
                SELECT * FROM dedup
                """
            )
            cur.execute("SELECT count(*) FROM stg_purchases_dedup")
            dedup_docs = int(cur.fetchone()[0] or 0)

            if replace_all:
                cur.execute("DELETE FROM agg_purchases_daily")
                cur.execute("DELETE FROM agg_purchases_monthly")
                cur.execute("SELECT to_regclass('agg_purchases_daily_company') IS NOT NULL")
                if bool(cur.fetchone()[0]):
                    cur.execute("DELETE FROM agg_purchases_daily_company")
                cur.execute("SELECT to_regclass('agg_purchases_daily_branch') IS NOT NULL")
                if bool(cur.fetchone()[0]):
                    cur.execute("DELETE FROM agg_purchases_daily_branch")
                cur.execute("DELETE FROM fact_purchases")

            cur.execute(
                """
                INSERT INTO dim_branches (external_id, branch_code, name, branch_name, created_at, updated_at)
                SELECT DISTINCT
                    LEFT('BRX_' || md5(lower(branch_name)), 64) AS external_id,
                    LEFT('BRX_' || md5(lower(branch_name)), 64) AS branch_code,
                    LEFT(branch_name, 255) AS name,
                    LEFT(branch_name, 255) AS branch_name,
                    NOW(), NOW()
                FROM stg_purchases_dedup
                WHERE branch_name IS NOT NULL
                ON CONFLICT (external_id) DO UPDATE SET
                    branch_code = EXCLUDED.branch_code,
                    name = EXCLUDED.name,
                    branch_name = EXCLUDED.branch_name,
                    updated_at = NOW()
                """
            )

            cur.execute(
                """
                INSERT INTO dim_warehouses (external_id, name, created_at, updated_at)
                SELECT DISTINCT
                    LEFT('WHX_' || md5(lower(warehouse_name)), 64) AS external_id,
                    LEFT(warehouse_name, 255) AS name,
                    NOW(), NOW()
                FROM stg_purchases_dedup
                WHERE warehouse_name IS NOT NULL
                ON CONFLICT (external_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    updated_at = NOW()
                """
            )

            cur.execute(
                """
                INSERT INTO dim_suppliers (external_id, name, created_at, updated_at)
                SELECT DISTINCT
                    LEFT('SUPX_' || md5(lower(supplier_name)), 64) AS external_id,
                    LEFT(supplier_name, 255) AS name,
                    NOW(), NOW()
                FROM stg_purchases_dedup
                WHERE supplier_name IS NOT NULL
                ON CONFLICT (external_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    updated_at = NOW()
                """
            )

            cur.execute(
                """
                WITH prepared AS (
                    SELECT
                        d.*,
                        b.id AS branch_id,
                        b.external_id AS branch_ext_id,
                        w.id AS warehouse_id,
                        w.external_id AS warehouse_ext_id,
                        s.id AS supplier_id,
                        s.external_id AS supplier_ext_id
                    FROM stg_purchases_dedup d
                    LEFT JOIN dim_branches b
                      ON b.external_id = LEFT('BRX_' || md5(lower(d.branch_name)), 64)
                    LEFT JOIN dim_warehouses w
                      ON w.external_id = LEFT('WHX_' || md5(lower(d.warehouse_name)), 64)
                    LEFT JOIN dim_suppliers s
                      ON s.external_id = LEFT('SUPX_' || md5(lower(d.supplier_name)), 64)
                )
                INSERT INTO fact_purchases (
                    external_id,
                    event_id,
                    doc_date,
                    branch_id,
                    item_id,
                    supplier_id,
                    warehouse_id,
                    brand_id,
                    category_id,
                    group_id,
                    branch_ext_id,
                    warehouse_ext_id,
                    supplier_ext_id,
                    brand_ext_id,
                    category_ext_id,
                    group_ext_id,
                    item_code,
                    qty,
                    net_value,
                    cost_amount,
                    created_at,
                    updated_at
                )
                SELECT
                    LEFT('PURXLS_' || md5(
                        to_char(p.doc_date, 'YYYY-MM-DD') || '|' ||
                        p.document_no || '|' ||
                        COALESCE(p.series_label, '') || '|' ||
                        COALESCE(p.branch_ext_id, '') || '|' ||
                        COALESCE(p.supplier_ext_id, '')
                    ), 128) AS external_id,
                    LEFT('PURXLS_EVT_' || md5(
                        to_char(p.doc_date, 'YYYY-MM-DD') || '|' ||
                        p.document_no || '|' ||
                        COALESCE(p.series_label, '') || '|' ||
                        COALESCE(p.branch_ext_id, '') || '|' ||
                        COALESCE(p.supplier_ext_id, '')
                    ), 128) AS event_id,
                    p.doc_date,
                    p.branch_id,
                    NULL,
                    p.supplier_id,
                    p.warehouse_id,
                    NULL,
                    NULL,
                    NULL,
                    p.branch_ext_id,
                    p.warehouse_ext_id,
                    p.supplier_ext_id,
                    NULL,
                    NULL,
                    NULL,
                    p.document_no,
                    1::numeric(18,4),
                    COALESCE(p.net_value, 0)::numeric(14,2),
                    COALESCE(p.net_value, 0)::numeric(14,2),
                    NOW(),
                    NOW()
                FROM prepared p
                ON CONFLICT (external_id) DO UPDATE SET
                    event_id = EXCLUDED.event_id,
                    doc_date = EXCLUDED.doc_date,
                    branch_id = EXCLUDED.branch_id,
                    supplier_id = EXCLUDED.supplier_id,
                    warehouse_id = EXCLUDED.warehouse_id,
                    branch_ext_id = EXCLUDED.branch_ext_id,
                    warehouse_ext_id = EXCLUDED.warehouse_ext_id,
                    supplier_ext_id = EXCLUDED.supplier_ext_id,
                    item_code = EXCLUDED.item_code,
                    qty = EXCLUDED.qty,
                    net_value = EXCLUDED.net_value,
                    cost_amount = EXCLUDED.cost_amount,
                    updated_at = NOW()
                """
            )
            inserted_rows = max(cur.rowcount, 0)

            cur.execute("DELETE FROM agg_purchases_daily")
            cur.execute(
                """
                INSERT INTO agg_purchases_daily (
                    doc_date, branch_ext_id, warehouse_ext_id, supplier_ext_id,
                    brand_ext_id, category_ext_id, group_ext_id,
                    qty, net_value, cost_amount, created_at, updated_at
                )
                SELECT
                    doc_date,
                    branch_ext_id,
                    warehouse_ext_id,
                    supplier_ext_id,
                    brand_ext_id,
                    category_ext_id,
                    group_ext_id,
                    COALESCE(SUM(qty), 0),
                    COALESCE(SUM(net_value), 0),
                    COALESCE(SUM(cost_amount), 0),
                    NOW(),
                    NOW()
                FROM fact_purchases
                GROUP BY doc_date, branch_ext_id, warehouse_ext_id, supplier_ext_id, brand_ext_id, category_ext_id, group_ext_id
                """
            )

            cur.execute("DELETE FROM agg_purchases_monthly")
            cur.execute(
                """
                INSERT INTO agg_purchases_monthly (
                    month_start, branch_ext_id, warehouse_ext_id, supplier_ext_id,
                    brand_ext_id, category_ext_id, group_ext_id,
                    qty, net_value, cost_amount, created_at, updated_at
                )
                SELECT
                    date_trunc('month', doc_date)::date,
                    branch_ext_id,
                    warehouse_ext_id,
                    supplier_ext_id,
                    brand_ext_id,
                    category_ext_id,
                    group_ext_id,
                    COALESCE(SUM(qty), 0),
                    COALESCE(SUM(net_value), 0),
                    COALESCE(SUM(cost_amount), 0),
                    NOW(),
                    NOW()
                FROM fact_purchases
                GROUP BY date_trunc('month', doc_date)::date, branch_ext_id, warehouse_ext_id, supplier_ext_id, brand_ext_id, category_ext_id, group_ext_id
                """
            )

            cur.execute("SELECT to_regclass('agg_purchases_daily_company') IS NOT NULL")
            has_purchases_daily_company = bool(cur.fetchone()[0])
            if has_purchases_daily_company:
                cur.execute("DELETE FROM agg_purchases_daily_company")
                cur.execute(
                    """
                    INSERT INTO agg_purchases_daily_company (
                        doc_date, qty, net_value, cost_amount, branches, suppliers, created_at, updated_at
                    )
                    SELECT
                        fp.doc_date,
                        COALESCE(SUM(fp.qty), 0),
                        COALESCE(SUM(fp.net_value), 0),
                        COALESCE(SUM(fp.cost_amount), 0),
                        COUNT(DISTINCT fp.branch_ext_id),
                        COUNT(DISTINCT fp.supplier_ext_id),
                        NOW(),
                        NOW()
                    FROM fact_purchases fp
                    GROUP BY fp.doc_date
                    """
                )

            cur.execute("SELECT to_regclass('agg_purchases_daily_branch') IS NOT NULL")
            has_purchases_daily_branch = bool(cur.fetchone()[0])
            if has_purchases_daily_branch:
                cur.execute("DELETE FROM agg_purchases_daily_branch")
                cur.execute(
                    """
                    WITH branch_rollup AS (
                        SELECT
                            fp.doc_date,
                            fp.branch_ext_id,
                            COALESCE(SUM(fp.qty), 0) AS qty,
                            COALESCE(SUM(fp.net_value), 0) AS net_value,
                            COALESCE(SUM(fp.cost_amount), 0) AS cost_amount,
                            COUNT(DISTINCT fp.supplier_ext_id) AS suppliers
                        FROM fact_purchases fp
                        GROUP BY fp.doc_date, fp.branch_ext_id
                    ),
                    day_rollup AS (
                        SELECT
                            br.doc_date,
                            COALESCE(SUM(br.net_value), 0) AS total_net
                        FROM branch_rollup br
                        GROUP BY br.doc_date
                    )
                    INSERT INTO agg_purchases_daily_branch (
                        doc_date, branch_ext_id, qty, net_value, cost_amount, contribution_pct, suppliers, created_at, updated_at
                    )
                    SELECT
                        br.doc_date,
                        br.branch_ext_id,
                        br.qty,
                        br.net_value,
                        br.cost_amount,
                        CASE WHEN dr.total_net = 0 THEN 0 ELSE br.net_value / dr.total_net * 100 END AS contribution_pct,
                        br.suppliers,
                        NOW(),
                        NOW()
                    FROM branch_rollup br
                    JOIN day_rollup dr ON dr.doc_date = br.doc_date
                    """
                )

            cur.execute("SELECT count(*) FROM fact_purchases")
            total_fact_rows = int(cur.fetchone()[0] or 0)

        conn.commit()

    return ImportStats(
        db_name=db_name,
        staged_rows=staged_rows,
        dedup_docs=dedup_docs,
        inserted_rows=inserted_rows,
        total_fact_rows=total_fact_rows,
    )


def main() -> None:
    args = parse_args()
    xlsx_path = Path(args.xlsx)
    if not xlsx_path.exists():
        raise SystemExit(f"XLSX not found: {xlsx_path}")
    fallback_doc_date = date.fromisoformat(str(args.fallback_doc_date))

    csv_path, staged_rows = build_staging_csv(
        xlsx_path=xlsx_path,
        fallback_doc_date=fallback_doc_date,
        limit=max(0, int(args.limit or 0)),
    )
    print(f"[info] staged_rows={staged_rows} csv={csv_path}")
    if staged_rows == 0:
        print("[warn] No purchase rows found in spreadsheet.")
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
                host=args.db_host,
                port=args.db_port,
                user=args.db_user,
                password=args.db_pass,
                replace_all=bool(args.replace_all),
            )
            stats.append(st)
            print(
                f"[ok] {db_name}: dedup_docs={st.dedup_docs} inserted={st.inserted_rows} "
                f"fact_rows_total={st.total_fact_rows}"
            )
    finally:
        csv_path.unlink(missing_ok=True)

    print("\n=== IMPORT SUMMARY ===")
    for st in stats:
        print(
            f"{st.db_name}: staged={st.staged_rows}, dedup_docs={st.dedup_docs}, "
            f"inserted={st.inserted_rows}, total_fact_rows={st.total_fact_rows}"
        )


if __name__ == "__main__":
    main()
