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
    inserted_or_updated: int
    skipped_existing: int
    imported_customers: int


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Import customers XLSX into tenant fact_sales as customer_master_import rows.")
    p.add_argument("--xlsx", required=True, help="Path to XLSX file")
    p.add_argument("--doc-date", default="2020-01-01", help="Synthetic doc_date for imported customer rows (YYYY-MM-DD)")
    p.add_argument("--tenant-db", action="append", default=[], help="Tenant DB name (repeatable). If omitted, imports into all active tenants from control DB.")
    p.add_argument("--limit", type=int, default=0, help="Optional max customer rows to stage (0 = all)")
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
    with tempfile.NamedTemporaryFile(prefix="customers_import_", suffix=".csv", delete=False, mode="w", encoding="utf-8", newline="") as tmp:
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
            def val(col: str) -> str:
                pos = index.get(col, -1)
                if pos < 0 or pos >= len(row):
                    return ""
                return clean_text(row[pos])

            code = _safe_slice(val("Κωδικός"), 128)
            name = _safe_slice(val("Επωνυμία"), 255)
            afm = _safe_slice(val("ΑΦΜ"), 64)
            amka = _safe_slice(val("Α.Μ.Κ.Α"), 64)
            address = val("Διεύθυνση")
            city = _safe_slice(val("Πόλη"), 128)
            phone1 = _safe_slice(val("Τηλέφωνο 1"), 64)
            phone2 = _safe_slice(val("Τηλέφωνο 2"), 64)
            profession = _safe_slice(val("Επάγγελμα"), 128)
            balance = parse_decimal(val("Υπόλοιπο"))
            turnover = parse_decimal(val("Τζίρος"))

            if not code and not name:
                continue
            writer.writerow(
                [
                    rows_written + 1,
                    code,
                    name,
                    afm,
                    amka,
                    address,
                    city,
                    phone1,
                    phone2,
                    profession,
                    str(balance.quantize(Decimal("0.01"))),
                    str(turnover.quantize(Decimal("0.01"))),
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
    source_file: str,
) -> ImportStats:
    with psycopg.connect(conninfo(host, port, user, password, db_name)) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TEMP TABLE stg_customers_import (
                    row_num integer,
                    customer_code text,
                    customer_name text,
                    afm text,
                    amka text,
                    address text,
                    city text,
                    phone_1 text,
                    phone_2 text,
                    profession text,
                    balance numeric(14,2),
                    turnover numeric(14,2)
                ) ON COMMIT DROP
                """
            )
            with csv_path.open("r", encoding="utf-8", newline="") as fh:
                with cur.copy(
                    """
                    COPY stg_customers_import (
                        row_num, customer_code, customer_name, afm, amka, address, city,
                        phone_1, phone_2, profession, balance, turnover
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
                WITH normalized AS (
                    SELECT
                        row_num,
                        NULLIF(BTRIM(customer_code), '') AS customer_code,
                        NULLIF(BTRIM(customer_name), '') AS customer_name,
                        NULLIF(BTRIM(afm), '') AS afm,
                        NULLIF(BTRIM(amka), '') AS amka,
                        NULLIF(BTRIM(address), '') AS address,
                        NULLIF(BTRIM(city), '') AS city,
                        NULLIF(BTRIM(phone_1), '') AS phone_1,
                        NULLIF(BTRIM(phone_2), '') AS phone_2,
                        NULLIF(BTRIM(profession), '') AS profession,
                        COALESCE(balance, 0)::numeric(14,2) AS balance,
                        COALESCE(turnover, 0)::numeric(14,2) AS turnover
                    FROM stg_customers_import
                ),
                keyed AS (
                    SELECT
                        row_num,
                        COALESCE(customer_code, customer_name, 'ROW_' || row_num::text) AS customer_key,
                        customer_code,
                        customer_name,
                        afm,
                        amka,
                        address,
                        city,
                        phone_1,
                        phone_2,
                        profession,
                        balance,
                        turnover
                    FROM normalized
                ),
                dedup AS (
                    SELECT DISTINCT ON (customer_key)
                        customer_key,
                        customer_code,
                        customer_name,
                        afm,
                        amka,
                        address,
                        city,
                        phone_1,
                        phone_2,
                        profession,
                        balance,
                        turnover
                    FROM keyed
                    ORDER BY customer_key, row_num DESC
                )
                SELECT count(*)
                FROM dedup d
                WHERE EXISTS (
                    SELECT 1
                    FROM fact_sales fs
                    WHERE COALESCE(NULLIF(BTRIM(fs.customer_code), ''), NULLIF(BTRIM(fs.customer_name), ''), '') = d.customer_key
                      AND COALESCE(fs.document_type, '') <> 'customer_master_import'
                )
                """
            )
            skipped_existing = int(cur.fetchone()[0] or 0)

            cur.execute(
                """
                WITH normalized AS (
                    SELECT
                        row_num,
                        NULLIF(BTRIM(customer_code), '') AS customer_code,
                        NULLIF(BTRIM(customer_name), '') AS customer_name,
                        NULLIF(BTRIM(afm), '') AS afm,
                        NULLIF(BTRIM(amka), '') AS amka,
                        NULLIF(BTRIM(address), '') AS address,
                        NULLIF(BTRIM(city), '') AS city,
                        NULLIF(BTRIM(phone_1), '') AS phone_1,
                        NULLIF(BTRIM(phone_2), '') AS phone_2,
                        NULLIF(BTRIM(profession), '') AS profession,
                        COALESCE(balance, 0)::numeric(14,2) AS balance,
                        COALESCE(turnover, 0)::numeric(14,2) AS turnover
                    FROM stg_customers_import
                ),
                keyed AS (
                    SELECT
                        row_num,
                        COALESCE(customer_code, customer_name, 'ROW_' || row_num::text) AS customer_key,
                        customer_code,
                        customer_name,
                        afm,
                        amka,
                        address,
                        city,
                        phone_1,
                        phone_2,
                        profession,
                        balance,
                        turnover
                    FROM normalized
                ),
                dedup AS (
                    SELECT DISTINCT ON (customer_key)
                        customer_key,
                        customer_code,
                        customer_name,
                        afm,
                        amka,
                        address,
                        city,
                        phone_1,
                        phone_2,
                        profession,
                        balance,
                        turnover
                    FROM keyed
                    ORDER BY customer_key, row_num DESC
                ),
                eligible AS (
                    SELECT d.*
                    FROM dedup d
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM fact_sales fs
                        WHERE COALESCE(NULLIF(BTRIM(fs.customer_code), ''), NULLIF(BTRIM(fs.customer_name), ''), '') = d.customer_key
                          AND COALESCE(fs.document_type, '') <> 'customer_master_import'
                    )
                )
                INSERT INTO fact_sales (
                    external_id,
                    event_id,
                    doc_date,
                    document_id,
                    document_no,
                    document_series,
                    document_type,
                    document_status,
                    customer_code,
                    customer_name,
                    delivery_address,
                    delivery_city,
                    notes,
                    qty,
                    net_value,
                    gross_value,
                    cost_amount,
                    profit_amount,
                    source_updated_at,
                    source_payload_json,
                    created_at,
                    updated_at
                )
                SELECT
                    'CUSTXLS_' || md5(e.customer_key),
                    'CUSTXLS_EVT_' || md5(e.customer_key),
                    %s::date,
                    LEFT('CUSTXLS-' || COALESCE(e.customer_code, e.customer_key), 128),
                    LEFT('CUSTXLS-' || COALESCE(e.customer_code, e.customer_key), 128),
                    'Customer Import XLSX',
                    'customer_master_import',
                    'Εισαγωγή πελατών',
                    LEFT(COALESCE(e.customer_code, e.customer_key), 128),
                    LEFT(COALESCE(e.customer_name, e.customer_code, 'ΠΕΛΑΤΗΣ'), 255),
                    e.address,
                    LEFT(COALESCE(e.city, ''), 128),
                    %s,
                    0,
                    COALESCE(e.turnover, 0),
                    COALESCE(e.turnover, 0),
                    0,
                    COALESCE(e.turnover, 0),
                    NOW(),
                    jsonb_build_object(
                        'import_source', %s::text,
                        'customer_afm', COALESCE(e.afm, ''),
                        'customer_amka', COALESCE(e.amka, ''),
                        'customer_profession', COALESCE(e.profession, ''),
                        'customer_phone', COALESCE(e.phone_1, ''),
                        'customer_phone2', COALESCE(e.phone_2, ''),
                        'customer_address', COALESCE(e.address, ''),
                        'customer_city', COALESCE(e.city, ''),
                        'customer_balance', COALESCE(e.balance, 0),
                        'is_active', 'true'
                    ),
                    NOW(),
                    NOW()
                FROM eligible e
                ON CONFLICT (external_id) DO UPDATE SET
                    customer_code = EXCLUDED.customer_code,
                    customer_name = EXCLUDED.customer_name,
                    delivery_address = EXCLUDED.delivery_address,
                    delivery_city = EXCLUDED.delivery_city,
                    notes = EXCLUDED.notes,
                    net_value = EXCLUDED.net_value,
                    gross_value = EXCLUDED.gross_value,
                    profit_amount = EXCLUDED.profit_amount,
                    source_updated_at = NOW(),
                    source_payload_json = EXCLUDED.source_payload_json,
                    updated_at = NOW()
                """,
                (doc_date.isoformat(), f"Imported from {source_file}", source_file),
            )
            inserted_or_updated = max(cur.rowcount, 0)

            cur.execute(
                """
                SELECT count(*)
                FROM fact_sales
                WHERE COALESCE(document_type, '') = 'customer_master_import'
                """
            )
            imported_customers = int(cur.fetchone()[0] or 0)

        conn.commit()

    return ImportStats(
        db_name=db_name,
        staged_rows=staged_rows,
        inserted_or_updated=inserted_or_updated,
        skipped_existing=skipped_existing,
        imported_customers=imported_customers,
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
        print("[warn] No data rows found in spreadsheet.")
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
                source_file=xlsx_path.name,
            )
            stats.append(st)
            print(
                f"[ok] {db_name}: affected={st.inserted_or_updated} skipped_existing={st.skipped_existing} "
                f"customer_master_import_rows={st.imported_customers}"
            )
    finally:
        csv_path.unlink(missing_ok=True)

    print("\n=== IMPORT SUMMARY ===")
    for st in stats:
        print(
            f"{st.db_name}: staged={st.staged_rows}, affected={st.inserted_or_updated}, "
            f"skipped_existing={st.skipped_existing}, customer_master_import_rows={st.imported_customers}"
        )


if __name__ == "__main__":
    main()
