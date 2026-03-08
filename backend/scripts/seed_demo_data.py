#!/usr/bin/env python3
from __future__ import annotations

import argparse
import math
import random
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

from sqlalchemy import create_engine, text

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
if BACKEND_ROOT is None:
    raise RuntimeError("Could not locate backend root for seed script.")
sys.path.insert(0, str(BACKEND_ROOT))

from app.core.config import settings  # noqa: E402


@dataclass
class TenantRow:
    id: int
    slug: str
    db_name: str


BRANCHES = [("BR1", "Έδρα"), ("BR2", "Κηφισιά"), ("BR3", "Σπάτα")]
WAREHOUSES = [("W1", "Κεντρική"), ("W2", "Δευτερεύουσα")]
BRANDS = [
    ("BRN1", "Frezyderm"),
    ("BRN2", "La Roche-Posay"),
    ("BRN3", "Vichy"),
    ("BRN4", "Korres"),
    ("BRN5", "Apivita"),
    ("BRN6", "Lanes"),
    ("BRN7", "Solgar"),
]
CATEGORIES = [
    ("CAT_L1_PHARMA", "Φάρμακα", None, 1),
    ("CAT_L2_OTC", "Μη Συνταγογραφούμενα Φάρμακα", "CAT_L1_PHARMA", 2),
    ("CAT_L3_COLD_FLU", "Κρυολόγημα & Γρίπη", "CAT_L2_OTC", 3),
    ("CAT_L3_ENT_NASAL", "Ρινικά", "CAT_L2_OTC", 3),
    ("CAT_L3_ENT_EYE", "Οφθαλμολογικά", "CAT_L2_OTC", 3),
    ("CAT_L1_PARAPH", "Παραφάρμακα", None, 1),
    ("CAT_L2_DERMO_FACE", "Καλλυντικά Προσώπου", "CAT_L1_PARAPH", 2),
    ("CAT_L3_ANTI_AGE", "Αντιγήρανση Προσώπου", "CAT_L2_DERMO_FACE", 3),
    ("CAT_L3_HYDRATION", "Ενυδάτωση Προσώπου", "CAT_L2_DERMO_FACE", 3),
    ("CAT_L2_DERMO_SUN", "Αντηλιακή Προστασία", "CAT_L1_PARAPH", 2),
    ("CAT_L3_FACE_SPF", "Αντηλιακά Προσώπου", "CAT_L2_DERMO_SUN", 3),
    ("CAT_L3_AFTER_SUN", "After Sun", "CAT_L2_DERMO_SUN", 3),
    ("CAT_L1_SUPP", "Συμπληρώματα Διατροφής", None, 1),
    ("CAT_L2_VIT_MIN", "Βιταμίνες & Μέταλλα", "CAT_L1_SUPP", 2),
    ("CAT_L3_VIT_C", "Βιταμίνη C", "CAT_L2_VIT_MIN", 3),
    ("CAT_L3_VIT_D", "Βιταμίνη D", "CAT_L2_VIT_MIN", 3),
    ("CAT_L3_MAGNESIUM", "Μαγνήσιο", "CAT_L2_VIT_MIN", 3),
    ("CAT_L2_IMMUNE_GUT", "Ανοσοποιητικό & Πεπτικό", "CAT_L1_SUPP", 2),
    ("CAT_L3_PROBIOTIC", "Προβιοτικά", "CAT_L2_IMMUNE_GUT", 3),
    ("CAT_L1_CONSUMABLES", "Αναλώσιμα", None, 1),
    ("CAT_L2_WOUND_CARE", "Φροντίδα Τραύματος", "CAT_L1_CONSUMABLES", 2),
    ("CAT_L3_GAUZE", "Γάζες & Επίδεσμοι", "CAT_L2_WOUND_CARE", 3),
    ("CAT_L3_PATCHES", "Τσιρότα", "CAT_L2_WOUND_CARE", 3),
    ("CAT_L2_HOMECARE", "Οικιακή Φροντίδα", "CAT_L1_CONSUMABLES", 2),
    ("CAT_L3_DIAGNOSTIC", "Διαγνωστικά", "CAT_L2_HOMECARE", 3),
]
GROUPS = [
    ("GRP_PHARMA", "Φάρμακα"),
    ("GRP_OTC", "Μη Συνταγογραφούμενα Φάρμακα"),
    ("GRP_PARAPH", "Παραφάρμακα"),
    ("GRP_CONS", "Αναλώσιμα"),
]
SUPPLIERS = [
    ("SUP_FREZY", "Frezyderm ABEE"),
    ("SUP_LOREAL", "L'Oreal Hellas (La Roche-Posay / Vichy)"),
    ("SUP_KORRES", "KORRES SA"),
    ("SUP_APIVITA", "APIVITA SA"),
    ("SUP_LANES", "Lanes Health"),
    ("SUP_SOLGAR", "Solgar Hellas"),
]
SUPPLIER_BY_BRAND = {
    "BRN1": "SUP_FREZY",   # Frezyderm
    "BRN2": "SUP_LOREAL",  # La Roche-Posay
    "BRN3": "SUP_LOREAL",  # Vichy
    "BRN4": "SUP_KORRES",  # Korres
    "BRN5": "SUP_APIVITA", # Apivita
    "BRN6": "SUP_LANES",   # Lanes
    "BRN7": "SUP_SOLGAR",  # Solgar
}

ITEM_TEMPLATES = [
    ("Frezyderm Anticort Cream", "BRN1", "CAT_L3_HYDRATION", "GRP_PARAPH"),
    ("Frezyderm Sea Side Dry Mist SPF 50+", "BRN1", "CAT_L3_FACE_SPF", "GRP_PARAPH"),
    ("La Roche-Posay Cicaplast Baume B5+", "BRN2", "CAT_L3_HYDRATION", "GRP_PARAPH"),
    ("La Roche-Posay Anthelios UVMune 400 SPF 50+", "BRN2", "CAT_L3_FACE_SPF", "GRP_PARAPH"),
    ("Vichy Minéral 89 Booster", "BRN3", "CAT_L3_ANTI_AGE", "GRP_PARAPH"),
    ("Vichy Capital Soleil UV-Age Daily SPF 50+", "BRN3", "CAT_L3_FACE_SPF", "GRP_PARAPH"),
    ("Korres Greek Yoghurt Foaming Cream Cleanser", "BRN4", "CAT_L3_HYDRATION", "GRP_PARAPH"),
    ("Korres Wild Rose Vitamin C Serum", "BRN4", "CAT_L3_ANTI_AGE", "GRP_PARAPH"),
    ("Apivita Bee Sun Safe SPF 50", "BRN5", "CAT_L3_FACE_SPF", "GRP_PARAPH"),
    ("Apivita Hand Cream", "BRN5", "CAT_L3_HYDRATION", "GRP_PARAPH"),
    ("Lanes Vitamin C 1000mg", "BRN6", "CAT_L3_VIT_C", "GRP_OTC"),
    ("Lanes Propolcare Pastilles", "BRN6", "CAT_L3_COLD_FLU", "GRP_OTC"),
    ("Solgar Vitamin D3 1000 IU", "BRN7", "CAT_L3_VIT_D", "GRP_OTC"),
    ("Solgar Omega-3 950mg", "BRN7", "CAT_L3_VIT_D", "GRP_OTC"),
    ("Magnesium Supplement", "BRN7", "CAT_L3_MAGNESIUM", "GRP_OTC"),
    ("Probiotic Daily", "BRN6", "CAT_L3_PROBIOTIC", "GRP_OTC"),
    ("Nasal Spray", "BRN3", "CAT_L3_ENT_NASAL", "GRP_OTC"),
    ("Eye Drops", "BRN3", "CAT_L3_ENT_EYE", "GRP_OTC"),
    ("Γάζες Αποστειρωμένες", "BRN4", "CAT_L3_GAUZE", "GRP_CONS"),
    ("Τσιρότα Ευαίσθητης Επιδερμίδας", "BRN4", "CAT_L3_PATCHES", "GRP_CONS"),
    ("Thermometer Digital", "BRN5", "CAT_L3_DIAGNOSTIC", "GRP_CONS"),
]


def _ean13_check_digit(base12: str) -> str:
    digits = [int(x) for x in base12]
    odd = sum(digits[::2])
    even = sum(digits[1::2])
    check = (10 - ((odd + 3 * even) % 10)) % 10
    return str(check)


def _make_barcode(index: int) -> str:
    base12 = f"520{(100_000_000 + index):09d}"
    return f"{base12}{_ean13_check_digit(base12)}"


def build_items(item_count: int):
    items = []
    manufacturers_by_item: dict[str, str] = {}
    count = max(6, min(item_count, 200))
    for idx in range(count):
        i = idx + 1
        item_ext = f"ITM{i}"
        barcode = _make_barcode(i)
        base_name, brand_ext, cat_ext, grp_ext = ITEM_TEMPLATES[idx % len(ITEM_TEMPLATES)]
        name = f"{base_name} {((idx // len(ITEM_TEMPLATES)) + 1)}"
        unit_price = round(4.5 + ((idx % 11) * 0.95) + (idx * 0.07), 2)
        manufacturer = SUPPLIER_BY_BRAND.get(brand_ext, SUPPLIERS[idx % len(SUPPLIERS)][0])
        items.append((item_ext, barcode, name, brand_ext, cat_ext, grp_ext, unit_price))
        manufacturers_by_item[item_ext] = manufacturer
    return items, manufacturers_by_item


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Seed demo data for tenant DBs (2024-2026).")
    p.add_argument("--tenant-slug", default="", help="Seed only this tenant slug (default: all tenants).")
    p.add_argument("--years", default="2024,2025,2026", help="Comma-separated years.")
    p.add_argument("--seed", type=int, default=4242, help="Random seed.")
    p.add_argument("--item-count", type=int, default=40, help="How many demo items to generate (min 6, max 200).")
    return p.parse_args()


def control_engine():
    return create_engine(settings.control_database_url_sync, future=True)


def tenant_engine(db_name: str):
    url = settings.tenant_database_url_template_sync.format(
        user=settings.tenant_db_superuser,
        password=settings.tenant_db_superpass,
        db_name=db_name,
    )
    return create_engine(url, future=True)


def load_tenants(slug: str) -> list[TenantRow]:
    q = "SELECT id, slug, db_name FROM tenants"
    params = {}
    if slug:
        q += " WHERE slug = :slug"
        params["slug"] = slug
    q += " ORDER BY id"
    with control_engine().connect() as conn:
        rows = conn.execute(text(q), params).mappings().all()
    return [TenantRow(id=r["id"], slug=r["slug"], db_name=r["db_name"]) for r in rows]


def upsert_dimensions(conn, items):
    for ext, name in BRANCHES:
        conn.execute(
            text(
                """
                INSERT INTO dim_branches (external_id, branch_code, name, branch_name, created_at, updated_at)
                VALUES (:e, :e, :n, :n, now(), now())
                ON CONFLICT (external_id) DO UPDATE
                SET branch_code = EXCLUDED.branch_code,
                    name = EXCLUDED.name,
                    branch_name = EXCLUDED.branch_name,
                    updated_at = now()
                """
            ),
            {"e": ext, "n": name},
        )
    for ext, name in WAREHOUSES:
        conn.execute(
            text(
                """
                INSERT INTO dim_warehouses (external_id, name, created_at, updated_at)
                VALUES (:e, :n, now(), now())
                ON CONFLICT (external_id) DO UPDATE
                SET name = EXCLUDED.name, updated_at = now()
                """
            ),
            {"e": ext, "n": name},
        )
    for ext, name in BRANDS:
        conn.execute(
            text(
                """
                INSERT INTO dim_brands (external_id, name, created_at, updated_at)
                VALUES (:e, :n, now(), now())
                ON CONFLICT (external_id) DO UPDATE
                SET name = EXCLUDED.name, updated_at = now()
                """
            ),
            {"e": ext, "n": name},
        )
    for ext, name, parent_ext, level in CATEGORIES:
        conn.execute(
            text(
                """
                INSERT INTO dim_categories (external_id, name, parent_id, level, created_at, updated_at)
                VALUES (
                    :e,
                    :n,
                    CASE
                      WHEN :p IS NULL THEN NULL
                      ELSE (SELECT id FROM dim_categories WHERE external_id = :p)
                    END,
                    :lvl,
                    now(),
                    now()
                )
                ON CONFLICT (external_id) DO UPDATE
                SET name = EXCLUDED.name,
                    parent_id = EXCLUDED.parent_id,
                    level = EXCLUDED.level,
                    updated_at = now()
                """
            ),
            {"e": ext, "n": name, "p": parent_ext, "lvl": level},
        )
    for ext, name in GROUPS:
        conn.execute(
            text(
                """
                INSERT INTO dim_groups (external_id, name, created_at, updated_at)
                VALUES (:e, :n, now(), now())
                ON CONFLICT (external_id) DO UPDATE
                SET name = EXCLUDED.name, updated_at = now()
                """
            ),
            {"e": ext, "n": name},
        )
    for ext, name in SUPPLIERS:
        conn.execute(
            text(
                """
                INSERT INTO dim_suppliers (external_id, name, created_at, updated_at)
                VALUES (:e, :n, now(), now())
                ON CONFLICT (external_id) DO UPDATE
                SET name = EXCLUDED.name, updated_at = now()
                """
            ),
            {"e": ext, "n": name},
        )
    for ext, sku, name, brand, category, group_, _price in items:
        conn.execute(
            text(
                """
                INSERT INTO dim_items (external_id, sku, name, brand_id, category_id, group_id, created_at, updated_at)
                VALUES (
                    :e,
                    :s,
                    :n,
                    (SELECT id FROM dim_brands WHERE external_id = :b),
                    (SELECT id FROM dim_categories WHERE external_id = :c),
                    (SELECT id FROM dim_groups WHERE external_id = :g),
                    now(),
                    now()
                )
                ON CONFLICT (external_id) DO UPDATE
                SET sku = EXCLUDED.sku,
                    name = EXCLUDED.name,
                    brand_id = EXCLUDED.brand_id,
                    category_id = EXCLUDED.category_id,
                    group_id = EXCLUDED.group_id,
                    updated_at = now()
                """
            ),
            {"e": ext, "s": sku, "n": name, "b": brand, "c": category, "g": group_},
        )


def each_day(years: list[int]):
    start = date(min(years), 1, 1)
    end = date(max(years), 12, 31)
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def rebuild_aggregates(conn):
    def table_exists(table_name: str) -> bool:
        return conn.execute(text("SELECT to_regclass(:table_name)"), {"table_name": table_name}).scalar() is not None

    conn.execute(text("DELETE FROM agg_sales_daily"))
    conn.execute(
        text(
            """
            INSERT INTO agg_sales_daily (
                doc_date, branch_ext_id, warehouse_ext_id, brand_ext_id, category_ext_id, group_ext_id,
                qty, net_value, gross_value, created_at, updated_at
            )
            SELECT
                doc_date, branch_ext_id, warehouse_ext_id, brand_ext_id, category_ext_id, group_ext_id,
                COALESCE(SUM(qty),0), COALESCE(SUM(net_value),0), COALESCE(SUM(gross_value),0), now(), now()
            FROM fact_sales
            GROUP BY doc_date, branch_ext_id, warehouse_ext_id, brand_ext_id, category_ext_id, group_ext_id
            """
        )
    )

    conn.execute(text("DELETE FROM agg_sales_monthly"))
    conn.execute(
        text(
            """
            INSERT INTO agg_sales_monthly (
                month_start, branch_ext_id, warehouse_ext_id, brand_ext_id, category_ext_id, group_ext_id,
                qty, net_value, gross_value, created_at, updated_at
            )
            SELECT
                date_trunc('month', doc_date)::date, branch_ext_id, warehouse_ext_id, brand_ext_id, category_ext_id, group_ext_id,
                COALESCE(SUM(qty),0), COALESCE(SUM(net_value),0), COALESCE(SUM(gross_value),0), now(), now()
            FROM fact_sales
            GROUP BY date_trunc('month', doc_date)::date, branch_ext_id, warehouse_ext_id, brand_ext_id, category_ext_id, group_ext_id
            """
        )
    )

    conn.execute(text("DELETE FROM agg_sales_item_daily"))
    conn.execute(
        text(
            """
            INSERT INTO agg_sales_item_daily (
                doc_date, item_external_id, qty, net_value, cost_amount, created_at, updated_at
            )
            SELECT
                doc_date, item_code, COALESCE(SUM(qty),0), COALESCE(SUM(net_value),0), COALESCE(SUM(cost_amount),0), now(), now()
            FROM fact_sales
            WHERE item_code IS NOT NULL
            GROUP BY doc_date, item_code
            """
        )
    )

    conn.execute(text("DELETE FROM agg_purchases_daily"))
    conn.execute(
        text(
            """
            INSERT INTO agg_purchases_daily (
                doc_date, branch_ext_id, warehouse_ext_id, supplier_ext_id, brand_ext_id, category_ext_id, group_ext_id,
                qty, net_value, cost_amount, created_at, updated_at
            )
            SELECT
                doc_date, branch_ext_id, warehouse_ext_id, supplier_ext_id, brand_ext_id, category_ext_id, group_ext_id,
                COALESCE(SUM(qty),0), COALESCE(SUM(net_value),0), COALESCE(SUM(cost_amount),0), now(), now()
            FROM fact_purchases
            GROUP BY doc_date, branch_ext_id, warehouse_ext_id, supplier_ext_id, brand_ext_id, category_ext_id, group_ext_id
            """
        )
    )

    conn.execute(text("DELETE FROM agg_purchases_monthly"))
    conn.execute(
        text(
            """
            INSERT INTO agg_purchases_monthly (
                month_start, branch_ext_id, warehouse_ext_id, supplier_ext_id, brand_ext_id, category_ext_id, group_ext_id,
                qty, net_value, cost_amount, created_at, updated_at
            )
            SELECT
                date_trunc('month', doc_date)::date, branch_ext_id, warehouse_ext_id, supplier_ext_id, brand_ext_id, category_ext_id, group_ext_id,
                COALESCE(SUM(qty),0), COALESCE(SUM(net_value),0), COALESCE(SUM(cost_amount),0), now(), now()
            FROM fact_purchases
            GROUP BY date_trunc('month', doc_date)::date, branch_ext_id, warehouse_ext_id, supplier_ext_id, brand_ext_id, category_ext_id, group_ext_id
            """
        )
    )

    if table_exists("agg_sales_daily_company"):
        conn.execute(text("DELETE FROM agg_sales_daily_company"))
        conn.execute(
            text(
                """
                INSERT INTO agg_sales_daily_company (
                    doc_date, qty, net_value, gross_value, cost_amount, branches, margin_pct, created_at, updated_at
                )
                SELECT
                    fs.doc_date,
                    COALESCE(SUM(fs.qty), 0),
                    COALESCE(SUM(fs.net_value), 0),
                    COALESCE(SUM(fs.gross_value), 0),
                    COALESCE(SUM(fs.cost_amount), 0),
                    COUNT(DISTINCT fs.branch_ext_id),
                    CASE
                        WHEN COALESCE(SUM(fs.net_value), 0) = 0 THEN 0
                        ELSE (COALESCE(SUM(fs.net_value), 0) - COALESCE(SUM(fs.cost_amount), 0)) / COALESCE(SUM(fs.net_value), 0) * 100
                    END,
                    NOW(),
                    NOW()
                FROM fact_sales fs
                GROUP BY fs.doc_date
                """
            )
        )

    if table_exists("agg_sales_daily_branch"):
        conn.execute(text("DELETE FROM agg_sales_daily_branch"))
        conn.execute(
            text(
                """
                WITH branch_rollup AS (
                    SELECT
                        fs.doc_date,
                        fs.branch_ext_id,
                        COALESCE(SUM(fs.qty), 0) AS qty,
                        COALESCE(SUM(fs.net_value), 0) AS net_value,
                        COALESCE(SUM(fs.gross_value), 0) AS gross_value,
                        COALESCE(SUM(fs.cost_amount), 0) AS cost_amount
                    FROM fact_sales fs
                    GROUP BY fs.doc_date, fs.branch_ext_id
                ),
                day_rollup AS (
                    SELECT
                        br.doc_date,
                        COALESCE(SUM(br.net_value), 0) AS total_net
                    FROM branch_rollup br
                    GROUP BY br.doc_date
                )
                INSERT INTO agg_sales_daily_branch (
                    doc_date, branch_ext_id, qty, net_value, gross_value, cost_amount, contribution_pct, margin_pct, created_at, updated_at
                )
                SELECT
                    br.doc_date,
                    br.branch_ext_id,
                    br.qty,
                    br.net_value,
                    br.gross_value,
                    br.cost_amount,
                    CASE WHEN dr.total_net = 0 THEN 0 ELSE br.net_value / dr.total_net * 100 END AS contribution_pct,
                    CASE WHEN br.net_value = 0 THEN 0 ELSE (br.net_value - br.cost_amount) / br.net_value * 100 END AS margin_pct,
                    NOW(),
                    NOW()
                FROM branch_rollup br
                JOIN day_rollup dr ON dr.doc_date = br.doc_date
                """
            )
        )

    if table_exists("agg_purchases_daily_company"):
        conn.execute(text("DELETE FROM agg_purchases_daily_company"))
        conn.execute(
            text(
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
        )

    if table_exists("agg_purchases_daily_branch"):
        conn.execute(text("DELETE FROM agg_purchases_daily_branch"))
        conn.execute(
            text(
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
        )

    conn.execute(text("DELETE FROM agg_inventory_snapshot_daily"))
    conn.execute(
        text(
            """
            INSERT INTO agg_inventory_snapshot_daily (
                snapshot_date, item_external_id, qty_on_hand, value_amount, created_at, updated_at
            )
            SELECT
                fi.doc_date,
                di.external_id,
                COALESCE(SUM(fi.qty_on_hand), 0),
                COALESCE(SUM(fi.value_amount), 0),
                NOW(),
                NOW()
            FROM fact_inventory fi
            JOIN dim_items di ON di.id = fi.item_id
            GROUP BY fi.doc_date, di.external_id
            """
        )
    )


def seed_tenant(tenant: TenantRow, years: list[int], seed: int, item_count: int):
    rng = random.Random(seed + tenant.id)
    items, manufacturers_by_item = build_items(item_count)
    engine = tenant_engine(tenant.db_name)
    inserted_sales = 0
    inserted_purchases = 0
    inserted_inventory = 0
    inserted_cashflows = 0
    with engine.begin() as conn:
        upsert_dimensions(conn, items)

        branch_ids = dict(conn.execute(text("SELECT external_id, id FROM dim_branches")).all())
        warehouse_ids = dict(conn.execute(text("SELECT external_id, id FROM dim_warehouses")).all())
        item_rows = conn.execute(text("SELECT external_id, id FROM dim_items")).all()
        item_ids = {str(ext): item_id for ext, item_id in item_rows}

        conn.execute(text("DELETE FROM fact_sales WHERE external_id LIKE 'DEMO_%'"))
        conn.execute(text("DELETE FROM fact_purchases WHERE external_id LIKE 'DEMO_%'"))
        conn.execute(text("DELETE FROM fact_inventory WHERE external_id LIKE 'DEMO_%'"))
        conn.execute(text("DELETE FROM fact_cashflows WHERE external_id LIKE 'DEMO_%'"))

        sales_rows = []
        purchases_rows = []
        inventory_rows = []
        cashflow_rows = []

        for d in each_day(years):
            day_of_year = d.timetuple().tm_yday
            season = 1.0 + 0.18 * math.sin((day_of_year / 365.0) * 2.0 * math.pi)
            trend = 1.0 + (d.year - min(years)) * 0.07
            weekend = 1.10 if d.weekday() >= 5 else 1.0

            for b_idx, (branch_ext, _b_name) in enumerate(BRANCHES):
                wh_ext = WAREHOUSES[b_idx % len(WAREHOUSES)][0]
                for i_idx, (item_ext, _sku, _name, brand_ext, cat_ext, grp_ext, unit_price) in enumerate(items):
                    base = 1.2 + (b_idx * 0.25) + (i_idx * 0.1)
                    qty = max(0.2, (base + rng.random()) * season * trend * weekend)
                    qty = round(qty, 3)
                    net = round(qty * unit_price * (0.92 + rng.random() * 0.16), 2)
                    cost = round(net * (0.58 + rng.random() * 0.18), 2)
                    gross = round(net * 1.24, 2)
                    profit = round(net - cost, 2)
                    ext_id = f"DEMO_S_{d.isoformat()}_{branch_ext}_{item_ext}"
                    sales_rows.append(
                        {
                            "external_id": ext_id,
                            "event_id": f"{ext_id}_EV",
                            "doc_date": d,
                            "branch_ext_id": branch_ext,
                            "warehouse_ext_id": wh_ext,
                            "brand_ext_id": brand_ext,
                            "category_ext_id": cat_ext,
                            "group_ext_id": grp_ext,
                            "item_code": item_ext,
                            "qty": qty,
                            "net_value": net,
                            "gross_value": gross,
                            "cost_amount": cost,
                            "profit_amount": profit,
                        }
                    )

                if d.day % 2 == 0:
                    for i_idx, (item_ext, _sku, _name, brand_ext, cat_ext, grp_ext, unit_price) in enumerate(items):
                        sup_ext = manufacturers_by_item.get(item_ext, SUPPLIERS[i_idx % len(SUPPLIERS)][0])
                        qty = round((2.0 + (i_idx % 4) + rng.random() * 2.5) * trend, 3)
                        cost = round(qty * unit_price * (0.70 + rng.random() * 0.25), 2)
                        net = round(cost * (1.02 + rng.random() * 0.03), 2)
                        ext_id = f"DEMO_P_{d.isoformat()}_{branch_ext}_{sup_ext}_{item_ext}"
                        purchases_rows.append(
                            {
                                "external_id": ext_id,
                                "event_id": f"{ext_id}_EV",
                                "doc_date": d,
                                "branch_ext_id": branch_ext,
                                "warehouse_ext_id": wh_ext,
                                "supplier_ext_id": sup_ext,
                                "brand_ext_id": brand_ext,
                                "category_ext_id": cat_ext,
                                "group_ext_id": grp_ext,
                                "item_code": item_ext,
                                "qty": qty,
                                "net_value": net,
                                "cost_amount": cost,
                            }
                        )

                for item_ext, _sku, _name, _brand_ext, _cat_ext, _grp_ext, unit_price in items:
                    item_id = item_ids.get(item_ext)
                    if item_id is None:
                        continue
                    qty_on_hand = round((18.0 + rng.random() * 42.0) * season * (0.9 + b_idx * 0.08), 3)
                    qty_reserved = round(max(0.0, qty_on_hand * (0.05 + rng.random() * 0.12)), 3)
                    unit_cost = unit_price * (0.55 + rng.random() * 0.22)
                    stock_cost = round(qty_on_hand * unit_cost, 2)
                    stock_value = round(stock_cost * (1.08 + rng.random() * 0.16), 2)
                    inv_ext_id = f"DEMO_I_{d.isoformat()}_{branch_ext}_{item_ext}"
                    inventory_rows.append(
                        {
                            "external_id": inv_ext_id,
                            "doc_date": d,
                            "branch_id": branch_ids.get(branch_ext),
                            "warehouse_id": warehouse_ids.get(wh_ext),
                            "item_id": item_id,
                            "qty_on_hand": qty_on_hand,
                            "qty_reserved": qty_reserved,
                            "cost_amount": stock_cost,
                            "value_amount": stock_value,
                        }
                    )

                base_sales = sum(row["net_value"] for row in sales_rows[-len(items) :])
                in_amount = round(base_sales * (0.82 + rng.random() * 0.18), 2)
                out_amount = round(in_amount * (0.64 + rng.random() * 0.24), 2)
                refund_amount = round(base_sales * (0.01 + rng.random() * 0.025), 2)
                cashflow_rows.extend(
                    [
                        {
                            "external_id": f"DEMO_C_{d.isoformat()}_{branch_ext}_IN",
                            "doc_date": d,
                            "branch_id": branch_ids.get(branch_ext),
                            "entry_type": "inflow",
                            "amount": in_amount,
                            "currency": "EUR",
                            "reference_no": f"IN-{branch_ext}-{d.strftime('%Y%m%d')}",
                            "notes": "Daily inflow",
                        },
                        {
                            "external_id": f"DEMO_C_{d.isoformat()}_{branch_ext}_OUT",
                            "doc_date": d,
                            "branch_id": branch_ids.get(branch_ext),
                            "entry_type": "outflow",
                            "amount": out_amount,
                            "currency": "EUR",
                            "reference_no": f"OUT-{branch_ext}-{d.strftime('%Y%m%d')}",
                            "notes": "Operational expenses",
                        },
                        {
                            "external_id": f"DEMO_C_{d.isoformat()}_{branch_ext}_REF",
                            "doc_date": d,
                            "branch_id": branch_ids.get(branch_ext),
                            "entry_type": "refund",
                            "amount": refund_amount,
                            "currency": "EUR",
                            "reference_no": f"REF-{branch_ext}-{d.strftime('%Y%m%d')}",
                            "notes": "Refunds",
                        },
                    ]
                )

        conn.execute(
            text(
                """
                INSERT INTO fact_sales (
                    external_id, event_id, doc_date, branch_ext_id, warehouse_ext_id, brand_ext_id, category_ext_id, group_ext_id,
                    item_code, qty, net_value, gross_value, cost_amount, profit_amount, updated_at, created_at
                ) VALUES (
                    :external_id, :event_id, :doc_date, :branch_ext_id, :warehouse_ext_id, :brand_ext_id, :category_ext_id, :group_ext_id,
                    :item_code, :qty, :net_value, :gross_value, :cost_amount, :profit_amount, now(), now()
                )
                """
            ),
            sales_rows,
        )
        inserted_sales = len(sales_rows)

        conn.execute(
            text(
                """
                INSERT INTO fact_purchases (
                    external_id, event_id, doc_date, branch_ext_id, warehouse_ext_id, supplier_ext_id, brand_ext_id, category_ext_id, group_ext_id,
                    item_code, qty, net_value, cost_amount, updated_at, created_at
                ) VALUES (
                    :external_id, :event_id, :doc_date, :branch_ext_id, :warehouse_ext_id, :supplier_ext_id, :brand_ext_id, :category_ext_id, :group_ext_id,
                    :item_code, :qty, :net_value, :cost_amount, now(), now()
                )
                """
            ),
            purchases_rows,
        )
        inserted_purchases = len(purchases_rows)

        conn.execute(
            text(
                """
                INSERT INTO fact_inventory (
                    external_id, doc_date, branch_id, item_id, warehouse_id,
                    qty_on_hand, qty_reserved, cost_amount, value_amount, updated_at, created_at
                ) VALUES (
                    :external_id, :doc_date, :branch_id, :item_id, :warehouse_id,
                    :qty_on_hand, :qty_reserved, :cost_amount, :value_amount, now(), now()
                )
                """
            ),
            inventory_rows,
        )
        inserted_inventory = len(inventory_rows)

        conn.execute(
            text(
                """
                INSERT INTO fact_cashflows (
                    external_id, doc_date, branch_id, entry_type, amount, currency, reference_no, notes, updated_at, created_at
                ) VALUES (
                    :external_id, :doc_date, :branch_id, :entry_type, :amount, :currency, :reference_no, :notes, now(), now()
                )
                """
            ),
            cashflow_rows,
        )
        inserted_cashflows = len(cashflow_rows)

        rebuild_aggregates(conn)

    print(
        f"[seed-demo] tenant={tenant.slug} db={tenant.db_name} items={len(items)} sales_rows={inserted_sales} purchases_rows={inserted_purchases} inventory_rows={inserted_inventory} cashflow_rows={inserted_cashflows}"
    )


def main():
    args = parse_args()
    years = sorted(int(x.strip()) for x in args.years.split(",") if x.strip())
    tenants = load_tenants(args.tenant_slug.strip())
    if not tenants:
        print("[seed-demo] no tenants found")
        return 1
    for tenant in tenants:
        seed_tenant(tenant, years=years, seed=args.seed, item_count=args.item_count)
    print(f"[seed-demo] done for years={years} tenants={len(tenants)} item_count={max(6, min(args.item_count, 200))}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
