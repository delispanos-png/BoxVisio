from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
import math
import re
from types import SimpleNamespace
import xml.etree.ElementTree as ET
import zipfile

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.tenant import (
    ReplenishmentDataQualityIssue,
    ReplenishmentLine,
    ReplenishmentSnapshot,
)


XLSX_NS = {'a': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}
STORE_CODES = ('KAS', 'AGD', 'PER', 'ELL', 'SPA', 'LOGICA')
STORE_NAMES = {
    'KAS': 'Κηφισιά Κασσαβέτη',
    'AGD': 'Άγιος Δημήτριος',
    'PER': 'Περιστέρι',
    'ELL': 'Ελληνικό',
    'SPA': 'Σπάτα',
    'LOGICA': 'Logica',
}
FIELD_COLUMNS = {
    'item_code': 'A',
    'item_name': 'B',
    'category_1': 'C',
    'category_2': 'D',
    'category_3': 'E',
    'status_1': 'F',
    'status_2': 'G',
    'min_stock': 'H',
    'repl_moq': 'I',
    'vendor_moq': 'J',
    'supplier_order_qty': 'BG',
    'weeks_of_stock_total': 'BH',
    'purchase_price': 'BI',
    'supplier_order_value': 'BJ',
}
STORE_COLUMN_GROUPS = {
    'sales_avg_1': ('K', 'L', 'M', 'N', 'O', 'P'),
    'sales_avg_2': ('Q', 'R', 'S', 'T', 'U', 'V'),
    'stock_qty': ('W', 'X', 'Y', 'Z', 'AA', 'AB'),
    'stock_weeks': ('AC', 'AD', 'AE', 'AF', 'AG', 'AH'),
    'expected_qty': ('AI', 'AJ', 'AK', 'AL', 'AM', 'AN'),
    'target_stock': ('AO', 'AP', 'AQ', 'AR', 'AS', 'AT'),
    'need_qty': ('AU', 'AV', 'AW', 'AX', 'AY', 'AZ'),
    'overstock_qty': ('BA', 'BB', 'BC', 'BD', 'BE', 'BF'),
}
NUMERIC_FIELDS = {
    'min_stock',
    'repl_moq',
    'vendor_moq',
    'supplier_order_qty',
    'weeks_of_stock_total',
    'purchase_price',
    'supplier_order_value',
}
DATA_START_ROW = 15
DEFAULT_ZERO_SALES_HIGH_STOCK_THRESHOLD = 20.0


def _as_float(value: object, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    if math.isnan(number) or math.isinf(number):
        return default
    return number


@dataclass(frozen=True)
class ParsedCell:
    value: str = ''
    formula: str = ''
    cell_type: str = ''


def _column_index(cell_ref: str) -> int:
    letters = re.sub(r'\d+', '', cell_ref or '').upper()
    value = 0
    for char in letters:
        value = value * 26 + (ord(char) - 64)
    return max(value - 1, 0)


def _column_letter_to_index(column: str) -> int:
    value = 0
    for char in column.upper():
        value = value * 26 + (ord(char) - 64)
    return value - 1


def _read_shared_strings(archive: zipfile.ZipFile) -> list[str]:
    if 'xl/sharedStrings.xml' not in archive.namelist():
        return []
    root = ET.fromstring(archive.read('xl/sharedStrings.xml'))
    return [''.join(t.text or '' for t in item.findall('.//a:t', XLSX_NS)) for item in root.findall('a:si', XLSX_NS)]


def _cell_value(cell: ET.Element, shared_strings: list[str]) -> str:
    cell_type = cell.attrib.get('t', '')
    if cell_type == 'inlineStr':
        return ''.join(t.text or '' for t in cell.findall('.//a:t', XLSX_NS)).strip()
    value = cell.find('a:v', XLSX_NS)
    if value is None or value.text is None:
        return ''
    raw = value.text.strip()
    if cell_type == 's' and raw.isdigit():
        idx = int(raw)
        return shared_strings[idx] if idx < len(shared_strings) else raw
    return raw


def _cell_formula(cell: ET.Element) -> str:
    formula = cell.find('a:f', XLSX_NS)
    return formula.text.strip() if formula is not None and formula.text else ''


def _clean_code(value: object) -> str:
    text = str(value or '').strip()
    if re.fullmatch(r'-?\d+\.0+', text):
        return text.split('.', 1)[0]
    return text


def _to_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        if math.isnan(float(value)) or math.isinf(float(value)):
            return None
        return float(value)
    text = str(value).strip()
    if not text or text.startswith('#'):
        return None
    text = text.replace('€', '').replace('%', '').replace('\xa0', '').strip()
    if ',' in text and '.' in text:
        text = text.replace('.', '').replace(',', '.')
    else:
        text = text.replace(',', '.')
    try:
        number = float(text)
    except ValueError:
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def _safe_float(value: object) -> float:
    return _to_float(value) or 0.0


def _cell_ref(column: str, row_no: int) -> str:
    return f'{column}{row_no}'


def _field_cell(row: dict[str, ParsedCell], column: str) -> ParsedCell:
    return row.get(column, ParsedCell())


def _field_text(row: dict[str, ParsedCell], column: str) -> str:
    return _field_cell(row, column).value.strip()


def _field_number(row: dict[str, ParsedCell], column: str) -> float | None:
    return _to_float(_field_cell(row, column).value)


def _read_workbook(path: Path) -> tuple[str, dict[int, dict[str, ParsedCell]]]:
    with zipfile.ZipFile(path) as archive:
        shared_strings = _read_shared_strings(archive)
        workbook_root = ET.fromstring(archive.read('xl/workbook.xml'))
        sheet = workbook_root.find('.//a:sheets/a:sheet', XLSX_NS)
        sheet_name = sheet.attrib.get('name', 'Sheet1') if sheet is not None else 'Sheet1'
        root = ET.fromstring(archive.read('xl/worksheets/sheet1.xml'))
        rows: dict[int, dict[str, ParsedCell]] = {}
        for row_el in root.findall('.//a:sheetData/a:row', XLSX_NS):
            row_no = int(row_el.attrib.get('r', '0') or 0)
            parsed: dict[str, ParsedCell] = {}
            for cell in row_el.findall('a:c', XLSX_NS):
                ref = cell.attrib.get('r', '')
                column_idx = _column_index(ref)
                column = _index_to_column_letter(column_idx)
                parsed[column] = ParsedCell(
                    value=_cell_value(cell, shared_strings),
                    formula=_cell_formula(cell),
                    cell_type=cell.attrib.get('t', ''),
                )
            if parsed:
                rows[row_no] = parsed
        return sheet_name, rows


def _index_to_column_letter(index: int) -> str:
    index += 1
    out = ''
    while index:
        index, remainder = divmod(index - 1, 26)
        out = chr(65 + remainder) + out
    return out


def _period_label_from_filename(path: Path) -> str:
    stem = path.stem
    match = re.search(r'([A-Za-z]+[0-9]{2,4})$', stem)
    return match.group(1) if match else ''


def _parameters(rows: dict[int, dict[str, ParsedCell]]) -> dict[str, float | int]:
    return {
        'target_stock_weeks': _safe_float(_field_text(rows.get(7, {}), 'C')) or 4,
        'overstock_weeks': _safe_float(_field_text(rows.get(8, {}), 'C')) or 12,
        'sales_avg_period_1_weeks': int(_safe_float(_field_text(rows.get(9, {}), 'C')) or 4),
        'sales_avg_period_2_weeks': int(_safe_float(_field_text(rows.get(10, {}), 'C')) or 12),
    }


def _row_has_product(row: dict[str, ParsedCell]) -> bool:
    return bool(_field_text(row, 'A') or _field_text(row, 'B'))


def _issue(
    *,
    row_no: int,
    item_code: str,
    item_name: str,
    issue_code: str,
    field_name: str,
    source_cell: str,
    raw_value: str,
    message: str,
    severity: str = 'warning',
    metadata: dict[str, object] | None = None,
) -> dict[str, object]:
    return {
        'source_row': row_no,
        'item_code': item_code or None,
        'item_name': item_name or None,
        'issue_code': issue_code,
        'field_name': field_name,
        'source_cell': source_cell,
        'raw_value': raw_value,
        'message': message,
        'severity': severity,
        'metadata_json': metadata or {},
    }


def _store_metrics(row: dict[str, ParsedCell]) -> dict[str, dict[str, float]]:
    metrics: dict[str, dict[str, float]] = {}
    for store_idx, store_code in enumerate(STORE_CODES):
        item: dict[str, float] = {}
        for field, columns in STORE_COLUMN_GROUPS.items():
            item[field] = _safe_float(_field_text(row, columns[store_idx]))
        item['store_name'] = STORE_NAMES[store_code]  # type: ignore[assignment]
        metrics[store_code] = item
    return metrics


def _parse_product_rows(rows: dict[int, dict[str, ParsedCell]]) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    parsed_rows: list[dict[str, object]] = []
    issues: list[dict[str, object]] = []
    for row_no in sorted(rows):
        if row_no < DATA_START_ROW:
            continue
        row = rows[row_no]
        if not _row_has_product(row):
            continue
        item_code = _clean_code(_field_text(row, FIELD_COLUMNS['item_code']))
        item_name = _field_text(row, FIELD_COLUMNS['item_name'])
        line: dict[str, object] = {
            'source_row': row_no,
            'item_code': item_code,
            'item_name': item_name,
            'category_1': _field_text(row, FIELD_COLUMNS['category_1']),
            'category_2': _field_text(row, FIELD_COLUMNS['category_2']),
            'category_3': _field_text(row, FIELD_COLUMNS['category_3']),
            'status_1': _field_text(row, FIELD_COLUMNS['status_1']),
            'status_2': _field_text(row, FIELD_COLUMNS['status_2']),
            'store_metrics_json': _store_metrics(row),
            'raw_json': {},
        }
        raw_json: dict[str, object] = {}
        for field, column in FIELD_COLUMNS.items():
            cell = _field_cell(row, column)
            raw_json[field] = {
                'cell': _cell_ref(column, row_no),
                'value': cell.value,
                'formula': cell.formula,
                'type': cell.cell_type,
            }
            if field in NUMERIC_FIELDS:
                line[field] = _field_number(row, column)
            elif field not in line:
                line[field] = cell.value
            if cell.cell_type == 'e' or str(cell.value).startswith('#'):
                issues.append(
                    _issue(
                        row_no=row_no,
                        item_code=item_code,
                        item_name=item_name,
                        issue_code='excel_error',
                        field_name=field,
                        source_cell=_cell_ref(column, row_no),
                        raw_value=cell.value,
                        message=f'Το Excel επιστρέφει σφάλμα {cell.value} στο πεδίο {field}.',
                        severity='error',
                        metadata={'formula': cell.formula},
                    )
                )
            if '#REF!' in cell.formula:
                issues.append(
                    _issue(
                        row_no=row_no,
                        item_code=item_code,
                        item_name=item_name,
                        issue_code='formula_ref_error',
                        field_name=field,
                        source_cell=_cell_ref(column, row_no),
                        raw_value=cell.value,
                        message=f'Ο τύπος του Excel περιέχει χαλασμένη αναφορά: {cell.formula}.',
                        severity='error',
                        metadata={'formula': cell.formula},
                    )
                )
        line['raw_json'] = raw_json
        vendor_moq = line.get('vendor_moq')
        purchase_price = line.get('purchase_price')
        if vendor_moq is None:
            cell = _field_cell(row, FIELD_COLUMNS['vendor_moq'])
            issues.append(
                _issue(
                    row_no=row_no,
                    item_code=item_code,
                    item_name=item_name,
                    issue_code='invalid_vendor_moq',
                    field_name='vendor_moq',
                    source_cell=_cell_ref(FIELD_COLUMNS['vendor_moq'], row_no),
                    raw_value=cell.value,
                    message='Το Vendor MOQ είναι κενό ή μη αριθμητικό και δεν μπορεί να χρησιμοποιηθεί σε πρόταση παραγγελίας.',
                    severity='error',
                    metadata={'formula': cell.formula},
                )
            )
        if purchase_price is None:
            cell = _field_cell(row, FIELD_COLUMNS['purchase_price'])
            issues.append(
                _issue(
                    row_no=row_no,
                    item_code=item_code,
                    item_name=item_name,
                    issue_code='invalid_purchase_price',
                    field_name='purchase_price',
                    source_cell=_cell_ref(FIELD_COLUMNS['purchase_price'], row_no),
                    raw_value=cell.value,
                    message='Η τελική τιμή αγοράς είναι κενή ή μη αριθμητική.',
                    severity='warning',
                    metadata={'formula': cell.formula},
                )
            )
        store_metrics = line['store_metrics_json']
        total_need = sum(float(metric.get('need_qty') or 0) for metric in store_metrics.values())  # type: ignore[union-attr]
        total_overstock = sum(float(metric.get('overstock_qty') or 0) for metric in store_metrics.values())  # type: ignore[union-attr]
        total_stock = sum(float(metric.get('stock_qty') or 0) for metric in store_metrics.values())  # type: ignore[union-attr]
        total_sales = sum(max(float(metric.get('sales_avg_1') or 0), float(metric.get('sales_avg_2') or 0)) for metric in store_metrics.values())  # type: ignore[union-attr]
        line['total_need_qty'] = total_need
        line['total_overstock_qty'] = total_overstock
        if total_sales <= 0 and total_stock >= DEFAULT_ZERO_SALES_HIGH_STOCK_THRESHOLD:
            issues.append(
                _issue(
                    row_no=row_no,
                    item_code=item_code,
                    item_name=item_name,
                    issue_code='zero_sales_high_stock',
                    field_name='stock_qty',
                    source_cell=f'W{row_no}:AB{row_no}',
                    raw_value=str(total_stock),
                    message='Το προϊόν έχει μηδενικές πωλήσεις και υψηλό διαθέσιμο stock.',
                    severity='warning',
                    metadata={'total_stock': total_stock},
                )
            )
        for store_code, metric in store_metrics.items():  # type: ignore[union-attr]
            stock_qty = float(metric.get('stock_qty') or 0)
            expected_qty = float(metric.get('expected_qty') or 0)
            if stock_qty < 0:
                issues.append(
                    _issue(
                        row_no=row_no,
                        item_code=item_code,
                        item_name=item_name,
                        issue_code='negative_stock',
                        field_name='stock_qty',
                        source_cell=f'{STORE_COLUMN_GROUPS["stock_qty"][STORE_CODES.index(store_code)]}{row_no}',
                        raw_value=str(stock_qty),
                        message=f'Αρνητικό απόθεμα στο σημείο {STORE_NAMES.get(store_code, store_code)}.',
                        severity='warning',
                        metadata={'store': store_code},
                    )
                )
            if expected_qty < 0:
                issues.append(
                    _issue(
                        row_no=row_no,
                        item_code=item_code,
                        item_name=item_name,
                        issue_code='negative_expected_qty',
                        field_name='expected_qty',
                        source_cell=f'{STORE_COLUMN_GROUPS["expected_qty"][STORE_CODES.index(store_code)]}{row_no}',
                        raw_value=str(expected_qty),
                        message=f'Αρνητικά αναμενόμενα στο σημείο {STORE_NAMES.get(store_code, store_code)}. Στους υπολογισμούς πρέπει να μετρήσουν ως 0.',
                        severity='warning',
                        metadata={'store': store_code},
                    )
                )
        parsed_rows.append(line)
    return parsed_rows, issues


def load_fnr_workbook(path: Path) -> dict[str, object]:
    if not path.exists():
        raise FileNotFoundError(f'Δεν βρέθηκε το αρχείο FnR: {path}')
    try:
        sheet_name, workbook_rows = _read_workbook(path)
    except zipfile.BadZipFile as exc:
        raise ValueError('Το αρχείο FnR δεν είναι έγκυρο XLSX.') from exc
    params = _parameters(workbook_rows)
    rows, issues = _parse_product_rows(workbook_rows)
    total_supplier_order_qty = sum(_safe_float(row.get('supplier_order_qty')) for row in rows)
    total_supplier_order_value = sum(_safe_float(row.get('supplier_order_value')) for row in rows)
    products_with_need = sum(1 for row in rows if _safe_float(row.get('total_need_qty')) > 0)
    products_with_overstock = sum(1 for row in rows if _safe_float(row.get('total_overstock_qty')) < 0)
    summary = {
        'sheet_name': sheet_name,
        'rows_count': len(rows),
        'issue_count': len(issues),
        'total_supplier_order_qty': total_supplier_order_qty,
        'total_supplier_order_value': total_supplier_order_value,
        'products_with_need': products_with_need,
        'products_with_overstock': products_with_overstock,
        'data_quality': {
            'errors': sum(1 for issue in issues if issue.get('severity') == 'error'),
            'warnings': sum(1 for issue in issues if issue.get('severity') != 'error'),
        },
    }
    return {
        'source_filename': path.name,
        'period_label': _period_label_from_filename(path),
        'parameters': params,
        'summary': summary,
        'rows': rows,
        'issues': issues,
    }


def validate_fnr_file(path: Path) -> dict[str, object]:
    loaded = load_fnr_workbook(path)
    return {
        'source_filename': loaded['source_filename'],
        'period_label': loaded['period_label'],
        'parameters': loaded['parameters'],
        'summary': loaded['summary'],
        'issues': loaded['issues'],
    }


async def build_replenishment_from_facts(
    db: AsyncSession,
    *,
    as_of: date | None = None,
    target_stock_weeks: float = 4.0,
    overstock_weeks: float = 12.0,
    sales_avg_period_1_weeks: int = 4,
    sales_avg_period_2_weeks: int = 12,
) -> dict[str, object]:
    """Build a lightweight FnR view from BI facts when no FnR upload exists yet."""
    as_of = as_of or date.today()
    period_1_start = as_of - timedelta(weeks=max(sales_avg_period_1_weeks, 1))
    period_2_start = as_of - timedelta(weeks=max(sales_avg_period_2_weeks, 1))
    purchase_avg_start = as_of - timedelta(days=365)
    sql = text(
        """
        WITH latest_snapshot AS (
            SELECT MAX(snapshot_date) AS snapshot_date
            FROM agg_inventory_snapshot_daily
            WHERE snapshot_date <= :as_of
        ),
        inventory AS (
            SELECT
                ais.item_external_id AS item_code,
                MAX(COALESCE(di.name, ais.item_external_id)) AS item_name,
                MAX(di.category_1) AS category_1,
                MAX(di.category_2) AS category_2,
                MAX(di.category_3) AS category_3,
                MAX(di.replenishment_status_1) AS status_1,
                MAX(di.replenishment_status_2) AS status_2,
                MAX(COALESCE(di.min_stock, 1)) AS min_stock,
                MAX(COALESCE(di.replenishment_moq, 1)) AS repl_moq,
                MAX(COALESCE(di.vendor_moq, 1)) AS vendor_moq,
                MAX(COALESCE(di.current_purchase_price, 0)) AS purchase_price,
                SUM(COALESCE(ais.qty_on_hand, 0)) AS available_qty,
                SUM(COALESCE(ais.value_amount, 0)) AS stock_value
            FROM agg_inventory_snapshot_daily ais
            JOIN latest_snapshot ls ON ls.snapshot_date = ais.snapshot_date
            LEFT JOIN dim_items di ON di.external_id = ais.item_external_id
            GROUP BY ais.item_external_id
        ),
        expected_orders AS (
            SELECT
                COALESCE(di.external_id, fso.item_code) AS item_code,
                SUM(
                    GREATEST(
                        COALESCE(fso.order_qty, 0)
                        - COALESCE(fso.covered_qty, 0)
                        - COALESCE(fso.cancelled_qty, 0),
                        0
                    )
                ) AS expected_qty
            FROM fact_supplier_orders fso
            LEFT JOIN dim_items di ON di.id = fso.item_id
            WHERE COALESCE(fso.order_status, 'open') = 'open'
              AND COALESCE(fso.has_transformation, false) = false
              AND (fso.item_id IS NOT NULL OR COALESCE(fso.item_code, '') <> '')
            GROUP BY COALESCE(di.external_id, fso.item_code)
        ),
        latest_purchase_price AS (
            SELECT item_code, purchase_price
            FROM (
                SELECT
                    COALESCE(di.external_id, fp.item_code) AS item_code,
                    COALESCE(fp.net_value, 0) / NULLIF(COALESCE(fp.qty, 0), 0) AS purchase_price,
                    ROW_NUMBER() OVER (
                        PARTITION BY COALESCE(di.external_id, fp.item_code)
                        ORDER BY fp.doc_date DESC, fp.updated_at DESC
                    ) AS rn
                FROM fact_purchases fp
                LEFT JOIN dim_items di ON di.id = fp.item_id
                WHERE COALESCE(fp.item_code, di.external_id, '') <> ''
                  AND COALESCE(fp.qty, 0) > 0
                  AND COALESCE(fp.net_value, 0) > 0
            ) ranked_price
            WHERE rn = 1
        ),
        weighted_purchase_price AS (
            SELECT
                COALESCE(di.external_id, fp.item_code) AS item_code,
                SUM(COALESCE(fp.net_value, 0)) / NULLIF(SUM(COALESCE(fp.qty, 0)), 0) AS avg_purchase_price
            FROM fact_purchases fp
            LEFT JOIN dim_items di ON di.id = fp.item_id
            WHERE fp.doc_date >= :purchase_avg_start
              AND fp.doc_date <= :as_of
              AND COALESCE(fp.item_code, di.external_id, '') <> ''
              AND COALESCE(fp.qty, 0) > 0
              AND COALESCE(fp.net_value, 0) > 0
            GROUP BY COALESCE(di.external_id, fp.item_code)
        ),
        sales AS (
            SELECT
                asi.item_external_id AS item_code,
                GREATEST(SUM(CASE WHEN asi.doc_date >= :period_1_start THEN COALESCE(asi.qty, 0) ELSE 0 END), 0) / :period_1_weeks AS sales_avg_1,
                GREATEST(SUM(CASE WHEN asi.doc_date >= :period_2_start THEN COALESCE(asi.qty, 0) ELSE 0 END), 0) / :period_2_weeks AS sales_avg_2
            FROM agg_sales_item_daily asi
            WHERE asi.doc_date >= :period_2_start
              AND asi.doc_date <= :as_of
              AND COALESCE(asi.item_external_id, '') <> ''
            GROUP BY asi.item_external_id
        ),
        scope AS (
            SELECT item_code FROM inventory WHERE COALESCE(item_code, '') <> ''
            UNION
            SELECT item_code FROM sales WHERE COALESCE(item_code, '') <> ''
            UNION
            SELECT item_code FROM expected_orders WHERE COALESCE(item_code, '') <> ''
        )
        SELECT
            scope.item_code,
            COALESCE(inventory.item_name, dim_items.name, scope.item_code) AS item_name,
            COALESCE(inventory.category_1, dim_items.category_1) AS category_1,
            COALESCE(inventory.category_2, dim_items.category_2) AS category_2,
            COALESCE(inventory.category_3, dim_items.category_3) AS category_3,
            COALESCE(inventory.status_1, dim_items.replenishment_status_1) AS status_1,
            COALESCE(inventory.status_2, dim_items.replenishment_status_2) AS status_2,
            COALESCE(inventory.min_stock, dim_items.min_stock, 1) AS min_stock,
            COALESCE(inventory.repl_moq, dim_items.replenishment_moq, 1) AS repl_moq,
            COALESCE(inventory.vendor_moq, dim_items.vendor_moq, 1) AS vendor_moq,
            COALESCE(
                NULLIF(latest_purchase_price.purchase_price, 0),
                NULLIF(dim_items.current_purchase_price, 0),
                NULLIF(inventory.purchase_price, 0),
                NULLIF(weighted_purchase_price.avg_purchase_price, 0),
                0
            ) AS purchase_price,
            COALESCE(
                NULLIF(weighted_purchase_price.avg_purchase_price, 0),
                NULLIF(latest_purchase_price.purchase_price, 0),
                NULLIF(dim_items.current_purchase_price, 0),
                NULLIF(inventory.purchase_price, 0),
                0
            ) AS avg_purchase_price,
            COALESCE(inventory.available_qty, 0) AS available_qty,
            COALESCE(expected_orders.expected_qty, 0) AS expected_qty,
            COALESCE(inventory.stock_value, 0) AS stock_value,
            COALESCE(sales.sales_avg_1, 0) AS sales_avg_1,
            COALESCE(sales.sales_avg_2, 0) AS sales_avg_2
        FROM scope
        LEFT JOIN inventory ON inventory.item_code = scope.item_code
        LEFT JOIN expected_orders ON expected_orders.item_code = scope.item_code
        LEFT JOIN sales ON sales.item_code = scope.item_code
        LEFT JOIN dim_items ON dim_items.external_id = scope.item_code
        LEFT JOIN latest_purchase_price ON latest_purchase_price.item_code = scope.item_code
        LEFT JOIN weighted_purchase_price ON weighted_purchase_price.item_code = scope.item_code
        WHERE COALESCE(dim_items.softone_sotype, 51) = 51
          AND COALESCE(dim_items.is_active_source, true) = true
        """
    )
    result = await db.execute(
        sql,
        {
            'as_of': as_of,
            'period_1_start': period_1_start,
            'period_2_start': period_2_start,
            'purchase_avg_start': purchase_avg_start,
            'period_1_weeks': max(sales_avg_period_1_weeks, 1),
            'period_2_weeks': max(sales_avg_period_2_weeks, 1),
        },
    )
    lines: list[SimpleNamespace] = []
    issues: list[SimpleNamespace] = []
    for index, row in enumerate(result.mappings().all(), start=1):
        available_qty = _as_float(row.get('available_qty'))
        expected_qty = max(_as_float(row.get('expected_qty')), 0.0)
        sales_avg_1 = _as_float(row.get('sales_avg_1'))
        sales_avg_2 = _as_float(row.get('sales_avg_2'))
        weekly_sales = max(sales_avg_1, sales_avg_2)
        min_stock = max(_as_float(row.get('min_stock'), 1.0), 0.0)
        repl_moq = max(_as_float(row.get('repl_moq'), 1.0), 1.0)
        vendor_moq = max(_as_float(row.get('vendor_moq'), 1.0), 1.0)
        purchase_price = _as_float(row.get('purchase_price'))
        avg_purchase_price = _as_float(row.get('avg_purchase_price'))
        stock_value = _as_float(row.get('stock_value'))
        if purchase_price <= 0 and available_qty > 0 and stock_value > 0:
            purchase_price = stock_value / available_qty
        if avg_purchase_price <= 0 and available_qty > 0 and stock_value > 0:
            avg_purchase_price = stock_value / available_qty
        target_stock = max(target_stock_weeks * weekly_sales, min_stock)
        raw_need = target_stock - available_qty - expected_qty
        need_qty = max(raw_need, repl_moq) if raw_need > 0 else 0.0
        status_text = f"{row.get('status_1') or ''} {row.get('status_2') or ''}".upper()
        if available_qty <= 0:
            weeks_of_stock = 0.0
        elif weekly_sales <= 0:
            weeks_of_stock = 999.0
        else:
            weeks_of_stock = available_qty / weekly_sales
        if any(token in status_text.split() for token in ('C', 'D')):
            overstock_qty = -available_qty if available_qty > 0 else 0.0
        elif need_qty > 0:
            overstock_qty = 0.0
        elif weeks_of_stock > overstock_weeks:
            overstock_qty = min(target_stock - available_qty - expected_qty, 0.0)
        else:
            overstock_qty = 0.0
        supplier_order_qty = max(need_qty, vendor_moq) if need_qty > 0 else 0.0
        supplier_order_value = supplier_order_qty * purchase_price
        item_code = str(row.get('item_code') or '')
        item_name = str(row.get('item_name') or item_code)
        if supplier_order_qty > 0 and purchase_price <= 0:
            issues.append(
                SimpleNamespace(
                    source_row=index,
                    item_code=item_code,
                    item_name=item_name,
                    source_cell='current_purchase_price',
                    field_name='purchase_price',
                    issue_code='missing_purchase_price',
                    message='Υπάρχει ανάγκη παραγγελίας, αλλά δεν υπάρχει τελική τιμή αγοράς για αξία παραγγελίας.',
                )
            )
        lines.append(
            SimpleNamespace(
                source_row=index,
                item_code=item_code,
                item_name=item_name,
                category_1=row.get('category_1') or '',
                category_2=row.get('category_2') or '',
                category_3=row.get('category_3') or '',
                status_1=row.get('status_1') or '',
                status_2=row.get('status_2') or '',
                min_stock=min_stock,
                repl_moq=repl_moq,
                vendor_moq=vendor_moq,
                purchase_price=purchase_price,
                avg_purchase_price=avg_purchase_price,
                total_need_qty=need_qty,
                total_overstock_qty=overstock_qty,
                supplier_order_qty=supplier_order_qty,
                supplier_order_value=supplier_order_value,
                weeks_of_stock_total=weeks_of_stock,
                available_qty=available_qty,
                expected_qty=expected_qty,
                sales_avg_1=sales_avg_1,
                sales_avg_2=sales_avg_2,
                target_stock=target_stock,
            )
        )
    top_need_rows = sorted(
        (line for line in lines if (line.supplier_order_qty or 0) > 0),
        key=lambda line: line.supplier_order_qty or 0,
        reverse=True,
    )[:10]
    top_overstock_rows = sorted(
        (line for line in lines if (line.total_overstock_qty or 0) < 0),
        key=lambda line: line.total_overstock_qty or 0,
    )[:10]
    summary = {
        'source': 'facts',
        'source_label': 'Live BI facts',
        'as_of': as_of.isoformat(),
        'rows_count': len(lines),
        'issue_count': len(issues),
        'total_supplier_order_qty': sum(line.supplier_order_qty for line in lines),
        'total_supplier_order_value': sum(line.supplier_order_value for line in lines),
        'products_with_need': sum(1 for line in lines if line.total_need_qty > 0),
        'products_with_overstock': sum(1 for line in lines if line.total_overstock_qty < 0),
    }
    return {
        'summary': summary,
        'top_need_rows': top_need_rows,
        'top_overstock_rows': top_overstock_rows,
        'issue_rows': issues[:20],
    }


async def build_availability_foundation(
    db: AsyncSession,
    *,
    as_of: date | None = None,
    target_stock_weeks: float = 4.0,
    overstock_weeks: float = 12.0,
    sales_window_days: int = 30,
    branch: str | None = None,
    status: str | None = None,
    category: str | None = None,
    vendor: str | None = None,
    abc: str | None = None,
    search: str | None = None,
    include_detail_rows: bool = False,
    detail_kind: str = 'all',
    detail_dimension: str | None = None,
    detail_value: str | None = None,
    detail_limit: int = 120,
) -> dict[str, object]:
    """Availability view from BI facts, used as the base for FnR/Destocking."""
    as_of = as_of or date.today()
    window_days = max(int(sales_window_days or 30), 1)
    current_start = as_of - timedelta(days=window_days - 1)
    previous_start = current_start - timedelta(days=window_days)
    purchase_start = as_of - timedelta(days=180)
    purchase_avg_start = as_of - timedelta(days=365)
    sql = text(
        """
        WITH latest_stock AS (
            SELECT MAX(snapshot_date) AS snapshot_date
            FROM agg_stock_aging
            WHERE snapshot_date <= :as_of
        ),
        stock AS (
            SELECT
                asa.item_external_id AS item_code,
                COALESCE(asa.branch_ext_id, '') AS branch_ext_id,
                MAX(COALESCE(db.name, asa.branch_ext_id, 'Χωρίς σημείο')) AS branch_name,
                SUM(COALESCE(asa.qty_on_hand, 0)) AS stock_qty,
                SUM(COALESCE(asa.stock_value, 0)) AS stock_value
            FROM agg_stock_aging asa
            JOIN latest_stock ls ON ls.snapshot_date = asa.snapshot_date
            LEFT JOIN dim_branches db ON db.external_id = asa.branch_ext_id
            GROUP BY asa.item_external_id, COALESCE(asa.branch_ext_id, '')
        ),
        sales AS (
            SELECT
                fs.item_code AS item_code,
                COALESCE(fs.branch_ext_id, '') AS branch_ext_id,
                SUM(CASE WHEN fs.doc_date >= :current_start THEN COALESCE(fs.qty, 0) ELSE 0 END) AS sales_qty_current,
                SUM(CASE WHEN fs.doc_date >= :current_start THEN COALESCE(fs.net_value, 0) ELSE 0 END) AS sales_value_current,
                SUM(CASE WHEN fs.doc_date >= :previous_start AND fs.doc_date < :current_start THEN COALESCE(fs.qty, 0) ELSE 0 END) AS sales_qty_previous,
                SUM(CASE WHEN fs.doc_date >= :previous_start AND fs.doc_date < :current_start THEN COALESCE(fs.net_value, 0) ELSE 0 END) AS sales_value_previous
            FROM fact_sales fs
            WHERE fs.doc_date >= :previous_start
              AND fs.doc_date <= :as_of
              AND COALESCE(fs.item_code, '') <> ''
            GROUP BY fs.item_code, COALESCE(fs.branch_ext_id, '')
        ),
        expected AS (
            SELECT
                fso.item_code AS item_code,
                SUM(
                    GREATEST(
                        COALESCE(fso.order_qty, 0) - COALESCE(fso.covered_qty, 0) - COALESCE(fso.cancelled_qty, 0),
                        0
                    )
                ) AS expected_qty,
                MAX(COALESCE(fso.supplier_name, ds.name, ds_ext.name, fso.supplier_ext_id, 'Χωρίς προμηθευτή')) AS expected_supplier
            FROM fact_supplier_orders fso
            LEFT JOIN dim_suppliers ds ON ds.id = fso.supplier_id
            LEFT JOIN dim_suppliers ds_ext ON ds_ext.external_id = fso.supplier_ext_id
            WHERE COALESCE(fso.order_status, 'open') = 'open'
              AND COALESCE(fso.has_transformation, false) = false
              AND COALESCE(fso.item_code, '') <> ''
            GROUP BY fso.item_code
        ),
        supplier_rank AS (
            SELECT *
            FROM (
                SELECT
                    fp.item_code AS item_code,
                    COALESCE(ds.name, ds_ext.name, fp.supplier_ext_id, 'Χωρίς προμηθευτή') AS supplier_name,
                    ROW_NUMBER() OVER (
                        PARTITION BY fp.item_code
                        ORDER BY ABS(SUM(COALESCE(fp.net_value, 0))) DESC
                    ) AS rn
                FROM fact_purchases fp
                LEFT JOIN dim_suppliers ds ON ds.id = fp.supplier_id
                LEFT JOIN dim_suppliers ds_ext ON ds_ext.external_id = fp.supplier_ext_id
                WHERE fp.doc_date >= :purchase_start
                  AND fp.doc_date <= :as_of
                  AND COALESCE(fp.item_code, '') <> ''
                GROUP BY fp.item_code, COALESCE(ds.name, ds_ext.name, fp.supplier_ext_id, 'Χωρίς προμηθευτή')
            ) ranked
            WHERE rn = 1
        ),
        latest_purchase_price AS (
            SELECT item_code, purchase_price
            FROM (
                SELECT
                    COALESCE(di.external_id, fp.item_code) AS item_code,
                    COALESCE(fp.net_value, 0) / NULLIF(COALESCE(fp.qty, 0), 0) AS purchase_price,
                    ROW_NUMBER() OVER (
                        PARTITION BY COALESCE(di.external_id, fp.item_code)
                        ORDER BY fp.doc_date DESC, fp.updated_at DESC
                    ) AS rn
                FROM fact_purchases fp
                LEFT JOIN dim_items di ON di.id = fp.item_id
                WHERE COALESCE(fp.item_code, di.external_id, '') <> ''
                  AND COALESCE(fp.qty, 0) > 0
                  AND COALESCE(fp.net_value, 0) > 0
            ) ranked_price
            WHERE rn = 1
        ),
        weighted_purchase_price AS (
            SELECT
                COALESCE(di.external_id, fp.item_code) AS item_code,
                SUM(COALESCE(fp.net_value, 0)) / NULLIF(SUM(COALESCE(fp.qty, 0)), 0) AS avg_purchase_price
            FROM fact_purchases fp
            LEFT JOIN dim_items di ON di.id = fp.item_id
            WHERE fp.doc_date >= :purchase_avg_start
              AND fp.doc_date <= :as_of
              AND COALESCE(fp.item_code, di.external_id, '') <> ''
              AND COALESCE(fp.qty, 0) > 0
              AND COALESCE(fp.net_value, 0) > 0
            GROUP BY COALESCE(di.external_id, fp.item_code)
        ),
        scope AS (
            SELECT item_code, branch_ext_id FROM stock WHERE COALESCE(item_code, '') <> ''
            UNION
            SELECT item_code, branch_ext_id FROM sales WHERE COALESCE(item_code, '') <> ''
        )
        SELECT
            scope.item_code,
            COALESCE(scope.branch_ext_id, '') AS branch_ext_id,
            COALESCE(stock.branch_name, db.name, scope.branch_ext_id, 'Χωρίς σημείο') AS branch_name,
            COALESCE(di.name, scope.item_code) AS item_name,
            COALESCE(
                NULLIF(di.replenishment_status_1, ''),
                NULLIF(di.commercial_status, ''),
                'Χωρίς FnR status'
            ) AS status_1,
            COALESCE(NULLIF(di.replenishment_status_2, ''), 'Χωρίς status') AS status_2,
            COALESCE(NULLIF(di.category_1, ''), 'Χωρίς κατηγορία') AS category_1,
            COALESCE(NULLIF(di.category_2, ''), '') AS category_2,
            COALESCE(NULLIF(di.category_3, ''), '') AS category_3,
            COALESCE(NULLIF(di.manual_order_category, ''), NULLIF(di.abc_category, ''), 'Χωρίς ABC') AS abc_category,
            COALESCE(NULLIF(di.commercial_status, ''), '-') AS commercial_status,
            COALESCE(NULLIF(supplier_rank.supplier_name, ''), NULLIF(expected.expected_supplier, ''), 'Χωρίς προμηθευτή') AS supplier_name,
            COALESCE(stock.stock_qty, 0) AS stock_qty,
            COALESCE(stock.stock_value, 0) AS stock_value,
            COALESCE(sales.sales_qty_current, 0) AS sales_qty_current,
            COALESCE(sales.sales_value_current, 0) AS sales_value_current,
            COALESCE(sales.sales_qty_previous, 0) AS sales_qty_previous,
            COALESCE(sales.sales_value_previous, 0) AS sales_value_previous,
            COALESCE(expected.expected_qty, 0) AS expected_qty,
            COALESCE(di.min_stock, 1) AS min_stock,
            COALESCE(
                NULLIF(latest_purchase_price.purchase_price, 0),
                NULLIF(di.current_purchase_price, 0),
                NULLIF(weighted_purchase_price.avg_purchase_price, 0),
                0
            ) AS purchase_price,
            COALESCE(
                NULLIF(weighted_purchase_price.avg_purchase_price, 0),
                NULLIF(latest_purchase_price.purchase_price, 0),
                NULLIF(di.current_purchase_price, 0),
                0
            ) AS avg_purchase_price
        FROM scope
        LEFT JOIN stock ON stock.item_code = scope.item_code AND stock.branch_ext_id = scope.branch_ext_id
        LEFT JOIN sales ON sales.item_code = scope.item_code AND sales.branch_ext_id = scope.branch_ext_id
        LEFT JOIN dim_items di ON di.external_id = scope.item_code
        LEFT JOIN dim_branches db ON db.external_id = scope.branch_ext_id
        LEFT JOIN expected ON expected.item_code = scope.item_code
        LEFT JOIN supplier_rank ON supplier_rank.item_code = scope.item_code
        LEFT JOIN latest_purchase_price ON latest_purchase_price.item_code = scope.item_code
        LEFT JOIN weighted_purchase_price ON weighted_purchase_price.item_code = scope.item_code
        WHERE COALESCE(di.softone_sotype, 51) = 51
          AND COALESCE(di.is_active_source, true) = true
        """
    )
    result = await db.execute(
        sql,
        {
            'as_of': as_of,
            'current_start': current_start,
            'previous_start': previous_start,
            'purchase_start': purchase_start,
            'purchase_avg_start': purchase_avg_start,
        },
    )

    summary = {
        'as_of': as_of.isoformat(),
        'items': 0,
        'stock_value': 0.0,
        'stock_qty': 0.0,
        'expected_qty': 0.0,
        'shortage_items': 0,
        'shortage_value': 0.0,
        'overstock_items': 0,
        'stockout_items': 0,
    }
    by_store: dict[str, dict[str, object]] = {}
    by_status: dict[str, dict[str, object]] = {}
    by_category: dict[str, dict[str, object]] = {}
    by_vendor: dict[str, dict[str, object]] = {}
    seen_items: set[str] = set()
    seen_expected_items: set[str] = set()
    options = {
        'branches': set(),
        'statuses': set(),
        'categories': set(),
        'vendors': set(),
        'abcs': set(),
    }
    detail_rows: list[dict[str, object]] = []
    branch_value = (branch or '').strip()
    status_value = (status or '').strip()
    category_value = (category or '').strip()
    vendor_value = (vendor or '').strip()
    abc_value = (abc or '').strip()
    search_value = (search or '').strip()
    branch_filter = branch_value.casefold()
    status_filter = status_value.casefold()
    category_filter = category_value.casefold()
    vendor_filter = vendor_value.casefold()
    abc_filter = abc_value.casefold()
    search_filter = search_value.casefold()
    detail_dimension = (detail_dimension or '').strip()
    detail_value_fold = (detail_value or '').strip().casefold()
    detail_kind = (detail_kind or 'all').strip()

    for row in result.mappings().all():
        item_code = str(row.get('item_code') or '').strip()
        branch = str(row.get('branch_name') or row.get('branch_ext_id') or 'Χωρίς σημείο').strip() or 'Χωρίς σημείο'
        status = str(row.get('status_1') or 'Χωρίς status').strip() or 'Χωρίς status'
        category = str(row.get('category_1') or 'Χωρίς κατηγορία').strip() or 'Χωρίς κατηγορία'
        vendor = str(row.get('supplier_name') or 'Χωρίς προμηθευτή').strip() or 'Χωρίς προμηθευτή'
        abc_category = str(row.get('abc_category') or 'Χωρίς ABC').strip() or 'Χωρίς ABC'
        commercial_status = str(row.get('commercial_status') or '-').strip() or '-'
        stock_qty = _as_float(row.get('stock_qty'))
        stock_value = _as_float(row.get('stock_value'))
        expected_qty = max(_as_float(row.get('expected_qty')), 0.0)
        current_qty = max(_as_float(row.get('sales_qty_current')), 0.0)
        previous_qty = max(_as_float(row.get('sales_qty_previous')), 0.0)
        current_value = max(_as_float(row.get('sales_value_current')), 0.0)
        previous_value = max(_as_float(row.get('sales_value_previous')), 0.0)
        purchase_price = _as_float(row.get('purchase_price'))
        avg_purchase_price = _as_float(row.get('avg_purchase_price'))
        if purchase_price <= 0 and stock_qty > 0 and stock_value > 0:
            purchase_price = stock_value / stock_qty
        if avg_purchase_price <= 0:
            avg_purchase_price = purchase_price
        if avg_purchase_price <= 0 and stock_qty > 0 and stock_value > 0:
            avg_purchase_price = stock_value / stock_qty
        purchase_price_variance_pct = (
            ((purchase_price - avg_purchase_price) / avg_purchase_price * 100.0)
            if avg_purchase_price > 0 and purchase_price > 0
            else 0.0
        )
        valuation_purchase_price = avg_purchase_price if avg_purchase_price > 0 else purchase_price
        weekly_sales = current_qty / max(window_days / 7.0, 1.0)
        target_stock = max(target_stock_weeks * weekly_sales, _as_float(row.get('min_stock'), 1.0))
        # Expected supplier quantities are item-level in BI, not store-level. Keep them visible,
        # but do not subtract them from every store row and hide real availability pressure.
        shortage_qty = max(target_stock - stock_qty, 0.0)
        shortage_value = shortage_qty * max(valuation_purchase_price, 0.0)
        weeks_of_stock = 999.0 if weekly_sales <= 0 and stock_qty > 0 else (stock_qty / weekly_sales if weekly_sales > 0 else 0.0)
        is_stockout = current_qty > 0 and stock_qty <= 0
        is_overstock = stock_qty > 0 and weekly_sales > 0 and weeks_of_stock > overstock_weeks and shortage_qty <= 0

        options['branches'].add(branch)
        options['statuses'].add(status)
        options['categories'].add(category)
        options['vendors'].add(vendor)
        options['abcs'].add(abc_category)
        if branch_filter and branch.casefold() != branch_filter:
            continue
        if status_filter and status.casefold() != status_filter:
            continue
        if category_filter and category.casefold() != category_filter:
            continue
        if vendor_filter and vendor.casefold() != vendor_filter:
            continue
        if abc_filter and abc_category.casefold() != abc_filter:
            continue
        if search_filter:
            haystack = ' '.join(
                [
                    item_code,
                    str(row.get('item_name') or ''),
                    str(row.get('category_1') or ''),
                    str(row.get('category_2') or ''),
                    str(row.get('category_3') or ''),
                    abc_category,
                    commercial_status,
                    vendor,
                ]
            ).casefold()
            if search_filter not in haystack:
                continue

        dimension_match = True
        if detail_dimension and detail_value_fold:
            dimension_value = {
                'store': branch,
                'status': status,
                'category': category,
                'vendor': vendor,
            }.get(detail_dimension, '')
            dimension_match = dimension_value.casefold() == detail_value_fold
        kind_match = (
            detail_kind == 'all'
            or (detail_kind == 'shortage' and shortage_qty > 0)
            or (detail_kind == 'stockout' and is_stockout)
            or (detail_kind == 'overstock' and is_overstock)
        )
        if include_detail_rows and dimension_match and kind_match:
            detail_rows.append(
                {
                    'item_code': item_code,
                    'item_name': str(row.get('item_name') or item_code),
                    'store': branch,
                    'status': status,
                    'abc_category': abc_category,
                    'commercial_status': commercial_status,
                    'category': category,
                    'vendor': vendor,
                    'stock_qty': stock_qty,
                    'stock_value': stock_value,
                    'expected_qty': expected_qty,
                    'sales_qty_current': current_qty,
                    'sales_value_current': current_value,
                    'sales_growth_pct': ((current_value - previous_value) / previous_value * 100.0)
                    if previous_value > 0 else (100.0 if current_value > 0 else 0.0),
                    'weekly_sales': weekly_sales,
                    'target_stock': target_stock,
                    'shortage_qty': shortage_qty,
                    'shortage_value': shortage_value,
                    'latest_purchase_price': purchase_price,
                    'avg_purchase_price': avg_purchase_price,
                    'valuation_purchase_price': valuation_purchase_price,
                    'purchase_price_variance_pct': purchase_price_variance_pct,
                    'weeks_of_stock': weeks_of_stock,
                    'stockout': is_stockout,
                    'overstock': is_overstock,
                }
            )

        if item_code and item_code not in seen_items:
            seen_items.add(item_code)
            summary['items'] += 1
        summary['stock_value'] += stock_value
        summary['stock_qty'] += stock_qty
        if item_code and item_code not in seen_expected_items:
            seen_expected_items.add(item_code)
            summary['expected_qty'] += expected_qty
        if shortage_qty > 0:
            summary['shortage_items'] += 1
            summary['shortage_value'] += shortage_value
        if is_overstock:
            summary['overstock_items'] += 1
        if is_stockout:
            summary['stockout_items'] += 1

        def bucket(target: dict[str, dict[str, object]], key: str, label_key: str) -> dict[str, object]:
            entry = target.setdefault(
                key,
                {
                    label_key: key,
                    'items': 0,
                    'stock_qty': 0.0,
                    'stock_value': 0.0,
                    'expected_qty': 0.0,
                    'shortage_items': 0,
                    'shortage_value': 0.0,
                    'overstock_items': 0,
                    'stockout_items': 0,
                    'sales_qty_current': 0.0,
                    'sales_qty_previous': 0.0,
                    'sales_value_current': 0.0,
                    'sales_value_previous': 0.0,
                    '_expected_items': set(),
                },
            )
            entry['items'] = int(entry['items']) + 1
            entry['stock_qty'] = float(entry['stock_qty']) + stock_qty
            entry['stock_value'] = float(entry['stock_value']) + stock_value
            expected_items = entry.get('_expected_items')
            if isinstance(expected_items, set) and item_code and item_code not in expected_items:
                expected_items.add(item_code)
                entry['expected_qty'] = float(entry['expected_qty']) + expected_qty
            entry['sales_qty_current'] = float(entry['sales_qty_current']) + current_qty
            entry['sales_qty_previous'] = float(entry['sales_qty_previous']) + previous_qty
            entry['sales_value_current'] = float(entry['sales_value_current']) + current_value
            entry['sales_value_previous'] = float(entry['sales_value_previous']) + previous_value
            if shortage_qty > 0:
                entry['shortage_items'] = int(entry['shortage_items']) + 1
                entry['shortage_value'] = float(entry['shortage_value']) + shortage_value
            if is_overstock:
                entry['overstock_items'] = int(entry['overstock_items']) + 1
            if is_stockout:
                entry['stockout_items'] = int(entry['stockout_items']) + 1
            return entry

        bucket(by_store, branch, 'store')
        bucket(by_status, status, 'status')
        bucket(by_category, category, 'category')
        bucket(by_vendor, vendor, 'vendor')

    def finalize(rows: list[dict[str, object]], sort_key: str, limit: int) -> list[dict[str, object]]:
        out = []
        for row in rows:
            current = float(row.get('sales_value_current') or 0)
            previous = float(row.get('sales_value_previous') or 0)
            row['sales_growth_pct'] = ((current - previous) / previous * 100.0) if previous > 0 else (100.0 if current > 0 else 0.0)
            row['availability_pressure'] = float(row.get('shortage_value') or 0) + (float(row.get('stockout_items') or 0) * 100.0)
            row.pop('_expected_items', None)
            out.append(row)
        return sorted(out, key=lambda r: float(r.get(sort_key) or 0), reverse=True)[:limit]

    return {
        'summary': summary,
        'stores': finalize(list(by_store.values()), 'availability_pressure', 8),
        'statuses': finalize(list(by_status.values()), 'availability_pressure', 8),
        'categories': finalize(list(by_category.values()), 'availability_pressure', 8),
        'priority_vendors': finalize(list(by_vendor.values()), 'availability_pressure', 10),
        'options': {key: sorted(value) for key, value in options.items()},
        'filters': {
            'branch': branch_value,
            'status': status_value,
            'category': category_value,
            'vendor': vendor_value,
            'abc': abc_value,
            'search': search_value,
        },
        'detail_rows': sorted(
            detail_rows,
            key=lambda item: (
                -float(item.get('shortage_value') or 0),
                -float(item.get('sales_value_current') or 0),
                str(item.get('item_name') or ''),
            ),
        )[: max(detail_limit, 1)],
    }


async def import_fnr_workbook(db: AsyncSession, path: Path, imported_by: str | None = None) -> ReplenishmentSnapshot:
    loaded = load_fnr_workbook(path)
    params = loaded['parameters']
    summary = loaded['summary']
    snapshot = ReplenishmentSnapshot(
        source_filename=str(loaded['source_filename']),
        period_label=str(loaded['period_label'] or ''),
        target_stock_weeks=params['target_stock_weeks'],
        overstock_weeks=params['overstock_weeks'],
        sales_avg_period_1_weeks=params['sales_avg_period_1_weeks'],
        sales_avg_period_2_weeks=params['sales_avg_period_2_weeks'],
        rows_count=summary['rows_count'],
        issue_count=summary['issue_count'],
        summary_json=summary,
        imported_by=imported_by,
    )
    db.add(snapshot)
    await db.flush()
    for row in loaded['rows']:
        db.add(
            ReplenishmentLine(
                snapshot_id=snapshot.id,
                source_row=row['source_row'],
                item_code=str(row['item_code']),
                item_name=str(row.get('item_name') or ''),
                category_1=str(row.get('category_1') or ''),
                category_2=str(row.get('category_2') or ''),
                category_3=str(row.get('category_3') or ''),
                status_1=str(row.get('status_1') or ''),
                status_2=str(row.get('status_2') or ''),
                min_stock=row.get('min_stock'),
                repl_moq=row.get('repl_moq'),
                vendor_moq=row.get('vendor_moq'),
                purchase_price=row.get('purchase_price'),
                total_need_qty=row.get('total_need_qty') or 0,
                total_overstock_qty=row.get('total_overstock_qty') or 0,
                supplier_order_qty=row.get('supplier_order_qty') or 0,
                supplier_order_value=row.get('supplier_order_value') or 0,
                weeks_of_stock_total=row.get('weeks_of_stock_total'),
                store_metrics_json=row.get('store_metrics_json') or {},
                raw_json=row.get('raw_json') or {},
            )
        )
    for issue in loaded['issues']:
        db.add(
            ReplenishmentDataQualityIssue(
                snapshot_id=snapshot.id,
                severity=str(issue.get('severity') or 'warning'),
                issue_code=str(issue.get('issue_code') or 'unknown'),
                source_row=issue.get('source_row'),
                item_code=issue.get('item_code'),
                item_name=issue.get('item_name'),
                field_name=issue.get('field_name'),
                source_cell=issue.get('source_cell'),
                raw_value=issue.get('raw_value'),
                message=str(issue.get('message') or ''),
                metadata_json=issue.get('metadata_json') or {},
            )
        )
    await db.flush()
    return snapshot
