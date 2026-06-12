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
AVAILABILITY_STORE_LABELS = {
    'LOGICA': 'Logica',
    'AGD': 'Αγ. Δημ.',
    'KAS': 'Κασσαβέτη',
    'ELL': 'Ελληνικού',
    'SPA': 'Σπατών',
    'PER': 'Περιστερίου',
}
FNR_OUTPUT_COLUMNS = [
    'Κωδικός', 'Περιγραφή', 'Κατηγορία 1', 'Κατηγορία 2', 'Κατηγορία 3', 'Status 1', 'Status 2',
    'Min Stock', 'Repl MOQ', 'Vendor MOQ',
    'MO1 ΚΑΣ', 'MO1 ΑΓΔ', 'MO1 ΠΕΡ', 'MO1 ΕΛΛ', 'MO1 ΣΠΑ', 'MO1 Logica',
    'MO2 ΚΑΣ', 'MO2 ΑΓΔ', 'MO2 ΠΕΡ', 'MO2 ΕΛΛ', 'MO2 ΣΠΑ', 'MO2 Logica',
    'Stock ΚΑΣ', 'Stock ΑΓΔ', 'Stock ΠΕΡ', 'Stock ΕΛΛ', 'Stock ΣΠΑ', 'Stock Logica',
    'Weeks ΚΑΣ', 'Weeks ΑΓΔ', 'Weeks ΠΕΡ', 'Weeks ΕΛΛ', 'Weeks ΣΠΑ', 'Weeks Logica',
    'Expected ΚΑΣ', 'Expected ΑΓΔ', 'Expected ΠΕΡ', 'Expected ΕΛΛ', 'Expected ΣΠΑ', 'Expected Logica',
    'Target ΚΑΣ', 'Target ΑΓΔ', 'Target ΠΕΡ', 'Target ΕΛΛ', 'Target ΣΠΑ', 'Target Logica',
    'Need ΚΑΣ', 'Need ΑΓΔ', 'Need ΠΕΡ', 'Need ΕΛΛ', 'Need ΣΠΑ', 'Need Logica',
    'Overstock ΚΑΣ', 'Overstock ΑΓΔ', 'Overstock ΠΕΡ', 'Overstock ΕΛΛ', 'Overstock ΣΠΑ', 'Overstock Logica',
    'Παρ Προμ', 'Weeks Total', 'Τιμή αγοράς', 'Αξία Παραγγελίας',
]
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
                MAX(COALESCE(NULLIF(di.manual_order_category, ''), NULLIF(di.abc_category, ''), di.replenishment_status_1)) AS status_1,
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
            COALESCE(inventory.status_1, NULLIF(dim_items.manual_order_category, ''), NULLIF(dim_items.abc_category, ''), dim_items.replenishment_status_1) AS status_1,
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


def _fnr_store_code(label: object) -> str:
    # Already-resolved store codes (from the per-tenant warehouse->store map) pass through.
    direct = str(label or '').strip().upper()
    if direct in {'KAS', 'AGD', 'PER', 'ELL', 'SPA', 'LOGICA'}:
        return direct
    text_value = str(label or '').strip().casefold()
    text_value = text_value.translate(str.maketrans({
        'ά': 'α',
        'έ': 'ε',
        'ή': 'η',
        'ί': 'ι',
        'ό': 'ο',
        'ύ': 'υ',
        'ώ': 'ω',
        'ϊ': 'ι',
        'ΐ': 'ι',
        'ϋ': 'υ',
        'ΰ': 'υ',
        'ς': 'σ',
    }))
    compact = re.sub(r'[^a-zα-ω0-9]+', '', text_value)
    if compact in {'10011002', '1002'}:
        return 'KAS'
    if compact in {'10017', '7'}:
        return 'AGD'
    if compact in {'10018000', '8000'}:
        return 'PER'
    if compact in {'10011003', '1003'}:
        return 'ELL'
    if compact in {'10012000', '2000'}:
        return 'SPA'
    if compact in {'10011000', '1000'}:
        return 'LOGICA'
    if any(token in compact for token in ('κασ', 'kass', 'kasav', 'kassav')):
        return 'KAS'
    if any(token in compact for token in ('αγιοσδημητριοσ', 'αγδ', 'agiosdimitrios', 'agd')):
        return 'AGD'
    if any(token in compact for token in ('περιστερι', 'perister', 'per')):
        return 'PER'
    if any(token in compact for token in ('ελληνικο', 'ellin', 'ell')):
        return 'ELL'
    if any(token in compact for token in ('σπατα', 'spata', 'spa')):
        return 'SPA'
    if any(token in compact for token in ('logica', 'λογικα', 'εδρα', 'headquarters', 'hq')):
        return 'LOGICA'
    return ''


def _fnr_allowed(value: str, selected: list[str]) -> bool:
    if not selected:
        return True
    folded = value.casefold()
    return any(item.casefold() == folded for item in selected)


def _fnr_status_code(value: object) -> str:
    text_value = str(value or '').strip().upper()
    if text_value in {'A', 'B', 'C', 'D', 'S'}:
        return text_value
    # Normalize ABC sub-codes from manual_order_category to their base class.
    if len(text_value) >= 2 and text_value[0] in {'A', 'B', 'C', 'D'} and text_value[1:].isdigit():
        return text_value[0]  # D3->D, B1/B2->B, A2/A3->A
    if text_value.isalpha() and text_value.startswith('S') and len(text_value) <= 3:
        return 'S'  # S, SD, SC, SB -> S
    if text_value.startswith('FIRST-A'):
        return 'A'
    if text_value in {'CONS', 'TESTER'}:
        return 'D'
    folded = str(value or '').strip().casefold()
    folded = folded.translate(str.maketrans({
        'ά': 'α',
        'έ': 'ε',
        'ή': 'η',
        'ί': 'ι',
        'ό': 'ο',
        'ύ': 'υ',
        'ώ': 'ω',
        'ϊ': 'ι',
        'ΐ': 'ι',
        'ϋ': 'υ',
        'ΰ': 'υ',
        'ς': 'σ',
    }))
    if not folded or folded in {'χωρισ fnr status', 'χωρισ status', 'χωρισ abc', 'null'}:
        return ''
    if folded in {'τρεχον', 'new launch', 'αναμενομενο', 'παρακαταθηκη'}:
        return 'A'
    if folded in {'check'} or folded.startswith('check to return'):
        return 'B'
    if folded in {'seasonal status'}:
        return 'S'
    if folded in {'μεχρι εξαντλησεωσ', 'χωρισ αποθεμα'}:
        return 'C'
    if folded in {
        'καταργημενο',
        'επιστροφη',
        'ληγμενο',
        'tester',
        'πιστωτικα',
        'store consumables',
        'no agreement',
    }:
        return 'D'
    return 'A'


def _fnr_list(raw: object) -> list[str]:
    return [part.strip() for part in str(raw or '').replace('\n', ',').split(',') if part.strip()]


def _fnr_round(value: float) -> float:
    if abs(value) < 0.000001:
        return 0.0
    return float(round(value, 4))


async def build_fnr_excel_from_facts(
    db: AsyncSession,
    *,
    as_of: date | None = None,
    pharmacies: list[str] | None = None,
    groups: list[str] | None = None,
    category_1: list[str] | None = None,
    category_2: list[str] | None = None,
    category_3: list[str] | None = None,
    suppliers: list[str] | None = None,
    search: str | None = None,
    target_stock_weeks: float = 4.0,
    overstock_weeks: float = 12.0,
    sales_avg_period_1_weeks: int = 4,
    sales_avg_period_2_weeks: int = 12,
    limit: int = 5000,
    store_warehouse_map: dict[str, object] | None = None,
    include_no_order: bool = False,
) -> dict[str, object]:
    """Build the FNR worksheet using the same column logic as the provided Excel."""
    as_of = as_of or date.today()
    period_1 = await build_availability_foundation(
        db,
        as_of=as_of,
        target_stock_weeks=target_stock_weeks,
        overstock_weeks=overstock_weeks,
        sales_window_days=max(int(sales_avg_period_1_weeks or 4), 1) * 7,
        include_detail_rows=True,
        detail_limit=max(limit * len(STORE_CODES), 1000),
        store_warehouse_map=store_warehouse_map,
    )
    period_2_days = max(int(sales_avg_period_2_weeks or 12), 1) * 7
    period_2_start = as_of - timedelta(days=period_2_days - 1)
    period_2_result = await db.execute(
        text(
            """
            SELECT item_code,
                   COALESCE(branch_ext_id, '') AS branch_ext_id,
                   COALESCE(warehouse_ext_id, '') AS warehouse_ext_id,
                   SUM(COALESCE(qty, 0)) AS sales_qty
            FROM fact_sales
            WHERE doc_date >= :period_start
              AND doc_date <= :as_of
              AND COALESCE(item_code, '') <> ''
            GROUP BY item_code, COALESCE(branch_ext_id, ''), COALESCE(warehouse_ext_id, '')
            """
        ),
        {'period_start': period_2_start, 'as_of': as_of},
    )
    _wh_to_store: dict[str, str] = {}
    if store_warehouse_map:
        for _store, _whs in store_warehouse_map.items():
            _st = str(_store or '').strip()
            for _wh in (_whs or []):
                _w = str(_wh or '').strip()
                if _w and _st:
                    _wh_to_store[_w] = _st
    sales2_by_key: dict[tuple[str, str], float] = {}
    for row in period_2_result.mappings().all():
        if _wh_to_store:
            store_code = _wh_to_store.get(str(row.get('warehouse_ext_id') or '').strip(), '')
        else:
            store_code = _fnr_store_code(row.get('branch_ext_id'))
        if not store_code:
            continue
        key = (str(row.get('item_code') or ''), store_code)
        sales2_by_key[key] = sales2_by_key.get(key, 0.0) + _as_float(row.get('sales_qty')) / max(period_2_days / 7.0, 1.0)

    selected_pharmacies = {_fnr_store_code(item) for item in pharmacies or []}
    selected_pharmacies.discard('')
    group_filter = groups or []
    category_1_filter = category_1 or []
    category_2_filter = category_2 or []
    category_3_filter = category_3 or []
    supplier_filter = suppliers or []
    search_filter = str(search or '').strip().casefold()
    items: dict[str, dict[str, object]] = {}
    options = {
        'pharmacies': [STORE_NAMES[code] for code in STORE_CODES],
        'group': set(),
        'category_1': set(),
        'category_2': set(),
        'category_3': set(),
        'suppliers': set(),
    }
    for detail in period_1.get('detail_rows') or []:
        if not isinstance(detail, dict):
            continue
        item_code = str(detail.get('item_code') or '').strip()
        store_code = _fnr_store_code(detail.get('store'))
        if not item_code or not store_code:
            continue
        if selected_pharmacies and store_code not in selected_pharmacies:
            continue
        c1 = str(detail.get('category_1') or detail.get('category') or '').strip()
        c2 = str(detail.get('category_2') or '').strip()
        c3 = str(detail.get('category_3') or '').strip()
        grp = str(detail.get('group') or '').strip()
        supplier = str(detail.get('vendor') or '').strip()
        item_name = str(detail.get('item_name') or item_code)
        if grp:
            options['group'].add(grp)
        options['category_1'].add(c1)
        if c2:
            options['category_2'].add(c2)
        if c3:
            options['category_3'].add(c3)
        if supplier:
            options['suppliers'].add(supplier)
        if group_filter and not _fnr_allowed(grp, group_filter):
            continue
        if not _fnr_allowed(c1, category_1_filter):
            continue
        if category_2_filter and not _fnr_allowed(c2, category_2_filter):
            continue
        if category_3_filter and not _fnr_allowed(c3, category_3_filter):
            continue
        if supplier_filter and not _fnr_allowed(supplier, supplier_filter):
            continue
        if search_filter:
            haystack = ' '.join([item_code, item_name, c1, c2, c3, supplier]).casefold()
            if search_filter not in haystack:
                continue
        item = items.setdefault(
            item_code,
            {
                'item_code': item_code,
                'item_name': item_name,
                'category_1': c1,
                'category_2': c2,
                'category_3': c3,
                'status_1': str(detail.get('abc_category') or '').strip(),
                'status_2': str(detail.get('commercial_status') or '').strip(),
                'supplier': supplier,
                'group': grp,
                'min_stock': max(_as_float(detail.get('min_stock'), 1.0), 0.0),
                'repl_moq': max(_as_float(detail.get('repl_moq'), 1.0), 1.0),
                'vendor_moq': max(_as_float(detail.get('vendor_moq'), 1.0), 1.0),
                'purchase_price': _as_float(detail.get('latest_purchase_price') or detail.get('avg_purchase_price')),
                'stores': {code: {'sales_avg_1': 0.0, 'sales_avg_2': 0.0, 'stock_qty': 0.0, 'expected_qty': 0.0} for code in STORE_CODES},
            },
        )
        store = item['stores'][store_code]  # type: ignore[index]
        store['sales_avg_1'] = _as_float(detail.get('weekly_sales'))
        store['sales_avg_2'] = sales2_by_key.get((item_code, store_code), store['sales_avg_1'])
        store['stock_qty'] = max(_as_float(detail.get('stock_qty')), 0.0)
        # Current BI expected order quantities are item-level vendor incoming quantities.
        # Match the Excel note by assigning vendor expected quantities to Logica only.
        if store_code == 'LOGICA':
            store['expected_qty'] = max(_as_float(detail.get('expected_qty')), 0.0)

    output_rows: list[dict[str, object]] = []
    order_rows: list[dict[str, object]] = []
    for item in items.values():
        stores = item['stores']  # type: ignore[assignment]
        status_1 = _fnr_status_code(item.get('status_1'))
        min_stock = _as_float(item.get('min_stock'), 1.0)
        repl_moq = max(_as_float(item.get('repl_moq'), 1.0), 1.0)
        vendor_moq = max(_as_float(item.get('vendor_moq'), 1.0), 1.0)
        purchase_price = _as_float(item.get('purchase_price'))
        target: dict[str, float] = {}
        need: dict[str, float] = {}
        overstock: dict[str, float] = {}
        weeks: dict[str, float] = {}
        for code in STORE_CODES:
            metric = stores[code]
            sales = max(_as_float(metric.get('sales_avg_1')), _as_float(metric.get('sales_avg_2')))
            stock = max(_as_float(metric.get('stock_qty')), 0.0)
            expected = max(_as_float(metric.get('expected_qty')), 0.0)
            weeks[code] = 0.0 if stock <= 0 else (999.0 if sales <= 0 else stock / sales)
            if code in ('KAS', 'AGD'):
                active = status_1 in {'A', 'B', 'S'}
            elif code in ('PER', 'ELL', 'SPA'):
                active = status_1 in {'A', 'S'}
            else:
                active = status_1 in {'A', 'B', 'S', 'C'}
            target[code] = max(target_stock_weeks * sales, min_stock) if active else 0.0
            if code == 'LOGICA':
                need[code] = max(target[code], repl_moq) if target[code] > 0 else 0.0
            else:
                diff = target[code] - stock - expected
                need[code] = max(diff, repl_moq) if diff > 0 else 0.0
            if code in ('KAS', 'AGD'):
                inactive_overstock = status_1 in {'C', 'D'}
            elif code in ('PER', 'ELL', 'SPA'):
                inactive_overstock = status_1 in {'B', 'C', 'D'}
            else:
                inactive_overstock = status_1 == 'D'
            if inactive_overstock:
                overstock[code] = -stock if stock > 0 else 0.0
            elif code == 'LOGICA':
                overstock[code] = 0.0
            elif need[code] > 0:
                overstock[code] = 0.0
            elif weeks[code] > overstock_weeks:
                overstock[code] = min(target[code] - stock - expected, 0.0)
            else:
                overstock[code] = 0.0
        logica_stock = _as_float(stores['LOGICA'].get('stock_qty'))
        logica_expected = max(_as_float(stores['LOGICA'].get('expected_qty')), 0.0)
        total_need = sum(need.values())
        supplier_order_qty = max(total_need - logica_stock - logica_expected, vendor_moq) if total_need > (logica_stock + logica_expected) else 0.0
        total_sales2 = sum(_as_float(stores[code].get('sales_avg_2')) for code in STORE_CODES)
        total_stock = sum(_as_float(stores[code].get('stock_qty')) for code in STORE_CODES)
        weeks_total = 999.0 if total_sales2 <= 0 and total_stock > 0 else (total_stock / total_sales2 if total_sales2 > 0 else 0.0)
        row = {
            'item_code': item['item_code'],
            'item_name': item['item_name'],
            'category_1': item.get('category_1') or '',
            'category_2': item.get('category_2') or '',
            'category_3': item.get('category_3') or '',
            'status_1': status_1 or '',
            'status_2': item.get('status_2') or '',
            'supplier': item.get('supplier') or '',
            'group': item.get('group') or '',
            'min_stock': min_stock,
            'repl_moq': repl_moq,
            'vendor_moq': vendor_moq,
            'stores': stores,
            'stock_weeks': weeks,
            'target_stock': target,
            'need_qty': need,
            'overstock_qty': overstock,
            'supplier_order_qty': _fnr_round(supplier_order_qty),
            'weeks_of_stock_total': _fnr_round(weeks_total),
            'purchase_price': _fnr_round(purchase_price),
            'supplier_order_value': _fnr_round(supplier_order_qty * purchase_price),
            'total_need_qty': _fnr_round(total_need),
            'total_overstock_qty': _fnr_round(sum(overstock.values())),
        }
        output_rows.append(row)
        if supplier_order_qty > 0:
            order_rows.append(row)
    if include_no_order:
        # Toggle: surface every item, biggest overstock first (total_overstock_qty is <= 0),
        # then the order items by descending order quantity. Lets the user review overstock.
        output_rows = sorted(
            output_rows,
            key=lambda row: (_as_float(row.get('total_overstock_qty')), -_as_float(row.get('supplier_order_qty')), str(row.get('item_name') or '')),
        )[:limit]
    else:
        output_rows = sorted(output_rows, key=lambda row: (-_as_float(row.get('supplier_order_qty')), str(row.get('item_name') or '')))[:limit]
    order_rows = sorted(order_rows, key=lambda row: (-_as_float(row.get('supplier_order_qty')), str(row.get('item_name') or '')))
    expected_detail_result = await db.execute(
        text(
            """
            SELECT
                fso.doc_date,
                COALESCE(fso.document_no, fso.document_id, fso.external_id) AS document_no,
                COALESCE(fso.supplier_name, ds.name, ds_ext.name, fso.supplier_ext_id, 'Χωρίς προμηθευτή') AS supplier_name,
                COALESCE(di.external_id, fso.item_code) AS item_code,
                COALESCE(fso.item_name, di.name, fso.item_code) AS item_name,
                COALESCE(di.category_1, '') AS category_1,
                COALESCE(di.category_2, '') AS category_2,
                COALESCE(di.category_3, '') AS category_3,
                COALESCE(fso.order_qty, 0) AS order_qty,
                COALESCE(fso.covered_qty, 0) AS covered_qty,
                COALESCE(fso.cancelled_qty, 0) AS cancelled_qty,
                GREATEST(
                    COALESCE(fso.order_qty, 0) - COALESCE(fso.covered_qty, 0) - COALESCE(fso.cancelled_qty, 0),
                    0
                ) AS expected_qty,
                COALESCE(fso.order_status, 'open') AS order_status,
                COALESCE(fso.has_transformation, false) AS has_transformation
            FROM fact_supplier_orders fso
            LEFT JOIN dim_items di ON di.id = fso.item_id OR di.external_id = fso.item_code
            LEFT JOIN dim_suppliers ds ON ds.id = fso.supplier_id
            LEFT JOIN dim_suppliers ds_ext ON ds_ext.external_id = fso.supplier_ext_id
            WHERE COALESCE(fso.order_status, 'open') = 'open'
              AND COALESCE(fso.has_transformation, false) = false
              AND COALESCE(di.external_id, fso.item_code, '') <> ''
              AND GREATEST(
                    COALESCE(fso.order_qty, 0) - COALESCE(fso.covered_qty, 0) - COALESCE(fso.cancelled_qty, 0),
                    0
                  ) > 0
            ORDER BY fso.doc_date DESC, COALESCE(fso.document_no, fso.document_id, fso.external_id), COALESCE(fso.item_name, di.name, fso.item_code)
            """
        )
    )
    expected_order_rows: list[dict[str, object]] = []
    for row in expected_detail_result.mappings().all():
        c1 = str(row.get('category_1') or '').strip()
        c2 = str(row.get('category_2') or '').strip()
        c3 = str(row.get('category_3') or '').strip()
        supplier = str(row.get('supplier_name') or '').strip()
        item_code = str(row.get('item_code') or '').strip()
        item_name = str(row.get('item_name') or item_code).strip()
        if not _fnr_allowed(c1, category_1_filter):
            continue
        if category_2_filter and not _fnr_allowed(c2, category_2_filter):
            continue
        if category_3_filter and not _fnr_allowed(c3, category_3_filter):
            continue
        if supplier_filter and not _fnr_allowed(supplier, supplier_filter):
            continue
        if search_filter:
            haystack = ' '.join([item_code, item_name, c1, c2, c3, supplier, str(row.get('document_no') or '')]).casefold()
            if search_filter not in haystack:
                continue
        expected_order_rows.append(
            {
                'doc_date': row.get('doc_date').isoformat() if hasattr(row.get('doc_date'), 'isoformat') else str(row.get('doc_date') or ''),
                'document_no': row.get('document_no') or '',
                'supplier_name': supplier,
                'item_code': item_code,
                'item_name': item_name,
                'category_1': c1,
                'order_qty': _fnr_round(row.get('order_qty')),
                'covered_qty': _fnr_round(row.get('covered_qty')),
                'cancelled_qty': _fnr_round(row.get('cancelled_qty')),
                'expected_qty': _fnr_round(row.get('expected_qty')),
            }
        )
    summary = {
        'source': 'facts',
        'source_label': 'Live BI facts',
        'as_of': (as_of or date.today()).isoformat(),
        'rows_count': len(output_rows),
        'order_rows_count': len(order_rows),
        'total_supplier_order_qty': sum(_as_float(row.get('supplier_order_qty')) for row in order_rows),
        'total_supplier_order_value': sum(_as_float(row.get('supplier_order_value')) for row in order_rows),
        'products_with_need': sum(1 for row in output_rows if _as_float(row.get('total_need_qty')) > 0),
        'products_with_overstock': sum(1 for row in output_rows if _as_float(row.get('total_overstock_qty')) < 0),
        'expected_order_rows_count': len(expected_order_rows),
        'expected_order_qty': sum(_as_float(row.get('expected_qty')) for row in expected_order_rows),
        'include_no_order': bool(include_no_order),
    }
    return {
        'parameters': {
            'target_stock_weeks': target_stock_weeks,
            'overstock_weeks': overstock_weeks,
            'sales_avg_period_1_weeks': sales_avg_period_1_weeks,
            'sales_avg_period_2_weeks': sales_avg_period_2_weeks,
        },
        'summary': summary,
        'rows': output_rows,
        'order_rows': order_rows,
        'expected_order_rows': expected_order_rows,
        'columns': FNR_OUTPUT_COLUMNS,
        'store_codes': STORE_CODES,
        'store_names': STORE_NAMES,
        'options': {key: sorted(value) if isinstance(value, set) else value for key, value in options.items()},
        'filters': {
            'pharmacies': pharmacies or [],
            'category_1': category_1 or [],
            'category_2': category_2 or [],
            'category_3': category_3 or [],
            'suppliers': suppliers or [],
            'search': search or '',
        },
    }


def _availability_filter_list(raw: object) -> list[str]:
    return [part.strip() for part in str(raw or '').replace('\n', ',').split(',') if part.strip()]


def _availability_match(value: str, selected: list[str]) -> bool:
    if not selected:
        return True
    folded = value.casefold()
    return any(item.casefold() == folded for item in selected)


def _availability_step_start(value: date, step: str) -> date:
    if step == 'week':
        return value - timedelta(days=value.weekday())
    if step == 'quarter':
        month = ((value.month - 1) // 3) * 3 + 1
        return date(value.year, month, 1)
    return date(value.year, value.month, 1)


def _availability_add_step(value: date, step: str) -> date:
    if step == 'week':
        return value + timedelta(days=7)
    if step == 'quarter':
        month = value.month + 3
    else:
        month = value.month + 1
    year = value.year + (month - 1) // 12
    month = ((month - 1) % 12) + 1
    return date(year, month, 1)


def _availability_previous_year(value: date) -> date:
    try:
        return value.replace(year=value.year - 1)
    except ValueError:
        return value.replace(year=value.year - 1, day=28)


def _availability_periods(start: date, end: date, step: str) -> list[tuple[str, date, date]]:
    out: list[tuple[str, date, date]] = []
    cursor = _availability_step_start(start, step)
    month_labels = ['Ιαν', 'Φεβ', 'Μαρ', 'Απρ', 'Μαι', 'Ιουν', 'Ιουλ', 'Αυγ', 'Σεπ', 'Οκτ', 'Νοε', 'Δεκ']
    while cursor <= end:
        next_cursor = _availability_add_step(cursor, step)
        period_end = min(next_cursor - timedelta(days=1), end)
        period_start = max(cursor, start)
        if step == 'week':
            label = f'W{period_start.isocalendar().week}'
        elif step == 'quarter':
            label = f'Q{((period_start.month - 1) // 3) + 1}'
        else:
            label = month_labels[period_start.month - 1]
        out.append((label, period_start, period_end))
        cursor = next_cursor
    return out


def _availability_group_sort_key(row: dict[str, object]) -> tuple[int, str]:
    status = str(row.get('status_abcd') or '')
    order = {'A': 1, 'B': 2, 'C': 3, 'D': 4, 'S': 5, '': 99}
    return (order.get(status, 90), str(row.get('commercial_status') or ''))


async def build_availability_brief_from_facts(
    db: AsyncSession,
    *,
    pharmacies: list[str] | None = None,
    category_1: list[str] | None = None,
    category_2: list[str] | None = None,
    category_3: list[str] | None = None,
    suppliers: list[str] | None = None,
    group: list[str] | None = None,
    status_abcd: list[str] | None = None,
    commercial_status: list[str] | None = None,
    period_from: date | None = None,
    period_to: date | None = None,
    stock_date_1: date | None = None,
    stock_date_2: date | None = None,
    step: str = 'month',
) -> dict[str, object]:
    """Build the Availability brief worksheets from BI facts, following the Excel structure."""
    today = date.today()
    period_to = period_to or today
    period_from = period_from or date(period_to.year, max(period_to.month - 3, 1), 1)
    stock_date_1 = stock_date_1 or period_to
    stock_date_2 = stock_date_2 or date(period_to.year - 1, 12, 31)
    period_days = max((period_to - period_from).days + 1, 1)
    selected_pharmacies = {_fnr_store_code(item) for item in pharmacies or []}
    selected_pharmacies.discard('')
    category_1_filter = category_1 or []
    category_2_filter = category_2 or []
    category_3_filter = category_3 or []
    supplier_filter = suppliers or []
    group_filter = group or []
    status_abcd_filter = [str(item).strip().upper() for item in status_abcd or [] if str(item).strip()]
    commercial_filter = commercial_status or []
    sql = text(
        """
        WITH stock_snapshot_quality AS (
            SELECT snapshot_date, SUM(COALESCE(stock_value, 0)) AS stock_value
            FROM agg_stock_aging
            GROUP BY snapshot_date
        ),
        stock1_date AS (
            SELECT COALESCE(
                (SELECT MAX(snapshot_date) FROM stock_snapshot_quality WHERE snapshot_date <= :stock_date_1 AND stock_value > 0),
                (SELECT MAX(snapshot_date) FROM stock_snapshot_quality WHERE stock_value > 0),
                (SELECT MAX(snapshot_date) FROM stock_snapshot_quality WHERE snapshot_date <= :stock_date_1),
                (SELECT MAX(snapshot_date) FROM stock_snapshot_quality)
            ) AS snapshot_date
        ),
        stock2_date AS (
            SELECT COALESCE(
                (SELECT MAX(snapshot_date) FROM stock_snapshot_quality WHERE snapshot_date <= :stock_date_2 AND stock_value > 0),
                (SELECT MAX(snapshot_date) FROM stock_snapshot_quality WHERE stock_value > 0),
                (SELECT MAX(snapshot_date) FROM stock_snapshot_quality WHERE snapshot_date <= :stock_date_2),
                (SELECT MAX(snapshot_date) FROM stock_snapshot_quality)
            ) AS snapshot_date
        ),
        stock1 AS (
            SELECT asa.item_external_id AS item_code, COALESCE(asa.branch_ext_id, '') AS branch_ext_id,
                   SUM(COALESCE(asa.qty_on_hand, 0)) AS qty, SUM(COALESCE(asa.stock_value, 0)) AS value
            FROM agg_stock_aging asa
            JOIN stock1_date ON asa.snapshot_date = stock1_date.snapshot_date
            GROUP BY asa.item_external_id, COALESCE(asa.branch_ext_id, '')
        ),
        stock1_total AS (
            SELECT item_code, SUM(qty) AS qty, SUM(value) AS value
            FROM stock1
            GROUP BY item_code
        ),
        stock1_branch AS (
            SELECT item_code, jsonb_object_agg(branch_ext_id, qty) AS stock_by_branch
            FROM stock1
            GROUP BY item_code
        ),
        stock2 AS (
            SELECT asa.item_external_id AS item_code, COALESCE(asa.branch_ext_id, '') AS branch_ext_id,
                   SUM(COALESCE(asa.qty_on_hand, 0)) AS qty, SUM(COALESCE(asa.stock_value, 0)) AS value
            FROM agg_stock_aging asa
            JOIN stock2_date ON asa.snapshot_date = stock2_date.snapshot_date
            GROUP BY asa.item_external_id, COALESCE(asa.branch_ext_id, '')
        ),
        stock2_total AS (
            SELECT item_code, SUM(qty) AS qty, SUM(value) AS value
            FROM stock2
            GROUP BY item_code
        ),
        sales AS (
            SELECT item_code, COALESCE(branch_ext_id, '') AS branch_ext_id,
                   SUM(COALESCE(qty, 0)) AS qty,
                   SUM(COALESCE(net_value, 0)) AS value,
                   SUM(COALESCE(profit_amount, 0)) AS profit
            FROM fact_sales
            WHERE doc_date >= :period_from AND doc_date <= :period_to AND COALESCE(item_code, '') <> ''
            GROUP BY item_code, COALESCE(branch_ext_id, '')
        ),
        sales_total AS (
            SELECT item_code, SUM(qty) AS qty, SUM(value) AS value, SUM(profit) AS profit
            FROM sales
            GROUP BY item_code
        ),
        purchases AS (
            SELECT item_code,
                   SUM(COALESCE(qty, 0)) AS qty,
                   SUM(COALESCE(net_value, 0)) AS value
            FROM fact_purchases
            WHERE doc_date >= :period_from AND doc_date <= :period_to AND COALESCE(item_code, '') <> ''
            GROUP BY item_code
        ),
        supplier_rank AS (
            SELECT *
            FROM (
                SELECT
                    fp.item_code,
                    COALESCE(ds.name, ds_ext.name, fp.supplier_ext_id, 'Χωρίς προμηθευτή') AS supplier_name,
                    ROW_NUMBER() OVER (PARTITION BY fp.item_code ORDER BY ABS(SUM(COALESCE(fp.net_value, 0))) DESC) AS rn
                FROM fact_purchases fp
                LEFT JOIN dim_suppliers ds ON ds.id = fp.supplier_id
                LEFT JOIN dim_suppliers ds_ext ON ds_ext.external_id = fp.supplier_ext_id
                WHERE fp.doc_date >= :purchase_rank_start
                  AND fp.doc_date <= :period_to
                  AND COALESCE(fp.item_code, '') <> ''
                GROUP BY fp.item_code, COALESCE(ds.name, ds_ext.name, fp.supplier_ext_id, 'Χωρίς προμηθευτή')
            ) ranked
            WHERE rn = 1
        ),
        scope AS (
            SELECT item_code FROM stock1
            UNION SELECT item_code FROM stock2
            UNION SELECT item_code FROM sales_total
            UNION SELECT item_code FROM purchases
        )
        SELECT
            scope.item_code,
            COALESCE(di.name, scope.item_code) AS item_name,
            COALESCE(NULLIF(di.category_1, ''), 'Χωρίς κατηγορία') AS category_1,
            COALESCE(NULLIF(di.category_2, ''), '') AS category_2,
            COALESCE(NULLIF(di.category_3, ''), '') AS category_3,
            COALESCE(NULLIF(di.replenishment_status_1, ''), NULLIF(di.commercial_status, ''), '') AS raw_status,
            COALESCE(NULLIF(di.commercial_status, ''), 'Χωρίς status') AS commercial_status,
            COALESCE(NULLIF(supplier_rank.supplier_name, ''), 'Χωρίς προμηθευτή') AS supplier_name,
            COALESCE(NULLIF(dg.name, ''), 'Χωρίς ομάδα') AS group_name,
            COALESCE(purchases.qty, 0) AS purchase_qty,
            COALESCE(purchases.value, 0) AS purchase_value,
            COALESCE(sales_total.qty, 0) AS sales_qty,
            COALESCE(sales_total.value, 0) AS sales_value,
            COALESCE(sales_total.profit, 0) AS sales_profit,
            COALESCE(stock1_total.qty, 0) AS stock1_qty,
            COALESCE(stock1_total.value, 0) AS stock1_value,
            COALESCE(stock2_total.qty, 0) AS stock2_qty,
            COALESCE(stock2_total.value, 0) AS stock2_value,
            stock1_branch.stock_by_branch AS stock_by_branch
        FROM scope
        LEFT JOIN dim_items di ON di.external_id = scope.item_code
        LEFT JOIN dim_groups dg ON dg.id = di.group_id
        LEFT JOIN supplier_rank ON supplier_rank.item_code = scope.item_code
        LEFT JOIN purchases ON purchases.item_code = scope.item_code
        LEFT JOIN sales_total ON sales_total.item_code = scope.item_code
        LEFT JOIN stock1_total ON stock1_total.item_code = scope.item_code
        LEFT JOIN stock1_branch ON stock1_branch.item_code = scope.item_code
        LEFT JOIN stock2_total ON stock2_total.item_code = scope.item_code
        WHERE COALESCE(di.softone_sotype, 51) = 51 AND COALESCE(di.is_active_source, true) = true
        """
    )
    rows = (await db.execute(
        sql,
        {
            'period_from': period_from,
            'period_to': period_to,
            'stock_date_1': stock_date_1,
            'stock_date_2': stock_date_2,
            'purchase_rank_start': period_to - timedelta(days=365),
        },
    )).mappings().all()
    groups: dict[tuple[str, str], dict[str, object]] = {}
    vendor_location: dict[tuple[str, str, str, str], dict[str, object]] = {}
    selected_item_codes: list[str] = []
    options = {'pharmacies': [STORE_NAMES[code] for code in STORE_CODES], 'category_1': set(), 'category_2': set(), 'category_3': set(), 'suppliers': set(), 'group': set(), 'status_abcd': set(), 'commercial_status': set()}
    total_sku = 0
    for row in rows:
        status_code = _fnr_status_code(row.get('raw_status'))
        comm = str(row.get('commercial_status') or 'Χωρίς status')
        c1 = str(row.get('category_1') or 'Χωρίς κατηγορία')
        c2 = str(row.get('category_2') or '')
        c3 = str(row.get('category_3') or '')
        supplier = str(row.get('supplier_name') or 'Χωρίς προμηθευτή')
        grp = str(row.get('group_name') or 'Χωρίς ομάδα')
        options['category_1'].add(c1)
        if c2:
            options['category_2'].add(c2)
        if c3:
            options['category_3'].add(c3)
        options['suppliers'].add(supplier)
        options['group'].add(grp)
        if status_code:
            options['status_abcd'].add(status_code)
        options['commercial_status'].add(comm)
        if not _availability_match(c1, category_1_filter) or not _availability_match(c2, category_2_filter) or not _availability_match(c3, category_3_filter):
            continue
        if not _availability_match(supplier, supplier_filter) or not _availability_match(comm, commercial_filter):
            continue
        if not _availability_match(grp, group_filter):
            continue
        if status_abcd_filter and status_code not in status_abcd_filter:
            continue
        stock_by_branch = row.get('stock_by_branch') if isinstance(row.get('stock_by_branch'), dict) else {}
        store_stock = {code: 0.0 for code in STORE_CODES}
        for raw_branch, qty in stock_by_branch.items():
            code = _fnr_store_code(raw_branch)
            if code:
                store_stock[code] += _as_float(qty)
        if selected_pharmacies and not any(store_stock.get(code, 0) > 0 for code in selected_pharmacies):
            continue
        selected_item_codes.append(str(row.get('item_code') or ''))
        key = (status_code, comm)
        group = groups.setdefault(
            key,
            {
                'status_abcd': status_code,
                'commercial_status': comm,
                'sku_count': 0,
                'sku_live_online': 0,
                'sales_units': 0.0,
                'sales_value': 0.0,
                'sales_profit': 0.0,
                'purchase_units': 0.0,
                'purchase_value': 0.0,
                'stock_units_1': 0.0,
                'stock_value_1': 0.0,
                'stock_units_2': 0.0,
                'stock_value_2': 0.0,
                'availability_counts': {code: 0 for code in STORE_CODES},
            },
        )
        group['sku_count'] = int(group['sku_count']) + 1
        total_sku += 1
        stock1_qty = _as_float(row.get('stock1_qty'))
        if stock1_qty >= 1:
            group['sku_live_online'] = int(group['sku_live_online']) + 1
        for code, qty in store_stock.items():
            if qty >= 1:
                group['availability_counts'][code] = int(group['availability_counts'][code]) + 1  # type: ignore[index]
        for target_key, source_key in (
            ('sales_units', 'sales_qty'), ('sales_value', 'sales_value'), ('sales_profit', 'sales_profit'),
            ('purchase_units', 'purchase_qty'), ('purchase_value', 'purchase_value'),
            ('stock_units_1', 'stock1_qty'), ('stock_value_1', 'stock1_value'),
            ('stock_units_2', 'stock2_qty'), ('stock_value_2', 'stock2_value'),
        ):
            group[target_key] = _as_float(group.get(target_key)) + _as_float(row.get(source_key))
        for code in STORE_CODES:
            avail = 1.0 if store_stock.get(code, 0.0) >= 1 else 0.0
            rec_key = (supplier, code, status_code, comm)
            rec = vendor_location.setdefault(rec_key, {'vendor': supplier, 'location': AVAILABILITY_STORE_LABELS.get(code, code), 'status_abcd': status_code, 'commercial_status': comm, 'sku_count': 0, 'available': 0, 'sales_value': 0.0})
            rec['sku_count'] = int(rec['sku_count']) + 1
            rec['available'] = int(rec['available']) + (1 if avail else 0)
            rec['sales_value'] = _as_float(rec.get('sales_value')) + _as_float(row.get('sales_value'))
    table_rows: list[dict[str, object]] = []
    total = {'status_abcd': 'Grand Total', 'commercial_status': '', 'sku_count': 0, 'sku_live_online': 0, 'sales_units': 0.0, 'sales_value': 0.0, 'sales_profit': 0.0, 'purchase_units': 0.0, 'purchase_value': 0.0, 'stock_units_1': 0.0, 'stock_value_1': 0.0, 'stock_units_2': 0.0, 'stock_value_2': 0.0, 'availability_counts': {code: 0 for code in STORE_CODES}}
    for group in groups.values():
        sku_count = max(int(group.get('sku_count') or 0), 1)
        sales_units = _as_float(group.get('sales_units'))
        row = dict(group)
        row['web_availability'] = int(group.get('sku_live_online') or 0) / sku_count
        row['margin_pct'] = _as_float(group.get('sales_profit')) / _as_float(group.get('sales_value')) if _as_float(group.get('sales_value')) else 0.0
        row['dio'] = _as_float(group.get('stock_units_1')) / (sales_units / period_days) if sales_units else '-'
        row['availability'] = {code: int(group['availability_counts'].get(code, 0)) / sku_count for code in STORE_CODES}  # type: ignore[index]
        table_rows.append(row)
        for key in total:
            if key == 'availability_counts':
                for code in STORE_CODES:
                    total['availability_counts'][code] = int(total['availability_counts'][code]) + int(group['availability_counts'].get(code, 0))  # type: ignore[index]
            elif isinstance(total[key], (int, float)):
                total[key] = total[key] + group.get(key, 0)  # type: ignore[operator]
    total_sku_safe = max(int(total['sku_count'] or 0), 1)
    total['web_availability'] = int(total['sku_live_online'] or 0) / total_sku_safe
    total['margin_pct'] = _as_float(total.get('sales_profit')) / _as_float(total.get('sales_value')) if _as_float(total.get('sales_value')) else 0.0
    total['dio'] = _as_float(total.get('stock_units_1')) / (_as_float(total.get('sales_units')) / period_days) if _as_float(total.get('sales_units')) else '-'
    total['availability'] = {code: int(total['availability_counts'].get(code, 0)) / total_sku_safe for code in STORE_CODES}  # type: ignore[index]
    table_rows = sorted(table_rows, key=_availability_group_sort_key)
    table_rows.append(total)
    recommendations = []
    for rec in vendor_location.values():
        sku_count = max(int(rec.get('sku_count') or 0), 1)
        cur = int(rec.get('available') or 0) / sku_count
        target = 1.0
        if cur >= 0.98 or _as_float(rec.get('sales_value')) <= 0:
            continue
        recommendations.append({
            'action': 'Improve availability',
            'status_abcd': rec.get('status_abcd') or '',
            'commercial_status': rec.get('commercial_status') or '',
            'vendor': rec.get('vendor') or 'Χωρίς προμηθευτή',
            'location': rec.get('location') or '',
            'cur_availability': cur,
            'target_availability': target,
            'monthly_revenue_potential': (target - cur) * _as_float(rec.get('sales_value')) / max(period_days / 30.0, 1.0),
        })
    recommendations = sorted(recommendations, key=lambda item: -_as_float(item.get('monthly_revenue_potential')))[:50]
    trend_periods = _availability_periods(period_from, period_to, step if step in {'week', 'month', 'quarter'} else 'month')[:18]
    trend_labels = [label for label, _, _ in trend_periods]
    series_seed = [('TOTAL', 'Total'), *[(code, AVAILABILITY_STORE_LABELS.get(code, code)) for code in STORE_CODES]]
    trend_series = {code: {'name': label, 'values': []} for code, label in series_seed}
    correlation_series = {code: {'name': label, 'availability': [], 'sales_vs_py': []} for code, label in series_seed}
    if selected_item_codes:
        trend_sql = text(
            """
            WITH latest_stock AS (
                SELECT MAX(snapshot_date) AS snapshot_date
                FROM agg_stock_aging
                WHERE snapshot_date <= :period_end
            ),
            stock AS (
                SELECT COALESCE(asa.branch_ext_id, '') AS branch_ext_id,
                       COUNT(DISTINCT asa.item_external_id) AS available_count
                FROM agg_stock_aging asa
                JOIN latest_stock ON asa.snapshot_date = latest_stock.snapshot_date
                WHERE asa.item_external_id = ANY(:item_codes)
                  AND COALESCE(asa.qty_on_hand, 0) >= 1
                GROUP BY COALESCE(asa.branch_ext_id, '')
            ),
            sales_current AS (
                SELECT COALESCE(branch_ext_id, '') AS branch_ext_id, SUM(COALESCE(net_value, 0)) AS value
                FROM fact_sales
                WHERE doc_date >= :period_start AND doc_date <= :period_end
                  AND item_code = ANY(:item_codes)
                GROUP BY COALESCE(branch_ext_id, '')
            ),
            sales_py AS (
                SELECT COALESCE(branch_ext_id, '') AS branch_ext_id, SUM(COALESCE(net_value, 0)) AS value
                FROM fact_sales
                WHERE doc_date >= :py_start AND doc_date <= :py_end
                  AND item_code = ANY(:item_codes)
                GROUP BY COALESCE(branch_ext_id, '')
            )
            SELECT 'stock' AS metric, branch_ext_id, available_count::numeric AS value FROM stock
            UNION ALL
            SELECT 'sales_current' AS metric, branch_ext_id, value FROM sales_current
            UNION ALL
            SELECT 'sales_py' AS metric, branch_ext_id, value FROM sales_py
            """
        )
        denominator = max(len(set(selected_item_codes)), 1)
        for _label, start_date, end_date in trend_periods:
            py_start = _availability_previous_year(start_date)
            py_end = _availability_previous_year(end_date)
            result = await db.execute(
                trend_sql,
                {
                    'period_start': start_date,
                    'period_end': end_date,
                    'py_start': py_start,
                    'py_end': py_end,
                    'item_codes': list(set(selected_item_codes)),
                },
            )
            stock_counts = {code: 0.0 for code in STORE_CODES}
            sales_current = {code: 0.0 for code in STORE_CODES}
            sales_py = {code: 0.0 for code in STORE_CODES}
            for trend_row in result.mappings().all():
                code = _fnr_store_code(trend_row.get('branch_ext_id'))
                if code not in STORE_CODES:
                    continue
                metric = str(trend_row.get('metric') or '')
                if metric == 'stock':
                    stock_counts[code] += _as_float(trend_row.get('value'))
                elif metric == 'sales_current':
                    sales_current[code] += _as_float(trend_row.get('value'))
                elif metric == 'sales_py':
                    sales_py[code] += _as_float(trend_row.get('value'))
            total_available = 0.0
            total_current = 0.0
            total_py = 0.0
            for code in STORE_CODES:
                availability_value = stock_counts[code] / denominator
                current_value = sales_current[code]
                previous_value = sales_py[code]
                sales_delta = ((current_value - previous_value) / abs(previous_value)) if previous_value else (1.0 if current_value > 0 else 0.0)
                trend_series[code]['values'].append(_fnr_round(availability_value))
                correlation_series[code]['availability'].append(_fnr_round(availability_value))
                correlation_series[code]['sales_vs_py'].append(_fnr_round(sales_delta))
                total_available += stock_counts[code]
                total_current += current_value
                total_py += previous_value
            total_availability = min(total_available / denominator, 1.0)
            total_delta = ((total_current - total_py) / abs(total_py)) if total_py else (1.0 if total_current > 0 else 0.0)
            trend_series['TOTAL']['values'].append(_fnr_round(total_availability))
            correlation_series['TOTAL']['availability'].append(_fnr_round(total_availability))
            correlation_series['TOTAL']['sales_vs_py'].append(_fnr_round(total_delta))
    else:
        for code, _label in series_seed:
            trend_series[code]['values'] = [0.0 for _ in trend_periods]
            correlation_series[code]['availability'] = [0.0 for _ in trend_periods]
            correlation_series[code]['sales_vs_py'] = [0.0 for _ in trend_periods]
    trends = {'periods': trend_labels, 'series': list(trend_series.values())}
    correlation_items = list(correlation_series.values())
    correlation = {
        'periods': trend_labels,
        'series': correlation_items,
        'availability': correlation_items[0]['availability'] if correlation_items else [],
        'sales_vs_py': correlation_items[0]['sales_vs_py'] if correlation_items else [],
    }
    return {
        'parameters': {'period_from': period_from.isoformat(), 'period_to': period_to.isoformat(), 'stock_date_1': stock_date_1.isoformat(), 'stock_date_2': stock_date_2.isoformat(), 'step': step},
        'filters': {'pharmacies': pharmacies or [], 'category_1': category_1 or [], 'category_2': category_2 or [], 'category_3': category_3 or [], 'suppliers': suppliers or [], 'group': group_filter, 'status_abcd': status_abcd or [], 'commercial_status': commercial_status or []},
        'options': {key: sorted(value) if isinstance(value, set) else value for key, value in options.items()},
        'store_codes': STORE_CODES,
        'store_labels': AVAILABILITY_STORE_LABELS,
        'table_rows': table_rows,
        'trends': trends,
        'correlation': correlation,
        'recommendations': recommendations,
        'summary': {'rows_count': len(table_rows), 'sku_count': total.get('sku_count', 0), 'sku_live_online': total.get('sku_live_online', 0), 'web_availability': total.get('web_availability', 0), 'sales_value': total.get('sales_value', 0), 'stock_value_1': total.get('stock_value_1', 0), 'recommendations_count': len(recommendations)},
    }


async def build_destocking_brief_from_facts(
    db: AsyncSession,
    *,
    pharmacies: list[str] | None = None,
    category_1: list[str] | None = None,
    category_2: list[str] | None = None,
    category_3: list[str] | None = None,
    suppliers: list[str] | None = None,
    group: list[str] | None = None,
    status_abcd: list[str] | None = None,
    commercial_status: list[str] | None = None,
    period_from: date | None = None,
    period_to: date | None = None,
    stock_date_1: date | None = None,
    stock_date_2: date | None = None,
    threshold_weeks: float = 8.0,
    step: str = 'month',
) -> dict[str, object]:
    """Build the Destocking brief worksheets from BI facts, following the Excel structure."""
    today = date.today()
    period_to = period_to or today
    period_from = period_from or date(period_to.year, max(period_to.month - 3, 1), 1)
    stock_date_1 = stock_date_1 or period_to
    stock_date_2 = stock_date_2 or date(period_to.year - 1, 12, 31)
    period_days = max((period_to - period_from).days + 1, 1)
    selected_pharmacies = {_fnr_store_code(item) for item in pharmacies or []}
    selected_pharmacies.discard('')
    status_filter = [str(item).strip().upper() for item in status_abcd or [] if str(item).strip()]
    group_filter = group or []
    sql = text(
        """
        WITH stock_snapshot_quality AS (
            SELECT snapshot_date, SUM(COALESCE(stock_value, 0)) AS stock_value
            FROM agg_stock_aging
            GROUP BY snapshot_date
        ),
        stock1_date AS (
            SELECT COALESCE(
                (SELECT MAX(snapshot_date) FROM stock_snapshot_quality WHERE snapshot_date <= :stock_date_1 AND stock_value > 0),
                (SELECT MAX(snapshot_date) FROM stock_snapshot_quality WHERE stock_value > 0),
                (SELECT MAX(snapshot_date) FROM stock_snapshot_quality WHERE snapshot_date <= :stock_date_1),
                (SELECT MAX(snapshot_date) FROM stock_snapshot_quality)
            ) AS snapshot_date
        ),
        stock2_date AS (
            SELECT COALESCE(
                (SELECT MAX(snapshot_date) FROM stock_snapshot_quality WHERE snapshot_date <= :stock_date_2 AND stock_value > 0),
                (SELECT MAX(snapshot_date) FROM stock_snapshot_quality WHERE stock_value > 0),
                (SELECT MAX(snapshot_date) FROM stock_snapshot_quality WHERE snapshot_date <= :stock_date_2),
                (SELECT MAX(snapshot_date) FROM stock_snapshot_quality)
            ) AS snapshot_date
        ),
        stock1 AS (
            SELECT asa.item_external_id AS item_code, COALESCE(asa.branch_ext_id, '') AS branch_ext_id,
                   SUM(COALESCE(asa.qty_on_hand, 0)) AS qty, SUM(COALESCE(asa.stock_value, 0)) AS value
            FROM agg_stock_aging asa JOIN stock1_date ON asa.snapshot_date = stock1_date.snapshot_date
            GROUP BY asa.item_external_id, COALESCE(asa.branch_ext_id, '')
        ),
        stock1_total AS (
            SELECT item_code, SUM(qty) AS qty, SUM(value) AS value FROM stock1 GROUP BY item_code
        ),
        stock1_branch AS (
            SELECT item_code, jsonb_object_agg(branch_ext_id, jsonb_build_object('qty', qty, 'value', value)) AS stock_by_branch
            FROM stock1 GROUP BY item_code
        ),
        stock2 AS (
            SELECT asa.item_external_id AS item_code, SUM(COALESCE(asa.qty_on_hand, 0)) AS qty, SUM(COALESCE(asa.stock_value, 0)) AS value
            FROM agg_stock_aging asa JOIN stock2_date ON asa.snapshot_date = stock2_date.snapshot_date
            GROUP BY asa.item_external_id
        ),
        sales AS (
            SELECT item_code, COALESCE(branch_ext_id, '') AS branch_ext_id,
                   SUM(COALESCE(qty, 0)) AS qty,
                   SUM(COALESCE(net_value, 0)) AS value,
                   SUM(COALESCE(profit_amount, 0)) AS profit
            FROM fact_sales
            WHERE doc_date >= :period_from AND doc_date <= :period_to AND COALESCE(item_code, '') <> ''
            GROUP BY item_code, COALESCE(branch_ext_id, '')
        ),
        sales_total AS (
            SELECT item_code, SUM(qty) AS qty, SUM(value) AS value, SUM(profit) AS profit FROM sales GROUP BY item_code
        ),
        sales_branch AS (
            SELECT item_code, jsonb_object_agg(branch_ext_id, jsonb_build_object('qty', qty, 'value', value, 'profit', profit)) AS sales_by_branch
            FROM sales GROUP BY item_code
        ),
        supplier_rank AS (
            SELECT *
            FROM (
                SELECT fp.item_code, COALESCE(ds.name, ds_ext.name, fp.supplier_ext_id, 'Χωρίς προμηθευτή') AS supplier_name,
                       ROW_NUMBER() OVER (PARTITION BY fp.item_code ORDER BY ABS(SUM(COALESCE(fp.net_value, 0))) DESC) AS rn
                FROM fact_purchases fp
                LEFT JOIN dim_suppliers ds ON ds.id = fp.supplier_id
                LEFT JOIN dim_suppliers ds_ext ON ds_ext.external_id = fp.supplier_ext_id
                WHERE fp.doc_date >= :purchase_rank_start AND fp.doc_date <= :period_to AND COALESCE(fp.item_code, '') <> ''
                GROUP BY fp.item_code, COALESCE(ds.name, ds_ext.name, fp.supplier_ext_id, 'Χωρίς προμηθευτή')
            ) ranked WHERE rn = 1
        ),
        scope AS (
            SELECT item_code FROM stock1_total
            UNION SELECT item_code FROM stock2
            UNION SELECT item_code FROM sales_total
        )
        SELECT
            scope.item_code,
            COALESCE(di.name, scope.item_code) AS item_name,
            COALESCE(NULLIF(di.category_1, ''), 'Χωρίς κατηγορία') AS category_1,
            COALESCE(NULLIF(di.category_2, ''), '') AS category_2,
            COALESCE(NULLIF(di.category_3, ''), '') AS category_3,
            COALESCE(NULLIF(di.replenishment_status_1, ''), NULLIF(di.commercial_status, ''), '') AS raw_status,
            COALESCE(NULLIF(di.commercial_status, ''), 'Χωρίς status') AS commercial_status,
            COALESCE(NULLIF(supplier_rank.supplier_name, ''), 'Χωρίς προμηθευτή') AS supplier_name,
            COALESCE(NULLIF(dg.name, ''), 'Χωρίς ομάδα') AS group_name,
            COALESCE(stock1_total.qty, 0) AS stock1_qty,
            COALESCE(stock1_total.value, 0) AS stock1_value,
            COALESCE(stock2.qty, 0) AS stock2_qty,
            COALESCE(stock2.value, 0) AS stock2_value,
            COALESCE(sales_total.qty, 0) AS sales_qty,
            COALESCE(sales_total.value, 0) AS sales_value,
            COALESCE(sales_total.profit, 0) AS sales_profit,
            stock1_branch.stock_by_branch,
            sales_branch.sales_by_branch
        FROM scope
        LEFT JOIN dim_items di ON di.external_id = scope.item_code
        LEFT JOIN dim_groups dg ON dg.id = di.group_id
        LEFT JOIN stock1_total ON stock1_total.item_code = scope.item_code
        LEFT JOIN stock1_branch ON stock1_branch.item_code = scope.item_code
        LEFT JOIN stock2 ON stock2.item_code = scope.item_code
        LEFT JOIN sales_total ON sales_total.item_code = scope.item_code
        LEFT JOIN sales_branch ON sales_branch.item_code = scope.item_code
        LEFT JOIN supplier_rank ON supplier_rank.item_code = scope.item_code
        WHERE COALESCE(di.softone_sotype, 51) = 51 AND COALESCE(di.is_active_source, true) = true
        """
    )
    rows = (await db.execute(
        sql,
        {
            'period_from': period_from,
            'period_to': period_to,
            'stock_date_1': stock_date_1,
            'stock_date_2': stock_date_2,
            'purchase_rank_start': period_to - timedelta(days=365),
        },
    )).mappings().all()
    filters = {
        'category_1': category_1 or [],
        'category_2': category_2 or [],
        'category_3': category_3 or [],
        'suppliers': suppliers or [],
        'commercial_status': commercial_status or [],
    }
    groups: dict[tuple[str, str], dict[str, object]] = {}
    options = {'pharmacies': [STORE_NAMES[code] for code in STORE_CODES], 'category_1': set(), 'category_2': set(), 'category_3': set(), 'suppliers': set(), 'group': set(), 'status_abcd': set(), 'commercial_status': set()}
    recommendations_by_key: dict[tuple[str, str, str, str], dict[str, object]] = {}
    selected_item_codes: list[str] = []
    destocking_buckets = ('A over', 'B over', 'C', 'D', 'D3')
    trend_base = {code: {bucket: 0.0 for bucket in destocking_buckets} for code in ('TOTAL', *STORE_CODES)}
    total_margin_profit = 0.0
    total_margin_sales = 0.0
    threshold = max(_as_float(threshold_weeks, 8.0), 0.0)
    period_weeks = max(period_days / 7.0, 1.0)
    for row in rows:
        status_code = _fnr_status_code(row.get('raw_status'))
        raw_status = str(row.get('raw_status') or '').casefold()
        if 'καταργη' in raw_status:
            status_code = 'D3'
        comm = str(row.get('commercial_status') or 'Χωρίς status')
        c1 = str(row.get('category_1') or 'Χωρίς κατηγορία')
        c2 = str(row.get('category_2') or '')
        c3 = str(row.get('category_3') or '')
        supplier = str(row.get('supplier_name') or 'Χωρίς προμηθευτή')
        grp = str(row.get('group_name') or 'Χωρίς ομάδα')
        options['category_1'].add(c1)
        if c2:
            options['category_2'].add(c2)
        if c3:
            options['category_3'].add(c3)
        options['suppliers'].add(supplier)
        options['group'].add(grp)
        options['status_abcd'].add(status_code)
        options['commercial_status'].add(comm)
        if status_filter and status_code not in status_filter:
            continue
        if not _availability_match(c1, filters['category_1']) or not _availability_match(c2, filters['category_2']) or not _availability_match(c3, filters['category_3']):
            continue
        if not _availability_match(supplier, filters['suppliers']) or not _availability_match(comm, filters['commercial_status']):
            continue
        if not _availability_match(grp, group_filter):
            continue
        stock_by_branch = row.get('stock_by_branch') if isinstance(row.get('stock_by_branch'), dict) else {}
        sales_by_branch = row.get('sales_by_branch') if isinstance(row.get('sales_by_branch'), dict) else {}
        branch_values = {code: {'qty': 0.0, 'value': 0.0, 'sales_qty': 0.0, 'sales_value': 0.0, 'profit': 0.0} for code in STORE_CODES}
        for branch, payload in stock_by_branch.items():
            code = _fnr_store_code(branch)
            if code and isinstance(payload, dict):
                branch_values[code]['qty'] += _as_float(payload.get('qty'))
                branch_values[code]['value'] += _as_float(payload.get('value'))
        for branch, payload in sales_by_branch.items():
            code = _fnr_store_code(branch)
            if code and isinstance(payload, dict):
                branch_values[code]['sales_qty'] += _as_float(payload.get('qty'))
                branch_values[code]['sales_value'] += _as_float(payload.get('value'))
                branch_values[code]['profit'] += _as_float(payload.get('profit'))
        if selected_pharmacies and not any(branch_values[code]['value'] > 0 for code in selected_pharmacies):
            continue
        selected_item_codes.append(str(row.get('item_code') or ''))
        group = groups.setdefault((status_code, comm), {'status_abcd': status_code, 'commercial_status': comm, 'sku_count': 0, 'stock_units_1': 0.0, 'stock_value_1': 0.0, 'stock_units_2': 0.0, 'stock_value_2': 0.0, 'sales_units': 0.0})
        group['sku_count'] = int(group['sku_count']) + 1
        for key, source in (('stock_units_1', 'stock1_qty'), ('stock_value_1', 'stock1_value'), ('stock_units_2', 'stock2_qty'), ('stock_value_2', 'stock2_value'), ('sales_units', 'sales_qty')):
            group[key] = _as_float(group.get(key)) + _as_float(row.get(source))
        total_margin_profit += _as_float(row.get('sales_profit'))
        total_margin_sales += _as_float(row.get('sales_value'))
        for code, metric in branch_values.items():
            stock_value = metric['value']
            if stock_value <= 0:
                continue
            weekly_sales_qty = metric['sales_qty'] / period_weeks
            unit_value = stock_value / metric['qty'] if metric['qty'] > 0 else 0.0
            over_value = max(metric['qty'] - (threshold * weekly_sales_qty), 0.0) * unit_value if status_code in {'A', 'B', 'C'} else stock_value
            if status_code == 'A':
                bucket = 'A over'
            elif status_code == 'B':
                bucket = 'B over'
            elif status_code == 'C':
                bucket = 'C'
                over_value = stock_value if code != 'LOGICA' else over_value
            elif status_code == 'D3':
                bucket = 'D3'
                over_value = stock_value
            elif status_code.startswith('D'):
                bucket = 'D'
                over_value = stock_value
            else:
                continue
            trend_base[code][bucket] += over_value
            trend_base['TOTAL'][bucket] += over_value
            rec_key = (bucket, supplier, code, comm)
            rec = recommendations_by_key.setdefault(rec_key, {'action': 'Reduce street prices' if code != 'LOGICA' else 'Reduce web prices', 'status_abcd': status_code, 'commercial_status': comm, 'vendor': supplier, 'location': AVAILABILITY_STORE_LABELS.get(code, code), 'cur_overstock': 0.0, 'target_overstock': 0.0, 'sku_codes': []})
            rec['cur_overstock'] = _as_float(rec.get('cur_overstock')) + over_value
            if len(rec['sku_codes']) < 8:
                rec['sku_codes'].append(str(row.get('item_code') or ''))
    table_rows: list[dict[str, object]] = []
    total = {'status_abcd': 'Grand Total', 'commercial_status': '', 'sku_count': 0, 'stock_units_1': 0.0, 'stock_value_1': 0.0, 'stock_units_2': 0.0, 'stock_value_2': 0.0, 'sales_units': 0.0}
    for group in groups.values():
        sales_units = _as_float(group.get('sales_units'))
        group['dio'] = _as_float(group.get('stock_units_1')) / (sales_units / period_days) if sales_units else '-'
        table_rows.append(group)
        for key in total:
            if isinstance(total[key], (int, float)):
                total[key] = total[key] + group.get(key, 0)  # type: ignore[operator]
    total['dio'] = _as_float(total.get('stock_units_1')) / (_as_float(total.get('sales_units')) / period_days) if _as_float(total.get('sales_units')) else '-'
    table_rows = sorted(table_rows, key=_availability_group_sort_key)
    table_rows.append(total)
    trend_periods = _availability_periods(period_from, period_to, step if step in {'week', 'month', 'quarter'} else 'month')[:18]
    period_labels = [label for label, _, _ in trend_periods]
    total_overstock = sum(_as_float(trend_base['TOTAL'][bucket]) for bucket in destocking_buckets)
    margin = total_margin_profit / total_margin_sales if total_margin_sales else 0.0
    trend_series = {code: {'name': label, 'lines': {bucket: [] for bucket in destocking_buckets}, 'margin': [], 'overstock': [], 'd3_overstock': []} for code, label in [('TOTAL', 'Total'), *[(store, AVAILABILITY_STORE_LABELS.get(store, store)) for store in STORE_CODES]]}
    if selected_item_codes and trend_periods:
        trend_sql = text(
            """
            WITH stock_snapshot_quality AS (
                SELECT snapshot_date, SUM(COALESCE(stock_value, 0)) AS stock_value
                FROM agg_stock_aging
                GROUP BY snapshot_date
            ),
            latest_stock AS (
                SELECT COALESCE(
                    (SELECT MAX(snapshot_date) FROM stock_snapshot_quality WHERE snapshot_date <= :period_end AND stock_value > 0),
                    (SELECT MAX(snapshot_date) FROM stock_snapshot_quality WHERE stock_value > 0),
                    (SELECT MAX(snapshot_date) FROM stock_snapshot_quality WHERE snapshot_date <= :period_end),
                    (SELECT MAX(snapshot_date) FROM stock_snapshot_quality)
                ) AS snapshot_date
            ),
            stock AS (
                SELECT asa.item_external_id AS item_code,
                       COALESCE(asa.branch_ext_id, '') AS branch_ext_id,
                       SUM(COALESCE(asa.qty_on_hand, 0)) AS qty,
                       SUM(COALESCE(asa.stock_value, 0)) AS value
                FROM agg_stock_aging asa
                JOIN latest_stock ON asa.snapshot_date = latest_stock.snapshot_date
                WHERE asa.item_external_id = ANY(:item_codes)
                GROUP BY asa.item_external_id, COALESCE(asa.branch_ext_id, '')
            ),
            sales AS (
                SELECT item_code,
                       COALESCE(branch_ext_id, '') AS branch_ext_id,
                       SUM(COALESCE(qty, 0)) AS qty,
                       SUM(COALESCE(net_value, 0)) AS value,
                       SUM(COALESCE(profit_amount, 0)) AS profit
                FROM fact_sales
                WHERE doc_date >= :period_start
                  AND doc_date <= :period_end
                  AND item_code = ANY(:item_codes)
                GROUP BY item_code, COALESCE(branch_ext_id, '')
            ),
            scope AS (
                SELECT item_code, branch_ext_id FROM stock
                UNION
                SELECT item_code, branch_ext_id FROM sales
            )
            SELECT
                scope.item_code,
                COALESCE(scope.branch_ext_id, '') AS branch_ext_id,
                COALESCE(NULLIF(di.replenishment_status_1, ''), NULLIF(di.commercial_status, ''), '') AS raw_status,
                COALESCE(stock.qty, 0) AS stock_qty,
                COALESCE(stock.value, 0) AS stock_value,
                COALESCE(sales.qty, 0) AS sales_qty,
                COALESCE(sales.value, 0) AS sales_value,
                COALESCE(sales.profit, 0) AS sales_profit
            FROM scope
            LEFT JOIN stock ON stock.item_code = scope.item_code AND stock.branch_ext_id = scope.branch_ext_id
            LEFT JOIN sales ON sales.item_code = scope.item_code AND sales.branch_ext_id = scope.branch_ext_id
            LEFT JOIN dim_items di ON di.external_id = scope.item_code
            WHERE COALESCE(di.softone_sotype, 51) = 51
              AND COALESCE(di.is_active_source, true) = true
            """
        )
        item_code_list = sorted({code for code in selected_item_codes if code})
        for _label, period_start, period_end in trend_periods:
            period_len_days = max((period_end - period_start).days + 1, 1)
            period_len_weeks = max(period_len_days / 7.0, 1.0)
            period_base = {code: {**{bucket: 0.0 for bucket in destocking_buckets}, 'sales_value': 0.0, 'sales_profit': 0.0} for code in ('TOTAL', *STORE_CODES)}
            result = await db.execute(
                trend_sql,
                {
                    'period_start': period_start,
                    'period_end': period_end,
                    'item_codes': item_code_list,
                },
            )
            for trend_row in result.mappings().all():
                code = _fnr_store_code(trend_row.get('branch_ext_id'))
                if code not in STORE_CODES:
                    continue
                if selected_pharmacies and code not in selected_pharmacies:
                    continue
                status_code = _fnr_status_code(trend_row.get('raw_status'))
                raw_status = str(trend_row.get('raw_status') or '').casefold()
                if 'καταργη' in raw_status:
                    status_code = 'D3'
                sales_qty = _as_float(trend_row.get('sales_qty'))
                sales_value = _as_float(trend_row.get('sales_value'))
                sales_profit = _as_float(trend_row.get('sales_profit'))
                stock_qty = _as_float(trend_row.get('stock_qty'))
                stock_value = _as_float(trend_row.get('stock_value'))
                period_base[code]['sales_value'] += sales_value
                period_base[code]['sales_profit'] += sales_profit
                period_base['TOTAL']['sales_value'] += sales_value
                period_base['TOTAL']['sales_profit'] += sales_profit
                if stock_value <= 0:
                    continue
                weekly_sales_qty = sales_qty / period_len_weeks
                unit_value = stock_value / stock_qty if stock_qty > 0 else 0.0
                over_value = max(stock_qty - (threshold * weekly_sales_qty), 0.0) * unit_value if status_code in {'A', 'B', 'C'} else stock_value
                if status_code == 'A':
                    bucket = 'A over'
                elif status_code == 'B':
                    bucket = 'B over'
                elif status_code == 'C':
                    bucket = 'C'
                    over_value = stock_value if code != 'LOGICA' else over_value
                elif status_code == 'D3':
                    bucket = 'D3'
                    over_value = stock_value
                elif status_code.startswith('D'):
                    bucket = 'D'
                    over_value = stock_value
                else:
                    continue
                period_base[code][bucket] += over_value
                period_base['TOTAL'][bucket] += over_value
            for code in trend_series:
                period_overstock = sum(_as_float(period_base[code][bucket]) for bucket in destocking_buckets)
                period_margin = _as_float(period_base[code]['sales_profit']) / _as_float(period_base[code]['sales_value']) if _as_float(period_base[code]['sales_value']) else 0.0
                for bucket in destocking_buckets:
                    trend_series[code]['lines'][bucket].append(_fnr_round(_as_float(period_base[code][bucket])))
                trend_series[code]['overstock'].append(_fnr_round(period_overstock))
                trend_series[code]['d3_overstock'].append(_fnr_round(_as_float(period_base[code]['D3'])))
                trend_series[code]['margin'].append(_fnr_round(period_margin))
    else:
        for code in trend_series:
            for bucket in destocking_buckets:
                trend_series[code]['lines'][bucket] = [0.0 for _ in trend_periods]
            trend_series[code]['overstock'] = [0.0 for _ in trend_periods]
            trend_series[code]['d3_overstock'] = [0.0 for _ in trend_periods]
            trend_series[code]['margin'] = [0.0 for _ in trend_periods]
    trends = {'periods': period_labels, 'series': []}
    for entry in trend_series.values():
        trends['series'].append({
            'name': entry['name'],
            'lines': [{'name': bucket, 'values': entry['lines'][bucket]} for bucket in destocking_buckets],
        })
    correlation = {'periods': period_labels, 'series': []}
    for entry in trend_series.values():
        correlation['series'].append({'name': entry['name'], 'overstock': entry['overstock'], 'd3_overstock': entry['d3_overstock'], 'margin': entry['margin']})
    recommendations = sorted(
        [
            {
                **rec,
                'destocking_potential': max(_as_float(rec.get('cur_overstock')) - _as_float(rec.get('target_overstock')), 0.0),
                'show_sku': ', '.join(rec.get('sku_codes') or []),
            }
            for rec in recommendations_by_key.values()
            if _as_float(rec.get('cur_overstock')) > 0
        ],
        key=lambda item: -_as_float(item.get('destocking_potential')),
    )[:50]
    return {
        'parameters': {'period_from': period_from.isoformat(), 'period_to': period_to.isoformat(), 'stock_date_1': stock_date_1.isoformat(), 'stock_date_2': stock_date_2.isoformat(), 'threshold_weeks': threshold, 'step': step},
        'filters': {'pharmacies': pharmacies or [], 'category_1': category_1 or [], 'category_2': category_2 or [], 'category_3': category_3 or [], 'suppliers': suppliers or [], 'group': group_filter, 'status_abcd': status_abcd or [], 'commercial_status': commercial_status or []},
        'options': {key: sorted(value) if isinstance(value, set) else value for key, value in options.items()},
        'table_rows': table_rows,
        'trends': trends,
        'correlation': correlation,
        'recommendations': recommendations,
        'summary': {'rows_count': len(table_rows), 'sku_count': total.get('sku_count', 0), 'stock_value_1': total.get('stock_value_1', 0), 'stock_value_2': total.get('stock_value_2', 0), 'total_overstock': total_overstock, 'margin_pct': margin, 'recommendations_count': len(recommendations)},
    }


def _apply_store_map(sql_body: str, store_warehouse_map: dict[str, object] | None) -> tuple[str, dict[str, str]]:
    """Per-tenant warehouse->store mapping for the FnR.

    When a tenant supplies a {store_code: [warehouse_ext_ids]} map, stock and
    sales are grouped by store (via a parameterized VALUES join) instead of by
    branch; warehouses not listed are excluded. With no map (other callers /
    tenants) the SQL is returned unchanged → current branch-level behaviour.
    Anchors are asserted so a future SQL change fails loudly, never silently
    mis-maps stock.
    """
    if not store_warehouse_map:
        return sql_body, {}
    pairs: list[tuple[str, str]] = []
    for store_code, warehouses in store_warehouse_map.items():
        store = str(store_code or '').strip()
        if not store:
            continue
        for wh in (warehouses or []):
            wh_id = str(wh or '').strip()
            if wh_id:
                pairs.append((wh_id, store))
    if not pairs:
        return sql_body, {}
    values = ', '.join(f'(:wh_{i}, :st_{i})' for i in range(len(pairs)))
    params: dict[str, str] = {}
    for i, (wh_id, store) in enumerate(pairs):
        params[f'wh_{i}'] = wh_id
        params[f'st_{i}'] = store
    cte = f"store_map AS (SELECT v.wh, v.store_code FROM (VALUES {values}) AS v(wh, store_code)),\n        "
    replacements = [
        ("WITH latest_stock AS (", f"WITH {cte}latest_stock AS ("),
        (
            "COALESCE(fi.branch_ext_id, '') AS branch_ext_id,\n"
            "                MAX(COALESCE(db.name, db.branch_name, fi.branch_ext_id, 'Χωρίς σημείο')) AS branch_name,",
            "COALESCE(smi.store_code, '') AS branch_ext_id,\n                '' AS branch_name,",
        ),
        (
            "LEFT JOIN dim_branches db ON db.external_id = fi.branch_ext_id\n"
            "            WHERE COALESCE(fi.movement_type, 'snapshot') = 'snapshot'\n"
            "            GROUP BY COALESCE(fi.item_code, di.external_id), COALESCE(fi.branch_ext_id, '')",
            "LEFT JOIN store_map smi ON smi.wh = COALESCE(fi.warehouse_ext_id, '')\n"
            "            WHERE COALESCE(fi.movement_type, 'snapshot') = 'snapshot'\n"
            "            GROUP BY 1, 2",
        ),
        (
            "fs.item_code AS item_code,\n                COALESCE(fs.branch_ext_id, '') AS branch_ext_id,",
            "fs.item_code AS item_code,\n                COALESCE(sms.store_code, '') AS branch_ext_id,",
        ),
        (
            "FROM fact_sales fs\n            WHERE fs.doc_date >= :previous_start",
            "FROM fact_sales fs\n            LEFT JOIN store_map sms ON sms.wh = COALESCE(fs.warehouse_ext_id, '')\n            WHERE fs.doc_date >= :previous_start",
        ),
        (
            "GROUP BY fs.item_code, COALESCE(fs.branch_ext_id, '')",
            "GROUP BY 1, 2",
        ),
    ]
    out = sql_body
    for old, new in replacements:
        if old not in out:
            raise ValueError('FnR store-map: SQL anchor not found; refusing to mis-map stock')
        out = out.replace(old, new, 1)
    return out, params


async def build_availability_foundation(
    db: AsyncSession,
    *,
    as_of: date | None = None,
    target_stock_weeks: float = 4.0,
    overstock_weeks: float = 12.0,
    sales_window_days: int = 30,
    store_warehouse_map: dict[str, object] | None = None,
    branch: str | None = None,
    status: str | None = None,
    category: str | None = None,
    group: str | None = None,
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
    sql_body = (
        """
        WITH latest_stock AS (
            SELECT MAX(doc_date) AS snapshot_date
            FROM fact_inventory
            WHERE doc_date <= :as_of
              AND COALESCE(movement_type, 'snapshot') = 'snapshot'
        ),
        stock AS (
            SELECT
                COALESCE(fi.item_code, di.external_id) AS item_code,
                COALESCE(fi.branch_ext_id, '') AS branch_ext_id,
                MAX(COALESCE(db.name, db.branch_name, fi.branch_ext_id, 'Χωρίς σημείο')) AS branch_name,
                SUM(COALESCE(NULLIF(fi.qty_available, 0), fi.qty_on_hand, 0)) AS stock_qty,
                SUM(COALESCE(NULLIF(fi.value_amount, 0), fi.cost_amount, 0)) AS stock_value
            FROM (
                -- Keep one row per (date, branch, warehouse, item): when more than one
                -- snapshot set lands on a day (nightly STKSNAP refresh + a full-sync pull)
                -- summing the raw rows would double-count stock. item_code is the stable
                -- natural key across sources (item_id may be resolved inconsistently).
                SELECT z.* FROM (
                    SELECT fi2.*, ROW_NUMBER() OVER (
                        PARTITION BY fi2.doc_date, COALESCE(fi2.branch_ext_id, ''), COALESCE(fi2.warehouse_ext_id, ''),
                                     COALESCE(NULLIF(fi2.item_code, ''), fi2.item_id::text, '')
                        ORDER BY fi2.updated_at DESC
                    ) AS _rn
                    FROM fact_inventory fi2
                    WHERE COALESCE(fi2.movement_type, 'snapshot') = 'snapshot'
                ) z WHERE z._rn = 1
            ) fi
            JOIN latest_stock ls ON ls.snapshot_date = fi.doc_date
            LEFT JOIN dim_items di ON di.id = fi.item_id
            LEFT JOIN dim_branches db ON db.external_id = fi.branch_ext_id
            WHERE COALESCE(fi.movement_type, 'snapshot') = 'snapshot'
            GROUP BY COALESCE(fi.item_code, di.external_id), COALESCE(fi.branch_ext_id, '')
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
            COALESCE(NULLIF(dg.name, ''), 'Χωρίς ομάδα') AS group_name,
            COALESCE(NULLIF(di.manual_order_category, ''), NULLIF(di.abc_category, ''), 'Χωρίς ABC') AS abc_category,
            COALESCE(NULLIF(di.commercial_status, ''), '-') AS commercial_status,
            COALESCE(NULLIF(supplier_rank.supplier_name, ''), NULLIF(expected.expected_supplier, ''), NULLIF(di.preferred_supplier_name, ''), 'Χωρίς προμηθευτή') AS supplier_name,
            COALESCE(stock.stock_qty, 0) AS stock_qty,
            COALESCE(stock.stock_value, 0) AS stock_value,
            COALESCE(sales.sales_qty_current, 0) AS sales_qty_current,
            COALESCE(sales.sales_value_current, 0) AS sales_value_current,
            COALESCE(sales.sales_qty_previous, 0) AS sales_qty_previous,
            COALESCE(sales.sales_value_previous, 0) AS sales_value_previous,
            COALESCE(expected.expected_qty, 0) AS expected_qty,
            COALESCE(di.min_stock, 1) AS min_stock,
            COALESCE(di.replenishment_moq, 1) AS repl_moq,
            COALESCE(di.vendor_moq, 1) AS vendor_moq,
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
        LEFT JOIN dim_groups dg ON dg.id = di.group_id
        LEFT JOIN dim_branches db ON db.external_id = scope.branch_ext_id
        LEFT JOIN expected ON expected.item_code = scope.item_code
        LEFT JOIN supplier_rank ON supplier_rank.item_code = scope.item_code
        LEFT JOIN latest_purchase_price ON latest_purchase_price.item_code = scope.item_code
        LEFT JOIN weighted_purchase_price ON weighted_purchase_price.item_code = scope.item_code
        WHERE COALESCE(di.softone_sotype, 51) = 51
          AND COALESCE(di.is_active_source, true) = true
        """
    )
    sql_body, store_map_params = _apply_store_map(sql_body, store_warehouse_map)
    sql = text(sql_body)
    result = await db.execute(
        sql,
        {
            'as_of': as_of,
            'current_start': current_start,
            'previous_start': previous_start,
            'purchase_start': purchase_start,
            'purchase_avg_start': purchase_avg_start,
            **store_map_params,
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
        'groups': set(),
        'vendors': set(),
        'abcs': set(),
    }
    detail_rows: list[dict[str, object]] = []
    branch_value = (branch or '').strip()
    status_value = (status or '').strip()
    category_value = (category or '').strip()
    group_value = (group or '').strip()
    vendor_value = (vendor or '').strip()
    abc_value = (abc or '').strip()
    search_value = (search or '').strip()
    branch_filter = branch_value.casefold()
    status_filter = status_value.casefold()
    category_filter = category_value.casefold()
    group_filter = group_value.casefold()
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
        group_name = str(row.get('group_name') or 'Χωρίς ομάδα').strip() or 'Χωρίς ομάδα'
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
        options['groups'].add(group_name)
        options['vendors'].add(vendor)
        options['abcs'].add(abc_category)
        if branch_filter and branch.casefold() != branch_filter:
            continue
        if status_filter and status.casefold() != status_filter:
            continue
        if category_filter and category.casefold() != category_filter:
            continue
        if group_filter and group_name.casefold() != group_filter:
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
                'group': group_name,
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
                    'category_1': category,
                    'category_2': str(row.get('category_2') or ''),
                    'category_3': str(row.get('category_3') or ''),
                    'group': group_name,
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
                    'min_stock': _as_float(row.get('min_stock'), 1.0),
                    'repl_moq': max(_as_float(row.get('repl_moq'), 1.0), 1.0),
                    'vendor_moq': max(_as_float(row.get('vendor_moq'), 1.0), 1.0),
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
