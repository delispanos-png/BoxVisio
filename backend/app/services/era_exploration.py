from __future__ import annotations

from functools import lru_cache
import hashlib
from pathlib import Path
import re
import xml.etree.ElementTree as ET
import zipfile

from sqlalchemy import desc, insert, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.control import Tenant
from app.models.tenant import DimItem, EraExplorationLine, EraExplorationSnapshot


_DEFAULT_PHARMACY295_FILE = Path('/opt/cloudon-bi/eRA_Exploration_data_April26.xlsx')
_XLSX_NS = {'a': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}
_NUMERIC_FIELDS = {
    'market_sales',
    'your_sales',
    'your_sales_value_ms',
    'market_units',
    'your_units',
    'your_units_ms',
}
_REQUIRED_HEADERS = (
    'brand',
    'product_name',
    'barcode',
    'category',
    'market_sales',
    'your_sales',
    'your_sales_value_ms',
    'market_units',
    'your_units',
    'your_units_ms',
)
_MONTH_TOKEN_RE = re.compile(r'([A-Za-z]+)[\s._-]*([0-9]{2,4})$', re.IGNORECASE)
_DEFAULT_RECOMMENDED_ADD_VALUE_SHARE_PCT = 0.05
_MONTHS = {
    'jan': 1, 'january': 1,
    'feb': 2, 'february': 2,
    'mar': 3, 'march': 3,
    'apr': 4, 'april': 4,
    'may': 5,
    'jun': 6, 'june': 6,
    'jul': 7, 'july': 7,
    'aug': 8, 'august': 8,
    'sep': 9, 'sept': 9, 'september': 9,
    'oct': 10, 'october': 10,
    'nov': 11, 'november': 11,
    'dec': 12, 'december': 12,
}


class DuplicateMarketImportError(ValueError):
    def __init__(self, message: str, *, existing_snapshot_id: str | None = None) -> None:
        super().__init__(message)
        self.existing_snapshot_id = existing_snapshot_id


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _tenant_file_path(tenant: Tenant) -> Path | None:
    flags = tenant.feature_flags if isinstance(tenant.feature_flags, dict) else {}
    cfg = flags.get('era_exploration_data_config') or flags.get('era_exploration')
    if not isinstance(cfg, dict) and isinstance(flags.get('era_exploration_data'), dict):
        cfg = flags.get('era_exploration_data')
    if isinstance(cfg, dict) and cfg.get('file_path'):
        return Path(str(cfg['file_path']))
    if str(getattr(tenant, 'slug', '')).lower() == 'pharmacy295':
        return _DEFAULT_PHARMACY295_FILE
    return None


def _column_index(cell_ref: str) -> int:
    letters = re.sub(r'\d+', '', cell_ref or '').upper()
    value = 0
    for char in letters:
        value = value * 26 + (ord(char) - 64)
    return max(value - 1, 0)


def _cell_text(cell: ET.Element, shared_strings: list[str]) -> str:
    cell_type = cell.attrib.get('t')
    if cell_type == 'inlineStr':
        return ''.join(t.text or '' for t in cell.findall('.//a:t', _XLSX_NS)).strip()
    value = cell.find('a:v', _XLSX_NS)
    if value is None or value.text is None:
        return ''
    raw = value.text.strip()
    if cell_type == 's' and raw.isdigit():
        idx = int(raw)
        return shared_strings[idx] if idx < len(shared_strings) else raw
    return raw


def _number(value: object) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace('%', '')
    if not text:
        return 0.0
    try:
        return float(text)
    except ValueError:
        return 0.0


def _barcode_parts(value: object) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for part in re.split(r'[\s,;|]+', str(value or '').strip()):
        clean = part.strip()
        if clean and clean not in seen:
            seen.add(clean)
            out.append(clean)
    return out


def era_period_from_filename(filename: str | Path | None) -> str:
    """Return the canonical reporting period as YYYYMM from the last filename token.

    eRA files arrive with names like ``eRA Exploration data_Feb 26.xlsx`` or
    ``eRA_Exploration_data_April26.xlsx``. The final segment is the business
    period, so we normalize it before storing the snapshot in the tenant DB.
    """
    stem = Path(str(filename or '')).stem
    if not stem:
        return ''
    token = re.split(r'[_]+', stem)[-1].strip()
    match = _MONTH_TOKEN_RE.fullmatch(token) or _MONTH_TOKEN_RE.search(stem)
    if not match:
        return ''
    month_token = match.group(1).lower()
    month = _MONTHS.get(month_token) or _MONTHS.get(month_token[:3])
    if not month:
        return ''
    year_token = match.group(2)
    year = int(year_token)
    if year < 100:
        year += 2000
    return f'{year:04d}{month:02d}'


def _period_month(period: str) -> int | None:
    canonical = str(period or '').strip()
    if re.fullmatch(r'20\d{4}', canonical):
        return int(canonical[4:6])
    match = re.match(r'([A-Za-z]+)', str(period or '').strip())
    if not match:
        return None
    token = match.group(1).lower()
    return _MONTHS.get(token[:3]) or _MONTHS.get(token)


def _period_season(period: str) -> str:
    month = _period_month(period)
    if month in {12, 1, 2}:
        return 'Χειμώνας'
    if month in {3, 4, 5}:
        return 'Άνοιξη'
    if month in {6, 7, 8}:
        return 'Καλοκαίρι'
    if month in {9, 10, 11}:
        return 'Φθινόπωρο'
    return ''


def _read_shared_strings(archive: zipfile.ZipFile) -> list[str]:
    if 'xl/sharedStrings.xml' not in archive.namelist():
        return []
    root = ET.fromstring(archive.read('xl/sharedStrings.xml'))
    return [''.join(t.text or '' for t in item.findall('.//a:t', _XLSX_NS)) for item in root.findall('a:si', _XLSX_NS)]


@lru_cache(maxsize=16)
def _load_xlsx_rows(path_str: str, mtime_ns: int) -> tuple[dict[str, object], ...]:
    del mtime_ns
    path = Path(path_str)
    with zipfile.ZipFile(path) as archive:
        shared_strings = _read_shared_strings(archive)
        root = ET.fromstring(archive.read('xl/worksheets/sheet1.xml'))
        parsed_rows: list[list[str]] = []
        for row in root.findall('.//a:sheetData/a:row', _XLSX_NS):
            values: list[str] = []
            for cell in row.findall('a:c', _XLSX_NS):
                idx = _column_index(cell.attrib.get('r', 'A1'))
                while len(values) <= idx:
                    values.append('')
                values[idx] = _cell_text(cell, shared_strings)
            if any(values):
                parsed_rows.append(values)
    if not parsed_rows:
        return tuple()
    headers = [str(h).strip() for h in parsed_rows[0]]
    rows: list[dict[str, object]] = []
    for values in parsed_rows[1:]:
        row = {headers[i]: (values[i] if i < len(values) else '') for i in range(len(headers))}
        if not any(row.values()):
            continue
        for field in _NUMERIC_FIELDS:
            row[field] = _number(row.get(field))
        parts = _barcode_parts(row.get('barcode'))
        row['barcodes'] = parts
        row['primary_barcode'] = parts[0] if parts else ''
        row['gap_sales'] = max(_number(row.get('market_sales')) - _number(row.get('your_sales')), 0.0)
        row['gap_units'] = max(_number(row.get('market_units')) - _number(row.get('your_units')), 0.0)
        rows.append(row)
    return tuple(rows)


def clear_era_exploration_cache() -> None:
    _load_xlsx_rows.cache_clear()


def validate_era_exploration_file(path: Path) -> dict[str, object]:
    try:
        rows = _load_xlsx_rows(str(path), path.stat().st_mtime_ns)
    except zipfile.BadZipFile as exc:
        raise ValueError('Το αρχείο δεν είναι έγκυρο XLSX.') from exc
    except KeyError as exc:
        raise ValueError('Το XLSX δεν έχει το αναμενόμενο φύλλο δεδομένων.') from exc
    if not rows:
        raise ValueError('Το αρχείο δεν περιέχει γραμμές eRA Exploration Data.')
    first = rows[0]
    missing = [header for header in _REQUIRED_HEADERS if header not in first]
    if missing:
        raise ValueError(f'Λείπουν στήλες από το Excel: {", ".join(missing)}')
    return {
        'rows': len(rows),
        'brands': len({str(row.get('brand') or '') for row in rows if row.get('brand')}),
        'categories': len({str(row.get('category') or '') for row in rows if row.get('category')}),
        'period': era_period_from_filename(path),
    }


async def import_era_exploration_file(
    db: AsyncSession,
    path: Path,
    *,
    source_filename: str | None = None,
    source_sha256: str | None = None,
    imported_by: str | None = None,
) -> dict[str, object]:
    validation = validate_era_exploration_file(path)
    rows = _load_xlsx_rows(str(path), path.stat().st_mtime_ns)
    period = str(validation.get('period') or era_period_from_filename(source_filename or path))
    checksum = (source_sha256 or file_sha256(path)).strip()
    filename = source_filename or path.name
    duplicate = (
        await db.execute(
            select(EraExplorationSnapshot)
            .where(EraExplorationSnapshot.source_sha256 == checksum)
            .order_by(desc(EraExplorationSnapshot.imported_at))
            .limit(1)
        )
    ).scalar_one_or_none()
    if duplicate is not None:
        raise DuplicateMarketImportError(
            'Το ίδιο eRA αρχείο έχει ήδη γίνει import.',
            existing_snapshot_id=str(duplicate.id),
        )
    snapshot = EraExplorationSnapshot(
        source_filename=filename,
        source_sha256=checksum,
        period_label=period,
        rows_count=len(rows),
        summary_json={
            'brands': validation.get('brands', 0),
            'categories': validation.get('categories', 0),
        },
        imported_by=imported_by,
    )
    db.add(snapshot)
    await db.flush()
    await db.execute(
        insert(EraExplorationLine),
        [
            {
                'snapshot_id': snapshot.id,
                'source_row': idx,
                'brand': str(row.get('brand') or '')[:255] or None,
                'product_name': str(row.get('product_name') or '')[:500] or None,
                'barcode': str(row.get('barcode') or '') or None,
                'primary_barcode': str(row.get('primary_barcode') or '')[:64] or None,
                'barcodes_json': list(row.get('barcodes') or []),
                'category': str(row.get('category') or '')[:255] or None,
                'market_sales': _number(row.get('market_sales')),
                'your_sales': _number(row.get('your_sales')),
                'your_sales_value_ms': _number(row.get('your_sales_value_ms')),
                'market_units': _number(row.get('market_units')),
                'your_units': _number(row.get('your_units')),
                'your_units_ms': _number(row.get('your_units_ms')),
                'gap_sales': _number(row.get('gap_sales')),
                'gap_units': _number(row.get('gap_units')),
                'raw_json': dict(row),
            }
            for idx, row in enumerate(rows, start=2)
        ],
    )
    await db.commit()
    return {
        **validation,
        'snapshot_id': str(snapshot.id),
        'source': 'db',
        'filename': filename,
        'source_sha256': checksum,
    }


async def _latest_era_snapshot(db: AsyncSession) -> EraExplorationSnapshot:
    snapshot = (
        await db.execute(
            select(EraExplorationSnapshot).order_by(desc(EraExplorationSnapshot.imported_at)).limit(1)
        )
    ).scalar_one_or_none()
    if snapshot is None:
        raise FileNotFoundError('Δεν έχει γίνει import eRA Exploration Data στη βάση του tenant.')
    return snapshot


async def _load_rows_from_db(db: AsyncSession, snapshot_id) -> list[dict[str, object]]:
    result = await db.execute(select(EraExplorationLine).where(EraExplorationLine.snapshot_id == snapshot_id))
    rows: list[dict[str, object]] = []
    for line in result.scalars():
        row = dict(line.raw_json or {})
        row.update(
            {
                'brand': line.brand or '',
                'product_name': line.product_name or '',
                'barcode': line.barcode or '',
                'primary_barcode': line.primary_barcode or '',
                'barcodes': list(line.barcodes_json or []),
                'category': line.category or '',
                'market_sales': _number(line.market_sales),
                'your_sales': _number(line.your_sales),
                'your_sales_value_ms': _number(line.your_sales_value_ms),
                'market_units': _number(line.market_units),
                'your_units': _number(line.your_units),
                'your_units_ms': _number(line.your_units_ms),
                'gap_sales': _number(line.gap_sales),
                'gap_units': _number(line.gap_units),
            }
        )
        rows.append(row)
    return rows


def _recommended_add_thresholds(tenant: Tenant) -> dict[str, float]:
    flags = tenant.feature_flags if isinstance(tenant.feature_flags, dict) else {}
    cfg = flags.get('era_exploration_data_config') if isinstance(flags.get('era_exploration_data_config'), dict) else {}
    try:
        value_share = float(cfg.get('recommended_add_value_share_pct') or _DEFAULT_RECOMMENDED_ADD_VALUE_SHARE_PCT)
    except (TypeError, ValueError):
        value_share = _DEFAULT_RECOMMENDED_ADD_VALUE_SHARE_PCT
    return {
        'value_share_pct': max(value_share, 0.0),
    }


def _item_barcode_tokens(barcode_value: object, alternate_barcodes_value: object) -> list[str]:
    tokens = _barcode_parts(barcode_value)
    tokens.extend(_barcode_parts(alternate_barcodes_value))
    seen: set[str] = set()
    out: list[str] = []
    for token in tokens:
        if token not in seen:
            seen.add(token)
            out.append(token)
    return out


async def _tenant_items_by_barcode(db: AsyncSession) -> dict[str, dict[str, object]]:
    result = await db.execute(
        select(
            DimItem.external_id,
            DimItem.name,
            DimItem.barcode,
            DimItem.alternate_barcodes,
            DimItem.is_active_source,
        ).where(
            (DimItem.barcode.is_not(None)) | (DimItem.alternate_barcodes.is_not(None))
        )
    )
    out: dict[str, dict[str, object]] = {}
    for row in result.mappings():
        payload = {
            'item_code': row.get('external_id'),
            'item_name': row.get('name'),
            'is_active_source': row.get('is_active_source'),
        }
        for barcode in _item_barcode_tokens(row.get('barcode'), row.get('alternate_barcodes')):
            out.setdefault(barcode, payload)
    return out


def _status_for_row(
    row: dict[str, object],
    item: dict[str, object] | None,
    market_value_share: float,
    market_unit_share: float,
    *,
    recommended_add_value_share_pct: float,
) -> dict[str, object]:
    has_your_sales = _number(row.get('your_sales')) > 0 or _number(row.get('your_units')) > 0
    del market_unit_share
    strong_market = _number(row.get('market_sales')) > 0 and market_value_share >= recommended_add_value_share_pct
    if item:
        active_source = item.get('is_active_source')
        if active_source is False:
            return {
                'code': 'inactive_in_db',
                'label': 'Υπάρχει ανενεργό',
                'tone': 'warning',
                'action': 'Έλεγξε αν πρέπει να ενεργοποιηθεί ή να αντικατασταθεί.',
            }
        return {
            'code': 'active_in_db',
            'label': 'Ενεργό στη βάση μας',
            'tone': 'success',
            'action': 'Παρακολούθησε μερίδιο και διαθεσιμότητα.',
        }
    if has_your_sales:
        return {
            'code': 'needs_mapping',
            'label': 'Θέλει αντιστοίχιση',
            'tone': 'info',
            'action': 'Το eRA δείχνει πωλήσεις μας, αλλά δεν βρέθηκε barcode στη βάση. Συμπλήρωσε barcode/εναλλακτικά barcode.',
        }
    if strong_market:
        return {
            'code': 'recommended_add',
            'label': 'Πρόταση προσθήκης',
            'tone': 'danger',
            'action': 'Δεν υπάρχει στη βάση και περνά το όριο σημαντικού μεριδίου αξίας αγοράς. Έλεγξε προμηθευτή, τιμή και περιθώριο.',
        }
    return {
        'code': 'not_in_db',
        'label': 'Δεν υπάρχει στη βάση',
        'tone': 'muted',
        'action': 'Χαμηλότερη προτεραιότητα, εκτός αν είναι στρατηγική κατηγορία.',
    }


def _price_position_for_row(row: dict[str, object]) -> dict[str, object]:
    market_avg = _number(row.get('market_avg_price'))
    your_avg = _number(row.get('your_avg_price'))
    if market_avg <= 0 or your_avg <= 0:
        return {
            'code': 'no_comparison',
            'label': 'Χωρίς σύγκριση',
            'tone': 'muted',
            'diff_amount': 0.0,
            'diff_pct': 0.0,
            'action': 'Δεν υπάρχουν αρκετές μονάδες/αξίες για ασφαλή σύγκριση τιμής.',
        }
    diff_amount = your_avg - market_avg
    diff_pct = (diff_amount / market_avg * 100) if market_avg else 0.0
    if abs(diff_pct) < 1:
        return {
            'code': 'near_market',
            'label': 'Κοντά στην αγορά',
            'tone': 'info',
            'diff_amount': diff_amount,
            'diff_pct': diff_pct,
            'action': 'Η μέση τιμή μας είναι πρακτικά κοντά στον ανταγωνισμό.',
        }
    if diff_amount > 0:
        return {
            'code': 'above_market',
            'label': 'Πάνω από αγορά',
            'tone': 'warning',
            'diff_amount': diff_amount,
            'diff_pct': diff_pct,
            'action': 'Ελέγξτε αν η υψηλότερη τιμή υποστηρίζεται από διαθεσιμότητα, υπηρεσία ή στρατηγική περιθωρίου.',
        }
    return {
        'code': 'below_market',
        'label': 'Κάτω από αγορά',
        'tone': 'danger',
        'diff_amount': diff_amount,
        'diff_pct': diff_pct,
        'action': 'Ελέγξτε αν χάνεται περιθώριο ή αν πρόκειται για συνειδητή επιθετική τιμολόγηση.',
    }


async def era_exploration_report(
    tenant: Tenant,
    db: AsyncSession,
    *,
    q: str | None = None,
    brand: str | None = None,
    category: str | None = None,
    assortment_status: str | None = None,
    price_position: str | None = None,
    sort: str = 'market_sales',
    direction: str = 'desc',
    page: int = 1,
    page_size: int = 50,
) -> dict[str, object]:
    snapshot = await _latest_era_snapshot(db)
    rows = await _load_rows_from_db(db, snapshot.id)
    all_rows = rows
    q_norm = (q or '').strip().lower()
    brand_norm = (brand or '').strip()
    category_norm = (category or '').strip()
    if q_norm:
        rows = [
            row for row in rows
            if q_norm in str(row.get('product_name') or '').lower()
            or q_norm in str(row.get('barcode') or '').lower()
            or q_norm in str(row.get('brand') or '').lower()
        ]
    if brand_norm:
        rows = [row for row in rows if str(row.get('brand') or '') == brand_norm]
    if category_norm:
        rows = [row for row in rows if str(row.get('category') or '') == category_norm]

    total_market_sales = sum(_number(row.get('market_sales')) for row in rows)
    total_market_units = sum(_number(row.get('market_units')) for row in rows)
    share_market_sales = sum(_number(row.get('market_sales')) for row in all_rows)
    share_market_units = sum(_number(row.get('market_units')) for row in all_rows)
    brands = sorted({str(row.get('brand') or '').strip() for row in all_rows if str(row.get('brand') or '').strip()})
    categories = sorted({str(row.get('category') or '').strip() for row in all_rows if str(row.get('category') or '').strip()})
    flags = tenant.feature_flags if isinstance(tenant.feature_flags, dict) else {}
    cfg = flags.get('era_exploration_data_config') if isinstance(flags.get('era_exploration_data_config'), dict) else {}
    period = str(snapshot.period_label or cfg.get('period') or era_period_from_filename(cfg.get('filename')) or '')
    season = _period_season(period)
    recommendation_thresholds = _recommended_add_thresholds(tenant)
    item_by_barcode = await _tenant_items_by_barcode(db)
    for row in rows:
        matched_item = None
        matched_barcode = ''
        for barcode in row.get('barcodes') or []:
            matched_item = item_by_barcode.get(str(barcode))
            if matched_item:
                matched_barcode = str(barcode)
                break
        row['tenant_item'] = matched_item or None
        row['matched_barcode'] = matched_barcode
        row['period'] = period
        row['season'] = season
        row['market_value_share_pct'] = (_number(row.get('market_sales')) / share_market_sales * 100) if share_market_sales else 0
        row['market_unit_share_pct'] = (_number(row.get('market_units')) / share_market_units * 100) if share_market_units else 0
        market_units = _number(row.get('market_units'))
        your_units = _number(row.get('your_units'))
        row['market_avg_price'] = (_number(row.get('market_sales')) / market_units) if market_units else 0
        row['your_avg_price'] = (_number(row.get('your_sales')) / your_units) if your_units else 0
        status = _status_for_row(
            row,
            matched_item,
            float(row['market_value_share_pct']),
            float(row['market_unit_share_pct']),
            recommended_add_value_share_pct=recommendation_thresholds['value_share_pct'],
        )
        row['assortment_status'] = status
        row_price_position = _price_position_for_row(row)
        row['price_position'] = row_price_position

    assortment_status_norm = (assortment_status or '').strip()
    price_position_norm = (price_position or '').strip()
    if assortment_status_norm:
        rows = [row for row in rows if str((row.get('assortment_status') or {}).get('code') or '') == assortment_status_norm]
    if price_position_norm:
        rows = [row for row in rows if str((row.get('price_position') or {}).get('code') or '') == price_position_norm]

    status_counts: dict[str, int] = {}
    price_counts: dict[str, int] = {}
    for row in rows:
        status = row.get('assortment_status') or {}
        status_counts[str(status.get('code') or 'unknown')] = status_counts.get(str(status.get('code') or 'unknown'), 0) + 1
        price = row.get('price_position') or {}
        price_counts[str(price.get('code') or 'unknown')] = price_counts.get(str(price.get('code') or 'unknown'), 0) + 1

    sort_key = sort if sort in (_NUMERIC_FIELDS | {
        'gap_sales',
        'gap_units',
        'brand',
        'category',
        'product_name',
        'market_avg_price',
        'your_avg_price',
        'price_diff_amount',
        'price_diff_pct',
        'market_value_share_pct',
        'market_unit_share_pct',
    }) else 'market_sales'
    reverse = direction.lower() != 'asc'

    def _sort_value(row: dict[str, object]) -> object:
        if sort_key == 'price_diff_amount':
            return _number((row.get('price_position') or {}).get('diff_amount'))
        if sort_key == 'price_diff_pct':
            return _number((row.get('price_position') or {}).get('diff_pct'))
        if sort_key in _NUMERIC_FIELDS or sort_key.startswith('gap_') or sort_key.endswith('_pct') or sort_key.endswith('_price'):
            return _number(row.get(sort_key))
        return str(row.get(sort_key) or '')

    rows.sort(key=_sort_value, reverse=reverse)

    total_your_sales = sum(_number(row.get('your_sales')) for row in rows)
    filtered_market_sales = sum(_number(row.get('market_sales')) for row in rows)
    total_your_units = sum(_number(row.get('your_units')) for row in rows)
    filtered_market_units = sum(_number(row.get('market_units')) for row in rows)
    start = max(page - 1, 0) * page_size
    end = start + page_size

    return {
        'file': str(snapshot.source_filename or ''),
        'snapshot_id': str(snapshot.id),
        'source': 'db',
        'period': period,
        'season': season,
        'summary': {
            'products': len(rows),
            'brands': len({str(row.get('brand') or '') for row in rows if row.get('brand')}),
            'categories': len({str(row.get('category') or '') for row in rows if row.get('category')}),
            'market_sales': filtered_market_sales,
            'your_sales': total_your_sales,
            'your_sales_ms': (total_your_sales / filtered_market_sales * 100) if filtered_market_sales else 0,
            'market_units': filtered_market_units,
            'your_units': total_your_units,
            'your_units_ms': (total_your_units / filtered_market_units * 100) if filtered_market_units else 0,
            'gap_sales': max(filtered_market_sales - total_your_sales, 0),
            'gap_units': max(filtered_market_units - total_your_units, 0),
            'status_counts': status_counts,
            'price_counts': price_counts,
            'recommendation_thresholds': recommendation_thresholds,
        },
        'filters': {'brands': brands, 'categories': categories},
        'pagination': {'page': page, 'page_size': page_size, 'total': len(rows)},
        'rows': rows[start:end],
    }
