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
from app.models.tenant import IqviaLine, IqviaSnapshot


_DEFAULT_PHARMACY295_FILE = Path('/opt/cloudon-bi/IQVIA-PHARMACY295_202603.xlsx')
_XLSX_NS = {
    'a': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main',
    'r': 'http://schemas.openxmlformats.org/officeDocument/2006/relationships',
}
_TEXT_FIELDS = (
    'category',
    'atc3',
    'otc3',
    'corporation',
    'manufacturer',
    'product',
    'pack',
    'area_code',
    'area_name',
    'territory_code',
    'territory_name',
)
_NUMERIC_FIELDS = ('units', 'values')
_REQUIRED_HEADERS = (
    'CATEGORY',
    'ATC3',
    'OTC3',
    'CORPORATION',
    'MANUFACTURER',
    'PRODUCT',
    'PACK',
    'AREA CODE',
    'AREA NAME',
    'TERRITORY CODE',
    'TERRITORY NAME',
)
_FIELD_MAP = {
    'CATEGORY': 'category',
    'ATC3': 'atc3',
    'OTC3': 'otc3',
    'CORPORATION': 'corporation',
    'MANUFACTURER': 'manufacturer',
    'PRODUCT': 'product',
    'PACK': 'pack',
    'AREA CODE': 'area_code',
    'AREA NAME': 'area_name',
    'TERRITORY CODE': 'territory_code',
    'TERRITORY NAME': 'territory_name',
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
    cfg = flags.get('iqvia_config') or flags.get('iqvia')
    if isinstance(cfg, dict) and cfg.get('file_path'):
        return Path(str(cfg['file_path']))
    if str(getattr(tenant, 'slug', '')).lower() == 'pharmacy295':
        return _DEFAULT_PHARMACY295_FILE
    return None


def iqvia_period_from_filename(filename: str | Path | None) -> str:
    """Return the reporting period as YYYYMM from the final filename token.

    IQVIA files are expected as ``PHARMACY295_202603.xlsx``. The last
    underscore-separated part is the business month stored on the snapshot.
    """
    stem = Path(str(filename or '')).stem
    token = re.split(r'[_\s-]+', stem)[-1].strip()
    match = re.fullmatch(r'(20\d{2})(0[1-9]|1[0-2])', token)
    if not match:
        match = re.search(r'(20\d{2})(0[1-9]|1[0-2])', stem)
    return match.group(0) if match else ''


def _period_label(period: str) -> str:
    if re.fullmatch(r'20\d{4}', period or ''):
        return f'{period[4:6]}/{period[:4]}'
    return period or '-'


def _column_index(cell_ref: str) -> int:
    letters = re.sub(r'\d+', '', cell_ref or '').upper()
    value = 0
    for char in letters:
        value = value * 26 + (ord(char) - 64)
    return max(value - 1, 0)


def _read_shared_strings(archive: zipfile.ZipFile) -> list[str]:
    if 'xl/sharedStrings.xml' not in archive.namelist():
        return []
    root = ET.fromstring(archive.read('xl/sharedStrings.xml'))
    return [''.join(t.text or '' for t in item.findall('.//a:t', _XLSX_NS)) for item in root.findall('a:si', _XLSX_NS)]


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
    text = str(value).strip().replace('%', '').replace('€', '').replace(' ', '')
    if not text:
        return 0.0
    if ',' in text and '.' in text:
        text = text.replace('.', '').replace(',', '.')
    elif ',' in text:
        text = text.replace(',', '.')
    try:
        return float(text)
    except ValueError:
        return 0.0


def _worksheet_paths(archive: zipfile.ZipFile) -> list[tuple[str, str]]:
    workbook = ET.fromstring(archive.read('xl/workbook.xml'))
    rels = ET.fromstring(archive.read('xl/_rels/workbook.xml.rels'))
    rid_to_target = {rel.attrib['Id']: rel.attrib['Target'] for rel in rels}
    sheets: list[tuple[str, str]] = []
    for sheet in workbook.findall('.//a:sheets/a:sheet', _XLSX_NS):
        name = sheet.attrib.get('name') or 'Sheet'
        rid = sheet.attrib.get('{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id') or ''
        target = rid_to_target.get(rid, '')
        path = target[1:] if target.startswith('/') else f'xl/{target}' if not target.startswith('xl/') else target
        if path in archive.namelist():
            sheets.append((name, path))
    return sheets


@lru_cache(maxsize=16)
def _load_xlsx_rows(path_str: str, mtime_ns: int) -> tuple[dict[str, object], ...]:
    del mtime_ns
    path = Path(path_str)
    with zipfile.ZipFile(path) as archive:
        shared_strings = _read_shared_strings(archive)
        worksheets = _worksheet_paths(archive)
        if not worksheets:
            raise KeyError('worksheet')
        sheet_name, worksheet_path = worksheets[0]
        root = ET.fromstring(archive.read(worksheet_path))
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

    raw_headers = [str(header or '').strip() for header in parsed_rows[0]]
    header_map: dict[int, str] = {}
    for idx, header in enumerate(raw_headers):
        upper = header.upper()
        if upper in _FIELD_MAP:
            header_map[idx] = _FIELD_MAP[upper]
        elif upper.startswith('UNITS '):
            header_map[idx] = 'units'
        elif upper.startswith('VALUES '):
            header_map[idx] = 'values'

    rows: list[dict[str, object]] = []
    for values in parsed_rows[1:]:
        row: dict[str, object] = {'sheet': sheet_name}
        for idx, key in header_map.items():
            row[key] = values[idx] if idx < len(values) else ''
        if not any(row.get(field) for field in (*_TEXT_FIELDS, *_NUMERIC_FIELDS)):
            continue
        for field in _TEXT_FIELDS:
            row[field] = str(row.get(field) or '').strip()
        for field in _NUMERIC_FIELDS:
            row[field] = _number(row.get(field))
        units = _number(row.get('units'))
        values_sum = _number(row.get('values'))
        row['avg_price'] = values_sum / units if units else 0.0
        row['product_label'] = ' '.join(part for part in (str(row.get('product') or ''), str(row.get('pack') or '')) if part).strip()
        rows.append(row)
    return tuple(rows)


def clear_iqvia_cache() -> None:
    _load_xlsx_rows.cache_clear()


def validate_iqvia_file(path: Path) -> dict[str, object]:
    try:
        rows = _load_xlsx_rows(str(path), path.stat().st_mtime_ns)
    except zipfile.BadZipFile as exc:
        raise ValueError('Το αρχείο δεν είναι έγκυρο XLSX.') from exc
    except KeyError as exc:
        raise ValueError('Το XLSX δεν έχει το αναμενόμενο φύλλο δεδομένων.') from exc
    if not rows:
        raise ValueError('Το αρχείο δεν περιέχει γραμμές IQVIA.')

    with zipfile.ZipFile(path) as archive:
        shared_strings = _read_shared_strings(archive)
        worksheets = _worksheet_paths(archive)
        root = ET.fromstring(archive.read(worksheets[0][1]))
        first_row = root.find('.//a:sheetData/a:row', _XLSX_NS)
        headers: list[str] = []
        if first_row is not None:
            for cell in first_row.findall('a:c', _XLSX_NS):
                idx = _column_index(cell.attrib.get('r', 'A1'))
                while len(headers) <= idx:
                    headers.append('')
                headers[idx] = _cell_text(cell, shared_strings)
    missing = [header for header in _REQUIRED_HEADERS if header not in headers]
    has_units = any(str(header).upper().startswith('UNITS ') for header in headers)
    has_values = any(str(header).upper().startswith('VALUES ') for header in headers)
    if missing or not has_units or not has_values:
        raise ValueError('Λείπουν στήλες από το IQVIA Excel.')

    return {
        'rows': len(rows),
        'categories': len({str(row.get('category') or '') for row in rows if row.get('category')}),
        'manufacturers': len({str(row.get('manufacturer') or '') for row in rows if row.get('manufacturer')}),
        'territories': len({str(row.get('territory_name') or '') for row in rows if row.get('territory_name')}),
        'period': iqvia_period_from_filename(path),
    }


async def import_iqvia_file(
    db: AsyncSession,
    path: Path,
    *,
    source_filename: str | None = None,
    source_sha256: str | None = None,
    imported_by: str | None = None,
) -> dict[str, object]:
    validation = validate_iqvia_file(path)
    rows = _load_xlsx_rows(str(path), path.stat().st_mtime_ns)
    period = str(validation.get('period') or iqvia_period_from_filename(source_filename or path))
    checksum = (source_sha256 or file_sha256(path)).strip()
    filename = source_filename or path.name
    duplicate = (
        await db.execute(
            select(IqviaSnapshot)
            .where(IqviaSnapshot.source_sha256 == checksum)
            .order_by(desc(IqviaSnapshot.imported_at))
            .limit(1)
        )
    ).scalar_one_or_none()
    if duplicate is not None:
        raise DuplicateMarketImportError(
            'Το ίδιο IQVIA αρχείο έχει ήδη γίνει import.',
            existing_snapshot_id=str(duplicate.id),
        )
    snapshot = IqviaSnapshot(
        source_filename=filename,
        source_sha256=checksum,
        period_label=period,
        rows_count=len(rows),
        summary_json={
            'categories': validation.get('categories', 0),
            'manufacturers': validation.get('manufacturers', 0),
            'territories': validation.get('territories', 0),
        },
        imported_by=imported_by,
    )
    db.add(snapshot)
    await db.flush()
    await db.execute(
        insert(IqviaLine),
        [
            {
                'snapshot_id': snapshot.id,
                'source_row': idx,
                'category': str(row.get('category') or '')[:255] or None,
                'atc3': str(row.get('atc3') or '')[:255] or None,
                'otc3': str(row.get('otc3') or '')[:255] or None,
                'corporation': str(row.get('corporation') or '')[:255] or None,
                'manufacturer': str(row.get('manufacturer') or '')[:255] or None,
                'product': str(row.get('product') or '')[:500] or None,
                'pack': str(row.get('pack') or '')[:500] or None,
                'product_label': str(row.get('product_label') or '')[:1000] or None,
                'area_code': str(row.get('area_code') or '')[:64] or None,
                'area_name': str(row.get('area_name') or '')[:255] or None,
                'territory_code': str(row.get('territory_code') or '')[:64] or None,
                'territory_name': str(row.get('territory_name') or '')[:255] or None,
                'units': _number(row.get('units')),
                'values': _number(row.get('values')),
                'avg_price': _number(row.get('avg_price')),
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


async def _iqvia_snapshots(db: AsyncSession) -> list[IqviaSnapshot]:
    result = await db.execute(select(IqviaSnapshot).order_by(desc(IqviaSnapshot.period_label), desc(IqviaSnapshot.imported_at)))
    return list(result.scalars())


async def _latest_iqvia_snapshot(db: AsyncSession, period: str | None = None) -> IqviaSnapshot:
    stmt = select(IqviaSnapshot)
    if period:
        stmt = stmt.where(IqviaSnapshot.period_label == period)
    snapshot = (
        await db.execute(stmt.order_by(desc(IqviaSnapshot.imported_at)).limit(1))
    ).scalar_one_or_none()
    if snapshot is None:
        raise FileNotFoundError('Δεν έχει γίνει import IQVIA στη βάση του tenant για την περίοδο.')
    return snapshot


async def _load_rows_from_db(db: AsyncSession, snapshot_id) -> list[dict[str, object]]:
    result = await db.execute(select(IqviaLine).where(IqviaLine.snapshot_id == snapshot_id))
    rows: list[dict[str, object]] = []
    for line in result.scalars():
        row = dict(line.raw_json or {})
        row.update(
            {
                'category': line.category or '',
                'atc3': line.atc3 or '',
                'otc3': line.otc3 or '',
                'corporation': line.corporation or '',
                'manufacturer': line.manufacturer or '',
                'product': line.product or '',
                'pack': line.pack or '',
                'product_label': line.product_label or '',
                'area_code': line.area_code or '',
                'area_name': line.area_name or '',
                'territory_code': line.territory_code or '',
                'territory_name': line.territory_name or '',
                'units': _number(line.units),
                'values': _number(line.values),
                'avg_price': _number(line.avg_price),
            }
        )
        rows.append(row)
    return rows


def _matches(row: dict[str, object], q_norm: str) -> bool:
    if not q_norm:
        return True
    haystack = ' '.join(str(row.get(field) or '') for field in ('category', 'atc3', 'otc3', 'corporation', 'manufacturer', 'product', 'pack')).lower()
    return q_norm in haystack


def _top_dimension(rows: list[dict[str, object]], key: str, limit: int = 10) -> list[dict[str, object]]:
    buckets: dict[str, dict[str, object]] = {}
    total_values = sum(_number(row.get('values')) for row in rows)
    total_units = sum(_number(row.get('units')) for row in rows)
    for row in rows:
        label = str(row.get(key) or 'N/A').strip() or 'N/A'
        bucket = buckets.setdefault(label, {'label': label, 'values': 0.0, 'units': 0.0, 'products': 0})
        bucket['values'] = _number(bucket['values']) + _number(row.get('values'))
        bucket['units'] = _number(bucket['units']) + _number(row.get('units'))
        bucket['products'] = int(bucket['products']) + 1
    out = sorted(buckets.values(), key=lambda item: _number(item.get('values')), reverse=True)[:limit]
    for item in out:
        item['value_share_pct'] = (_number(item.get('values')) / total_values * 100) if total_values else 0.0
        item['unit_share_pct'] = (_number(item.get('units')) / total_units * 100) if total_units else 0.0
        item['avg_price'] = (_number(item.get('values')) / _number(item.get('units'))) if _number(item.get('units')) else 0.0
    return out


async def iqvia_report(
    tenant: Tenant,
    db: AsyncSession,
    *,
    q: str | None = None,
    category: str | None = None,
    manufacturer: str | None = None,
    territory: str | None = None,
    atc3: str | None = None,
    otc3: str | None = None,
    period: str | None = None,
    sort: str = 'values',
    direction: str = 'desc',
    page: int = 1,
    page_size: int = 50,
) -> dict[str, object]:
    snapshots = await _iqvia_snapshots(db)
    snapshot = await _latest_iqvia_snapshot(db, (period or '').strip() or None)
    rows = await _load_rows_from_db(db, snapshot.id)
    all_rows = rows
    q_norm = (q or '').strip().lower()
    filters = {
        'category': (category or '').strip(),
        'manufacturer': (manufacturer or '').strip(),
        'territory_name': (territory or '').strip(),
        'atc3': (atc3 or '').strip(),
        'otc3': (otc3 or '').strip(),
    }

    rows = [row for row in rows if _matches(row, q_norm)]
    for key, value in filters.items():
        if value:
            rows = [row for row in rows if str(row.get(key) or '') == value]

    total_values_all = sum(_number(row.get('values')) for row in all_rows)
    total_units_all = sum(_number(row.get('units')) for row in all_rows)
    total_values = sum(_number(row.get('values')) for row in rows)
    total_units = sum(_number(row.get('units')) for row in rows)
    for row in rows:
        row['value_share_pct'] = (_number(row.get('values')) / total_values * 100) if total_values else 0.0
        row['unit_share_pct'] = (_number(row.get('units')) / total_units * 100) if total_units else 0.0
        row['market_value_share_pct'] = (_number(row.get('values')) / total_values_all * 100) if total_values_all else 0.0
        row['market_unit_share_pct'] = (_number(row.get('units')) / total_units_all * 100) if total_units_all else 0.0

    sort_key = sort if sort in {
        'category', 'atc3', 'otc3', 'corporation', 'manufacturer', 'product', 'pack',
        'area_name', 'territory_name', 'units', 'values', 'avg_price', 'value_share_pct', 'unit_share_pct',
    } else 'values'
    reverse = direction.lower() != 'asc'
    rows.sort(key=lambda row: _number(row.get(sort_key)) if sort_key in {'units', 'values', 'avg_price', 'value_share_pct', 'unit_share_pct'} else str(row.get(sort_key) or ''), reverse=reverse)

    start = max(page - 1, 0) * page_size
    end = start + page_size
    flags = tenant.feature_flags if isinstance(tenant.feature_flags, dict) else {}
    cfg = flags.get('iqvia_config') if isinstance(flags.get('iqvia_config'), dict) else {}
    period = str(snapshot.period_label or cfg.get('period') or iqvia_period_from_filename(cfg.get('filename')) or '')

    return {
        'file': str(snapshot.source_filename or ''),
        'snapshot_id': str(snapshot.id),
        'source': 'db',
        'period': period,
        'period_label': _period_label(period),
        'summary': {
            'products': len(rows),
            'categories': len({str(row.get('category') or '') for row in rows if row.get('category')}),
            'manufacturers': len({str(row.get('manufacturer') or '') for row in rows if row.get('manufacturer')}),
            'territories': len({str(row.get('territory_name') or '') for row in rows if row.get('territory_name')}),
            'values': total_values,
            'units': total_units,
            'avg_price': (total_values / total_units) if total_units else 0.0,
            'value_share_pct': (total_values / total_values_all * 100) if total_values_all else 0.0,
            'unit_share_pct': (total_units / total_units_all * 100) if total_units_all else 0.0,
        },
        'filters': {
            'periods': [
                {
                    'value': str(item.period_label or ''),
                    'label': _period_label(str(item.period_label or '')),
                    'filename': item.source_filename or '',
                }
                for item in snapshots
                if item.period_label
            ],
            'categories': sorted({str(row.get('category') or '').strip() for row in all_rows if str(row.get('category') or '').strip()}),
            'manufacturers': sorted({str(row.get('manufacturer') or '').strip() for row in all_rows if str(row.get('manufacturer') or '').strip()}),
            'territories': sorted({str(row.get('territory_name') or '').strip() for row in all_rows if str(row.get('territory_name') or '').strip()}),
            'atc3': sorted({str(row.get('atc3') or '').strip() for row in all_rows if str(row.get('atc3') or '').strip()}),
            'otc3': sorted({str(row.get('otc3') or '').strip() for row in all_rows if str(row.get('otc3') or '').strip()}),
        },
        'breakdowns': {
            'categories': _top_dimension(rows, 'category', 12),
            'manufacturers': _top_dimension(rows, 'manufacturer', 12),
            'territories': _top_dimension(rows, 'territory_name', 12),
            'otc3': _top_dimension(rows, 'otc3', 12),
            'atc3': _top_dimension(rows, 'atc3', 12),
        },
        'pagination': {'page': page, 'page_size': page_size, 'total': len(rows)},
        'rows': rows[start:end],
    }
