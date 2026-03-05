from __future__ import annotations

from datetime import date
from decimal import Decimal
from uuid import UUID

from sqlalchemy import func, select, or_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.tenant import DimItem, DimSupplier, FactPurchases, FactSales, SupplierTarget, SupplierTargetItem


def _to_float(v: object | None) -> float:
    if v is None:
        return 0.0
    if isinstance(v, Decimal):
        return float(v)
    try:
        return float(v)  # type: ignore[arg-type]
    except Exception:
        return 0.0


def _month_start_end(year: int, month: int) -> tuple[date, date]:
    from_dt = date(year, month, 1)
    if month == 12:
        return from_dt, date(year, 12, 31)
    return from_dt, date(year, month + 1, 1).fromordinal(date(year, month + 1, 1).toordinal() - 1)


async def supplier_target_filter_options(
    db: AsyncSession,
    supplier_ext_id: str | None = None,
) -> dict[str, list[dict[str, str]]]:
    suppliers = (
        await db.execute(
            select(DimSupplier.external_id, DimSupplier.name)
            .where(DimSupplier.external_id.is_not(None))
            .order_by(DimSupplier.name.asc())
        )
    ).all()
    if supplier_ext_id:
        items = (
            await db.execute(
                select(
                    DimItem.external_id,
                    func.coalesce(DimItem.name, DimItem.external_id),
                )
                .select_from(FactPurchases)
                .join(DimItem, DimItem.external_id == FactPurchases.item_code)
                .join(DimSupplier, FactPurchases.supplier_id == DimSupplier.id, isouter=True)
                .where(
                    DimItem.external_id.is_not(None),
                    or_(
                        FactPurchases.supplier_ext_id == supplier_ext_id,
                        DimSupplier.external_id == supplier_ext_id,
                        DimSupplier.name == supplier_ext_id,
                    ),
                )
                .group_by(DimItem.external_id, DimItem.name)
                .order_by(func.coalesce(DimItem.name, DimItem.external_id).asc())
            )
        ).all()
    else:
        items = (
            await db.execute(
                select(DimItem.external_id, func.coalesce(DimItem.name, DimItem.external_id))
                .where(DimItem.external_id.is_not(None))
                .order_by(func.coalesce(DimItem.name, DimItem.external_id).asc())
            )
        ).all()
    if supplier_ext_id:
        item_payload = [
            {
                'value': str(r[0]),
                'label': str(r[1] or r[0]),
                'supplier_ext_id': str(supplier_ext_id),
            }
            for r in items
        ]
    else:
        item_payload = [
            {
                'value': str(r[0]),
                'label': str(r[1] or r[0]),
                'supplier_ext_id': None,
            }
            for r in items
        ]
    return {
        'suppliers': [{'value': str(r[0]), 'label': str(r[1] or r[0])} for r in suppliers],
        'items': item_payload,
    }


async def _target_items(db: AsyncSession, target_id: UUID) -> list[dict[str, str]]:
    rows = (
        await db.execute(
            select(SupplierTargetItem.item_external_id, SupplierTargetItem.item_name)
            .where(SupplierTargetItem.supplier_target_id == target_id)
            .order_by(SupplierTargetItem.item_name.asc().nullslast(), SupplierTargetItem.item_external_id.asc())
        )
    ).all()
    return [{'item_external_id': str(r[0]), 'item_name': str(r[1] or r[0])} for r in rows]


async def _target_progress(db: AsyncSession, target: SupplierTarget, as_of: date | None = None) -> dict[str, object]:
    year = int(target.target_year)
    end_of_year = date(year, 12, 31)
    cap_to = min(as_of or date.today(), end_of_year)
    from_dt = date(year, 1, 1)

    target_items = await _target_items(db, target.id)
    item_codes = [x['item_external_id'] for x in target_items]
    if not item_codes:
        monthly = [{'month': m, 'actual': 0.0, 'target_progress': 0.0, 'gap': 0.0} for m in range(1, 13)]
        return {
            'items': target_items,
            'year_actual': 0.0,
            'progress_pct': 0.0,
            'remaining_amount': _to_float(target.target_amount),
            'remaining_pct': 100.0 if _to_float(target.target_amount) > 0 else 0.0,
            'potential_credit': 0.0,
            'target_credit': _to_float(target.target_amount) * (_to_float(target.rebate_percent) / 100.0),
            'monthly': monthly,
        }

    rows = (
        await db.execute(
            select(
                func.extract('month', FactSales.doc_date).label('month'),
                func.coalesce(func.sum(FactSales.net_value), 0).label('net_value'),
            )
            .where(
                FactSales.doc_date >= from_dt,
                FactSales.doc_date <= cap_to,
                FactSales.item_code.in_(item_codes),
            )
            .group_by(func.extract('month', FactSales.doc_date))
            .order_by(func.extract('month', FactSales.doc_date))
        )
    ).all()
    by_month = {int(r[0]): _to_float(r[1]) for r in rows}
    year_actual = sum(by_month.values())
    target_amount = _to_float(target.target_amount)
    rebate_pct = _to_float(target.rebate_percent)
    rebate_amount = _to_float(target.rebate_amount)

    monthly: list[dict[str, float | int]] = []
    cumulative = 0.0
    for m in range(1, 13):
        actual = by_month.get(m, 0.0)
        cumulative += actual
        expected = target_amount * (m / 12.0) if target_amount > 0 else 0.0
        monthly.append(
            {
                'month': m,
                'actual': round(actual, 2),
                'target_progress': round(expected, 2),
                'gap': round(cumulative - expected, 2),
            }
        )

    year_days = 366 if (year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)) else 365
    elapsed_ratio = max(0.0, min(1.0, cap_to.timetuple().tm_yday / year_days))
    expected_to_date = target_amount * elapsed_ratio if target_amount > 0 else 0.0
    trajectory_gap_amount = year_actual - expected_to_date
    trajectory_gap_pct = ((year_actual / expected_to_date - 1.0) * 100.0) if expected_to_date > 0 else 0.0

    progress_pct = (year_actual / target_amount * 100.0) if target_amount > 0 else 0.0
    remaining = max(0.0, target_amount - year_actual)
    remaining_pct = (remaining / target_amount * 100.0) if target_amount > 0 else 0.0
    return {
        'items': target_items,
        'year_actual': round(year_actual, 2),
        'progress_pct': round(progress_pct, 2),
        'remaining_amount': round(remaining, 2),
        'remaining_pct': round(remaining_pct, 2),
        'potential_credit': round((year_actual * rebate_pct / 100.0) + (rebate_amount if year_actual >= target_amount and target_amount > 0 else 0.0), 2),
        'target_credit': round((target_amount * rebate_pct / 100.0) + rebate_amount, 2),
        'expected_to_date': round(expected_to_date, 2),
        'trajectory_gap_amount': round(trajectory_gap_amount, 2),
        'trajectory_gap_pct': round(trajectory_gap_pct, 2),
        'monthly': monthly,
    }


async def list_supplier_targets(db: AsyncSession, year: int, as_of: date | None = None) -> list[dict[str, object]]:
    targets = (
        await db.execute(
            select(SupplierTarget)
            .where(SupplierTarget.target_year == year, SupplierTarget.is_active.is_(True))
            .order_by(SupplierTarget.supplier_name.asc().nullslast(), SupplierTarget.name.asc(), SupplierTarget.created_at.desc())
        )
    ).scalars().all()
    payload: list[dict[str, object]] = []
    for t in targets:
        progress = await _target_progress(db, t, as_of=as_of)
        payload.append(
            {
                'id': str(t.id),
                'name': t.name,
                'supplier_ext_id': t.supplier_ext_id,
                'supplier_name': t.supplier_name or t.supplier_ext_id,
                'target_year': t.target_year,
                'target_amount': round(_to_float(t.target_amount), 2),
                'rebate_percent': round(_to_float(t.rebate_percent), 4),
                'rebate_amount': round(_to_float(t.rebate_amount), 2),
                'notes': t.notes or '',
                'is_active': bool(t.is_active),
                **progress,
            }
        )
    return payload


async def create_supplier_target(
    db: AsyncSession,
    *,
    name: str,
    supplier_ext_id: str,
    supplier_name: str | None,
    target_year: int,
    target_amount: float,
    rebate_percent: float,
    rebate_amount: float = 0.0,
    item_external_ids: list[str],
    notes: str | None = None,
) -> dict[str, object]:
    target = SupplierTarget(
        name=name.strip() or 'Default Target',
        supplier_ext_id=supplier_ext_id.strip(),
        supplier_name=(supplier_name or '').strip() or None,
        target_year=int(target_year),
        target_amount=max(0.0, float(target_amount)),
        rebate_percent=max(0.0, float(rebate_percent)),
        rebate_amount=max(0.0, float(rebate_amount)),
        notes=(notes or '').strip() or None,
        is_active=True,
    )
    db.add(target)
    try:
        await db.flush()
    except IntegrityError as exc:
        await db.rollback()
        raise ValueError('duplicate_target') from exc

    clean_items = sorted(set([x.strip() for x in item_external_ids if x and x.strip()]))
    if clean_items:
        names = (
            await db.execute(select(DimItem.external_id, DimItem.name).where(DimItem.external_id.in_(clean_items)))
        ).all()
        name_map = {str(r[0]): str(r[1] or r[0]) for r in names}
        for item_code in clean_items:
            db.add(
                SupplierTargetItem(
                    supplier_target_id=target.id,
                    item_external_id=item_code,
                    item_name=name_map.get(item_code, item_code),
                )
            )

    try:
        await db.commit()
    except IntegrityError as exc:
        await db.rollback()
        raise ValueError('duplicate_target') from exc
    await db.refresh(target)
    return {
        'id': str(target.id),
        'name': target.name,
    }


async def update_supplier_target(
    db: AsyncSession,
    *,
    target_id: UUID,
    name: str | None = None,
    supplier_ext_id: str | None = None,
    supplier_name: str | None = None,
    target_year: int | None = None,
    target_amount: float | None = None,
    rebate_percent: float | None = None,
    rebate_amount: float | None = None,
    item_external_ids: list[str] | None = None,
    notes: str | None = None,
    is_active: bool | None = None,
) -> bool:
    target = (await db.execute(select(SupplierTarget).where(SupplierTarget.id == target_id))).scalar_one_or_none()
    if target is None:
        return False

    if name is not None:
        target.name = name.strip() or target.name
    if supplier_ext_id is not None:
        target.supplier_ext_id = supplier_ext_id.strip() or target.supplier_ext_id
    if supplier_name is not None:
        target.supplier_name = supplier_name.strip() or target.supplier_name
    if target_year is not None:
        target.target_year = int(target_year)
    if target_amount is not None:
        target.target_amount = max(0.0, float(target_amount))
    if rebate_percent is not None:
        target.rebate_percent = max(0.0, float(rebate_percent))
    if rebate_amount is not None:
        target.rebate_amount = max(0.0, float(rebate_amount))
    if notes is not None:
        target.notes = notes.strip() or None
    if is_active is not None:
        target.is_active = bool(is_active)

    if item_external_ids is not None:
        await db.execute(
            SupplierTargetItem.__table__.delete().where(SupplierTargetItem.supplier_target_id == target_id)
        )
        clean_items = sorted(set([x.strip() for x in item_external_ids if x and x.strip()]))
        if clean_items:
            names = (
                await db.execute(select(DimItem.external_id, DimItem.name).where(DimItem.external_id.in_(clean_items)))
            ).all()
            name_map = {str(r[0]): str(r[1] or r[0]) for r in names}
            for item_code in clean_items:
                db.add(
                    SupplierTargetItem(
                        supplier_target_id=target_id,
                        item_external_id=item_code,
                        item_name=name_map.get(item_code, item_code),
                    )
                )

    try:
        await db.commit()
    except IntegrityError as exc:
        await db.rollback()
        raise ValueError('duplicate_target') from exc
    return True
