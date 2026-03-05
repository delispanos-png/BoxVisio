import hmac

from fastapi import APIRouter, Depends, Header, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.db.control_session import get_control_db
from app.models.control import Tenant, WhmcsEvent, WhmcsService
from app.schemas.whmcs import WhmcsPayload

router = APIRouter(prefix='/whmcs', tags=['whmcs'])


def _verify_secret(incoming: str | None):
    if not incoming or not hmac.compare_digest(incoming, settings.whmcs_webhook_secret):
        raise HTTPException(status_code=401, detail='Invalid webhook secret')


async def _upsert_mirror_service(db: AsyncSession, payload: WhmcsPayload, status: str):
    svc = (await db.execute(select(WhmcsService).where(WhmcsService.service_id == payload.service_id))).scalar_one_or_none()
    if not svc:
        svc = WhmcsService(service_id=payload.service_id, status=status)
        db.add(svc)

    svc.product_id = payload.product_id
    svc.status = status

    # Optional link by tenant_slug if exists. WHMCS never provisions/controls access.
    if payload.tenant_slug:
        tenant = (await db.execute(select(Tenant).where(Tenant.slug == payload.tenant_slug))).scalar_one_or_none()
        if tenant:
            svc.tenant_id = tenant.id


@router.post('/provision')
async def whmcs_provision(
    payload: WhmcsPayload,
    db: AsyncSession = Depends(get_control_db),
    x_whmcs_secret: str | None = Header(default=None, alias='X-WHMCS-Secret'),
):
    _verify_secret(x_whmcs_secret)
    await _upsert_mirror_service(db, payload, status='active')
    db.add(WhmcsEvent(service_id=payload.service_id, event_type='provision_mirror', payload=payload.model_dump()))
    await db.commit()
    return {'status': 'mirrored', 'source': 'whmcs_optional'}


@router.post('/suspend')
async def whmcs_suspend(
    payload: WhmcsPayload,
    db: AsyncSession = Depends(get_control_db),
    x_whmcs_secret: str | None = Header(default=None, alias='X-WHMCS-Secret'),
):
    _verify_secret(x_whmcs_secret)
    await _upsert_mirror_service(db, payload, status='suspended')
    db.add(WhmcsEvent(service_id=payload.service_id, event_type='suspend_mirror', payload=payload.model_dump()))
    await db.commit()
    return {'status': 'mirrored', 'source': 'whmcs_optional'}


@router.post('/terminate')
async def whmcs_terminate(
    payload: WhmcsPayload,
    db: AsyncSession = Depends(get_control_db),
    x_whmcs_secret: str | None = Header(default=None, alias='X-WHMCS-Secret'),
):
    _verify_secret(x_whmcs_secret)
    await _upsert_mirror_service(db, payload, status='terminated')
    db.add(WhmcsEvent(service_id=payload.service_id, event_type='terminate_mirror', payload=payload.model_dump()))
    await db.commit()
    return {'status': 'mirrored', 'source': 'whmcs_optional'}


@router.post('/upgrade')
async def whmcs_upgrade(
    payload: WhmcsPayload,
    db: AsyncSession = Depends(get_control_db),
    x_whmcs_secret: str | None = Header(default=None, alias='X-WHMCS-Secret'),
):
    _verify_secret(x_whmcs_secret)
    await _upsert_mirror_service(db, payload, status='active')
    db.add(WhmcsEvent(service_id=payload.service_id, event_type='upgrade_mirror', payload=payload.model_dump()))
    await db.commit()
    return {'status': 'mirrored', 'source': 'whmcs_optional'}
