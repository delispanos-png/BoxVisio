from celery import Celery
from fastapi import APIRouter, Depends, Header, HTTPException, Request, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.db.control_session import get_control_db
from app.models.control import PlanName, Tenant, TenantApiKey, TenantStatus
from app.schemas.ingest import IngestBatchRequest
from app.services.hmac_auth import verify_hmac_signature
from app.services.ingestion import enqueue_tenant_job

router = APIRouter(prefix='/v1/ingest', tags=['ingestion'])
celery_client = Celery('ingest_sender', broker=settings.celery_broker_url)


async def get_ingest_tenant(
    x_api_key: str = Header(alias='X-API-Key'),
    x_tenant: str = Header(alias='X-Tenant'),
    db: AsyncSession = Depends(get_control_db),
) -> tuple[Tenant, TenantApiKey]:
    stmt = (
        select(Tenant, TenantApiKey)
        .join(TenantApiKey, TenantApiKey.tenant_id == Tenant.id)
        .where(
            TenantApiKey.key_id == x_api_key,
            TenantApiKey.is_active.is_(True),
            Tenant.slug == x_tenant,
        )
    )
    row = (await db.execute(stmt)).first()
    if not row:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail='Invalid API key/tenant')

    tenant, api_key = row
    if tenant.status != TenantStatus.active:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail='Tenant inactive')
    return tenant, api_key


def _enqueue_batch(entity: str, tenant: Tenant, payload: IngestBatchRequest) -> None:
    serialized = {'records': [record.model_dump(mode='json') for record in payload.records]}
    enqueue_tenant_job(
        tenant.slug,
        {
            'connector': 'external_api',
            'entity': entity,
            'tenant_slug': tenant.slug,
            'payload': serialized,
            'attempt': 0,
            'max_retries': settings.ingest_job_max_retries,
        },
    )
    celery_client.send_task(
        'worker.tasks.drain_tenant_ingest_queue',
        kwargs={'tenant_slug': tenant.slug},
        queue='ingest',
    )


@router.post('/sales')
async def ingest_sales(
    request: Request,
    payload: IngestBatchRequest,
    ctx: tuple[Tenant, TenantApiKey] = Depends(get_ingest_tenant),
    x_signature: str = Header(alias='X-Signature'),
):
    tenant, api_key = ctx
    body = await request.body()
    if not verify_hmac_signature(api_key.key_secret, body, x_signature):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail='Invalid signature')

    _enqueue_batch('sales', tenant, payload)
    return {
        'status': 'queued',
        'tenant': tenant.slug,
        'records': len(payload.records),
        'queue': f'ingest:{tenant.slug}',
    }


@router.post('/purchases')
async def ingest_purchases(
    request: Request,
    payload: IngestBatchRequest,
    ctx: tuple[Tenant, TenantApiKey] = Depends(get_ingest_tenant),
    x_signature: str = Header(alias='X-Signature'),
):
    tenant, api_key = ctx
    if tenant.plan == PlanName.standard:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail='Plan does not allow purchases ingestion')
    body = await request.body()
    if not verify_hmac_signature(api_key.key_secret, body, x_signature):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail='Invalid signature')

    _enqueue_batch('purchases', tenant, payload)
    return {
        'status': 'queued',
        'tenant': tenant.slug,
        'records': len(payload.records),
        'queue': f'ingest:{tenant.slug}',
    }
