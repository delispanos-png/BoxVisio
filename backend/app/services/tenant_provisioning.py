import logging
import secrets
import string
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.security import get_password_hash
from app.db.tenant_manager import tenant_db_name
from app.models.control import (
    AuditLog,
    Plan,
    PlanName,
    ProfessionalProfile,
    RoleName,
    Subscription,
    SubscriptionLimit,
    SubscriptionStatus,
    Tenant,
    TenantApiKey,
    TenantStatus,
    User,
)
from app.services.subscriptions import infer_default_features_for_plan

logger = logging.getLogger(__name__)

PRODUCT_PLAN_MAP = {
    '1': PlanName.standard,
    '2': PlanName.pro,
    '3': PlanName.enterprise,
}


def _rand_secret(size: int = 40) -> str:
    alphabet = string.ascii_letters + string.digits
    return ''.join(secrets.choice(alphabet) for _ in range(size))


async def _required_profile_id_by_code(db: AsyncSession, code: str) -> int:
    profile = (
        await db.execute(select(ProfessionalProfile).where(ProfessionalProfile.profile_code == str(code).upper()))
    ).scalar_one_or_none()
    if not profile:
        raise RuntimeError(f'missing professional profile seed: {code}')
    return int(profile.id)


def map_product_to_plan(product_id: str | None) -> PlanName:
    if not product_id:
        return PlanName.standard
    return PRODUCT_PLAN_MAP.get(str(product_id), PlanName.standard)


async def create_tenant_bundle(
    db: AsyncSession,
    tenant_name: str,
    tenant_slug: str,
    admin_email: str,
    plan: PlanName,
    source: str = 'external',
    subscription_status: SubscriptionStatus = SubscriptionStatus.trial,
    trial_days: int | None = None,
    whmcs_service_id: str | None = None,
    whmcs_product_id: str | None = None,
) -> Tenant:
    existing = await db.execute(select(Tenant).where(Tenant.slug == tenant_slug))
    tenant = existing.scalar_one_or_none()
    if tenant:
        features = infer_default_features_for_plan(plan)
        tenant.status = TenantStatus.active
        tenant.plan = plan
        tenant.feature_flags = features
        sub = (await db.execute(select(Subscription).where(Subscription.tenant_id == tenant.id))).scalar_one_or_none()
        if sub:
            sub.plan = plan
            sub.feature_flags = features
            sub.status = subscription_status
            if sub.status == SubscriptionStatus.trial:
                sub.trial_starts_at = datetime.utcnow()
                sub.trial_ends_at = datetime.utcnow() + timedelta(days=(trial_days or settings.default_trial_days))
            db.add(sub)
        await db.commit()
        return tenant

    db_user = f"u_{tenant_slug[:30]}"
    db_password = _rand_secret(24)
    db_name = tenant_db_name(tenant_slug)

    plan_features = infer_default_features_for_plan(plan)
    effective_trial_days = settings.default_trial_days if trial_days is None else trial_days

    trial_ends = (
        datetime.utcnow() + timedelta(days=effective_trial_days)
        if subscription_status == SubscriptionStatus.trial
        else None
    )
    period_end = (
        datetime.utcnow() + timedelta(days=30)
        if subscription_status in (SubscriptionStatus.active, SubscriptionStatus.past_due)
        else None
    )

    tenant = Tenant(
        name=tenant_name,
        slug=tenant_slug,
        plan=plan,
        status=TenantStatus.active,
        source=source,
        subscription_status=subscription_status,
        trial_ends_at=trial_ends,
        current_period_end=period_end,
        db_name=db_name,
        db_user=db_user,
        db_password=db_password,
        whmcs_service_id=whmcs_service_id,
        whmcs_product_id=whmcs_product_id,
        feature_flags=plan_features,
    )
    db.add(tenant)
    await db.flush()
    manager_profile_id = await _required_profile_id_by_code(db, 'MANAGER')

    user = User(
        tenant_id=tenant.id,
        professional_profile_id=manager_profile_id,
        email=admin_email,
        password_hash=get_password_hash(_rand_secret(32)),
        role=RoleName.tenant_admin,
        reset_token=secrets.token_urlsafe(24),
        reset_token_expires_at=datetime.utcnow() + timedelta(days=2),
        is_active=True,
    )
    db.add(user)

    api_key = TenantApiKey(
        tenant_id=tenant.id,
        key_id=secrets.token_urlsafe(16),
        key_secret=_rand_secret(40),
        is_active=True,
    )
    db.add(api_key)

    sub = Subscription(
        tenant_id=tenant.id,
        plan=plan,
        status=subscription_status,
        trial_starts_at=datetime.utcnow() if subscription_status == SubscriptionStatus.trial else None,
        trial_ends_at=trial_ends,
        current_period_start=datetime.utcnow() if subscription_status in (SubscriptionStatus.active, SubscriptionStatus.past_due) else None,
        current_period_end=period_end,
        feature_flags=plan_features,
    )
    db.add(sub)
    await db.flush()

    plan_row = (await db.execute(select(Plan).where(Plan.code == plan.value))).scalar_one_or_none()
    max_users = plan_row.max_users if plan_row else 5
    max_branches = plan_row.max_branches if plan_row else 5
    db.add(SubscriptionLimit(subscription_id=sub.id, limit_key='max_users', limit_value=max_users, used_value=0))
    db.add(SubscriptionLimit(subscription_id=sub.id, limit_key='max_branches', limit_value=max_branches, used_value=0))
    db.add(
        AuditLog(
            tenant_id=tenant.id,
            action='tenant_created',
            entity_type='tenant',
            entity_id=str(tenant.id),
            payload={'plan': plan.value, 'subscription_status': subscription_status.value},
        )
    )

    await db.commit()
    await db.refresh(tenant)

    _run_local_script('create_tenant_db.py', tenant.slug)
    _run_local_script('run_tenant_migrations.py', tenant.slug)

    return tenant


def _run_local_script(script_name: str, tenant_slug: str) -> None:
    project_root = Path(__file__).resolve().parents[3]
    script_path = project_root / 'scripts' / script_name
    if not script_path.exists():
        logger.warning('script_missing', extra={'script': str(script_path)})
        return
    subprocess.run(['python', str(script_path), '--tenant', tenant_slug], check=False)
