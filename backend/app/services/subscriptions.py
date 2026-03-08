from datetime import datetime, timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.models.control import (
    AuditLog,
    PlanFeature,
    PlanName,
    Subscription,
    SubscriptionStatus,
    Tenant,
    TenantStatus,
)


async def get_or_create_subscription(db: AsyncSession, tenant: Tenant) -> Subscription:
    sub = (await db.execute(select(Subscription).where(Subscription.tenant_id == tenant.id))).scalar_one_or_none()
    if sub:
        return sub

    sub = Subscription(
        tenant_id=tenant.id,
        plan=tenant.plan,
        status=tenant.subscription_status,
        trial_starts_at=tenant.created_at if tenant.subscription_status == SubscriptionStatus.trial else None,
        trial_ends_at=tenant.trial_ends_at,
        current_period_start=tenant.created_at if tenant.subscription_status in {SubscriptionStatus.active, SubscriptionStatus.past_due} else None,
        current_period_end=tenant.current_period_end,
        feature_flags=tenant.feature_flags or {},
        canceled_at=tenant.canceled_at,
    )
    db.add(sub)
    await db.flush()
    return sub


async def sync_tenant_from_subscription(db: AsyncSession, tenant: Tenant, sub: Subscription) -> None:
    tenant.plan = sub.plan
    tenant.subscription_status = sub.status
    tenant.trial_ends_at = sub.trial_ends_at
    tenant.current_period_end = sub.current_period_end
    tenant.canceled_at = sub.canceled_at
    if sub.status in {SubscriptionStatus.suspended, SubscriptionStatus.canceled}:
        tenant.status = TenantStatus.suspended
    elif tenant.status == TenantStatus.suspended:
        tenant.status = TenantStatus.active


async def apply_subscription_time_transitions(db: AsyncSession, tenant: Tenant, sub: Subscription) -> bool:
    now = datetime.utcnow()
    prev = sub.status

    if sub.status == SubscriptionStatus.trial and sub.trial_ends_at and sub.trial_ends_at < now:
        sub.status = SubscriptionStatus.suspended
        sub.suspended_at = now
    elif sub.status == SubscriptionStatus.active and sub.current_period_end and sub.current_period_end < now:
        sub.status = SubscriptionStatus.past_due
    elif (
        sub.status == SubscriptionStatus.past_due
        and sub.current_period_end
        and (sub.current_period_end + timedelta(days=settings.past_due_grace_days)) < now
    ):
        sub.status = SubscriptionStatus.suspended
        sub.suspended_at = now

    if sub.status != prev:
        db.add(
            AuditLog(
                tenant_id=tenant.id,
                action='subscription_auto_transition',
                entity_type='subscription',
                entity_id=str(sub.id),
                payload={'from': prev.value, 'to': sub.status.value},
            )
        )
        await sync_tenant_from_subscription(db, tenant, sub)
        return True
    return False


async def is_feature_enabled(
    db: AsyncSession,
    tenant: Tenant,
    sub: Subscription,
    feature: str,
) -> bool:
    # Plan-level DB feature flags.
    row = (
        await db.execute(
            select(PlanFeature).where(
                PlanFeature.plan == sub.plan,
                PlanFeature.feature_name == feature,
            )
        )
    ).scalar_one_or_none()
    if row is None:
        plan_default = infer_default_features_for_plan(sub.plan).get(feature, False)
    else:
        plan_default = bool(row.enabled)

    override = (sub.feature_flags or {}).get(feature)
    if override is None:
        return plan_default
    return bool(override)


def infer_default_features_for_plan(plan: PlanName) -> dict[str, bool]:
    if plan == PlanName.standard:
        return {'sales': True, 'purchases': False, 'inventory': False, 'cashflows': False}
    if plan == PlanName.pro:
        return {'sales': True, 'purchases': True, 'inventory': False, 'cashflows': False}
    return {'sales': True, 'purchases': True, 'inventory': True, 'cashflows': True}
