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
from app.services.subscription_features import ADD_ON_FEATURE_KEYS, addon_allowed_for_plan, infer_subscription_feature_defaults, normalize_subscription_feature_flags
from app.services.subscription_features import FEATURE_KEYS


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


SUBSCRIPTION_NOTICE_DAYS = 20


def subscription_access_state(sub: Subscription | None, now: datetime | None = None) -> dict:
    """Tenant-wide subscription gate — the single source of truth for whether a tenant's
    users may use the app and how many days remain. Applies to EVERY user of the tenant
    (there is no per-user expiry). Returns:
      blocked    -> access must be denied now
      expires_at -> effective expiry datetime (current_period_end, else trial_ends_at)
      days_left  -> whole days until expiry (negative if past), or None
      notice     -> days_left when within SUBSCRIPTION_NOTICE_DAYS of a real upcoming expiry
      reason     -> 'expired' | 'suspended' | 'canceled' | None
    """
    now = now or datetime.utcnow()
    base = {'blocked': False, 'expires_at': None, 'days_left': None, 'notice': None, 'grace': False, 'reason': None}
    if sub is None:
        return base
    expires_at = sub.current_period_end or sub.trial_ends_at
    days_left = (expires_at - now).days if expires_at is not None else None
    base['expires_at'] = expires_at
    base['days_left'] = days_left
    # Access is cut once suspended/canceled (after the past_due grace window, or a manual stop).
    if sub.status in (SubscriptionStatus.suspended, SubscriptionStatus.canceled):
        base['blocked'] = True
        base['reason'] = sub.status.value
        return base
    # past_due = the period has ended but the tenant is still inside the grace window: keep access,
    # but surface an urgent "expired — renew now" banner.
    if sub.status == SubscriptionStatus.past_due:
        base['grace'] = True
        base['reason'] = 'expired'
        return base
    # Otherwise (trial/active): warn when expiry is within the notice window.
    if days_left is not None and 0 <= days_left <= SUBSCRIPTION_NOTICE_DAYS:
        base['notice'] = days_left
    return base


async def sync_tenant_from_subscription(db: AsyncSession, tenant: Tenant, sub: Subscription) -> None:
    tenant.plan = sub.plan
    tenant.subscription_status = sub.status
    tenant.trial_ends_at = sub.trial_ends_at
    tenant.current_period_end = sub.current_period_end
    tenant.canceled_at = sub.canceled_at
    tenant_flags = {
        key: value
        for key, value in dict(tenant.feature_flags or {}).items()
        if key not in FEATURE_KEYS and key != '_temporary_upgrade'
    }
    sub_flags = dict(sub.feature_flags or {})
    normalized_flags = normalize_subscription_feature_flags(sub.plan, sub_flags)
    tenant_flags.update(normalized_flags)
    cleaned_sub_flags = dict(normalized_flags)
    for key, enabled in sub_flags.items():
        if key == '_temporary_upgrade' and isinstance(enabled, dict):
            tenant_flags[key] = enabled
            cleaned_sub_flags[key] = enabled
        elif key == 'custom_agreement_notes' and isinstance(enabled, str):
            tenant_flags[key] = enabled
            cleaned_sub_flags[key] = enabled
        elif key == 'custom_exclusive_implementations' and isinstance(enabled, str):
            tenant_flags[key] = enabled
            cleaned_sub_flags[key] = enabled
        elif key not in FEATURE_KEYS and isinstance(enabled, bool):
            tenant_flags[key] = enabled
            cleaned_sub_flags[key] = enabled
        elif sub.plan == PlanName.custom and key not in FEATURE_KEYS and isinstance(enabled, bool):
            tenant_flags[key] = enabled
            cleaned_sub_flags[key] = enabled
    sub.feature_flags = cleaned_sub_flags
    tenant.feature_flags = tenant_flags
    if sub.status in {SubscriptionStatus.suspended, SubscriptionStatus.canceled}:
        tenant.status = TenantStatus.suspended
    elif tenant.status == TenantStatus.suspended:
        tenant.status = TenantStatus.active


async def apply_subscription_time_transitions(db: AsyncSession, tenant: Tenant, sub: Subscription) -> bool:
    now = datetime.utcnow()
    prev = sub.status
    changed = False

    flags = dict(sub.feature_flags or {})
    temporary_upgrade = flags.get('_temporary_upgrade') if isinstance(flags.get('_temporary_upgrade'), dict) else None
    if temporary_upgrade and temporary_upgrade.get('active'):
        expires_raw = str(temporary_upgrade.get('expires_at') or '').strip()
        expires_at: datetime | None = None
        if expires_raw:
            try:
                expires_at = datetime.fromisoformat(expires_raw.replace('Z', '+00:00'))
                if expires_at.tzinfo is not None:
                    expires_at = expires_at.astimezone().replace(tzinfo=None)
            except ValueError:
                expires_at = None
        if expires_at is not None and expires_at <= now:
            original_plan_raw = str(temporary_upgrade.get('original_plan') or '').strip()
            try:
                sub.plan = PlanName(original_plan_raw)
            except ValueError:
                sub.plan = tenant.plan if tenant.plan != sub.plan else PlanName.standard
            original_status_raw = str(temporary_upgrade.get('original_status') or '').strip()
            try:
                sub.status = SubscriptionStatus(original_status_raw)
            except ValueError:
                pass
            original_feature_flags = temporary_upgrade.get('original_feature_flags')
            flags = dict(original_feature_flags) if isinstance(original_feature_flags, dict) else {}
            sub.feature_flags = flags
            original_current_period_end = str(temporary_upgrade.get('original_current_period_end') or '').strip()
            if original_current_period_end:
                try:
                    parsed_period_end = datetime.fromisoformat(original_current_period_end.replace('Z', '+00:00'))
                    sub.current_period_end = (
                        parsed_period_end.astimezone().replace(tzinfo=None)
                        if parsed_period_end.tzinfo is not None
                        else parsed_period_end
                    )
                except ValueError:
                    pass
            else:
                sub.current_period_end = None
            original_trial_ends_at = str(temporary_upgrade.get('original_trial_ends_at') or '').strip()
            if original_trial_ends_at:
                try:
                    parsed_trial_end = datetime.fromisoformat(original_trial_ends_at.replace('Z', '+00:00'))
                    sub.trial_ends_at = (
                        parsed_trial_end.astimezone().replace(tzinfo=None)
                        if parsed_trial_end.tzinfo is not None
                        else parsed_trial_end
                    )
                except ValueError:
                    pass
            db.add(
                AuditLog(
                    tenant_id=tenant.id,
                    action='subscription_temporary_upgrade_expired',
                    entity_type='subscription',
                    entity_id=str(sub.id),
                    payload={
                        'expired_at': now.isoformat(),
                        'temporary_plan': temporary_upgrade.get('temporary_plan'),
                        'restored_plan': sub.plan.value,
                    },
                )
            )
            changed = True

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
        changed = True
    if changed:
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
    if override is not None and (
        sub.plan == PlanName.custom
        or (feature in ADD_ON_FEATURE_KEYS and addon_allowed_for_plan(sub.plan, feature))
    ):
        return bool(override)
    return plan_default


def infer_default_features_for_plan(plan: PlanName) -> dict[str, bool]:
    return infer_subscription_feature_defaults(plan)
