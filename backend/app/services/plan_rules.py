from dataclasses import dataclass

from app.models.control import PlanName, Tenant

# Standard feature keys included in fixed plans.
_STANDARD_FEATURES = frozenset({'sales', 'purchases', 'inventory', 'cashflows'})


@dataclass(frozen=True)
class PlanPolicy:
    feature_sales: bool
    feature_purchases: bool
    feature_inventory: bool
    feature_cashflows: bool
    feature_supplier_targets: bool
    max_users: int
    max_branches: int


def resolve_plan_policy(tenant: Tenant) -> PlanPolicy:
    if tenant.plan == PlanName.standard:
        return PlanPolicy(
            feature_sales=True,
            feature_purchases=False,
            feature_inventory=False,
            feature_cashflows=False,
            feature_supplier_targets=False,
            max_users=3,
            max_branches=1,
        )
    if tenant.plan == PlanName.pro:
        return PlanPolicy(
            feature_sales=True,
            feature_purchases=True,
            feature_inventory=False,
            feature_cashflows=False,
            feature_supplier_targets=False,
            max_users=5,
            max_branches=5,
        )
    if tenant.plan == PlanName.enterprise:
        return PlanPolicy(
            feature_sales=True,
            feature_purchases=True,
            feature_inventory=True,
            feature_cashflows=True,
            feature_supplier_targets=True,
            max_users=50,
            max_branches=10,
        )
    # Custom: all modules + bespoke features via tenant.feature_flags.
    return PlanPolicy(
        feature_sales=True,
        feature_purchases=True,
        feature_inventory=True,
        feature_cashflows=True,
        feature_supplier_targets=True,
        max_users=9999,
        max_branches=9999,
    )


def is_feature_enabled(tenant: Tenant, feature: str) -> bool:
    """Check whether a feature is active for the tenant.

    For standard/pro/enterprise the answer comes purely from the plan policy.
    For custom plans, standard features are always on; bespoke features (keys
    not in _STANDARD_FEATURES) are gated by tenant.feature_flags so CloudOn
    can activate them individually after implementation.
    """
    policy = resolve_plan_policy(tenant)

    if feature == 'sales':
        return policy.feature_sales
    if feature == 'purchases':
        return policy.feature_purchases
    if feature == 'inventory':
        return policy.feature_inventory
    if feature == 'cashflows':
        return policy.feature_cashflows
    if feature == 'supplier_targets':
        return policy.feature_supplier_targets

    # Bespoke / custom feature key — only meaningful for custom plan.
    if tenant.plan == PlanName.custom:
        flags: dict = tenant.feature_flags if isinstance(tenant.feature_flags, dict) else {}
        return bool(flags.get(feature, False))

    return False


def has_bespoke_feature(tenant: Tenant, feature_key: str) -> bool:
    """True when a custom-plan tenant has a specific bespoke feature activated."""
    if tenant.plan != PlanName.custom:
        return False
    flags: dict = tenant.feature_flags if isinstance(tenant.feature_flags, dict) else {}
    return bool(flags.get(feature_key, False))
