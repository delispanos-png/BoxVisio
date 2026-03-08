from dataclasses import dataclass

from app.models.control import PlanName, Tenant


@dataclass(frozen=True)
class PlanPolicy:
    feature_sales: bool
    feature_purchases: bool
    feature_inventory: bool
    feature_cashflows: bool
    max_users: int
    max_branches: int


def resolve_plan_policy(tenant: Tenant) -> PlanPolicy:
    if tenant.plan == PlanName.standard:
        return PlanPolicy(
            feature_sales=True,
            feature_purchases=False,
            feature_inventory=False,
            feature_cashflows=False,
            max_users=5,
            max_branches=5,
        )
    if tenant.plan == PlanName.pro:
        return PlanPolicy(
            feature_sales=True,
            feature_purchases=True,
            feature_inventory=False,
            feature_cashflows=False,
            max_users=15,
            max_branches=20,
        )

    # Enterprise capabilities are source-agnostic at policy level.
    inventory_cashflows_allowed = True
    return PlanPolicy(
        feature_sales=True,
        feature_purchases=True,
        feature_inventory=inventory_cashflows_allowed,
        feature_cashflows=inventory_cashflows_allowed,
        max_users=100,
        max_branches=1000,
    )


def is_feature_enabled(tenant: Tenant, feature: str) -> bool:
    policy = resolve_plan_policy(tenant)
    if feature == 'sales':
        return policy.feature_sales
    if feature == 'purchases':
        return policy.feature_purchases
    if feature == 'inventory':
        return policy.feature_inventory
    if feature == 'cashflows':
        return policy.feature_cashflows
    return True
