from __future__ import annotations

from dataclasses import dataclass

from app.models.control import PlanName


@dataclass(frozen=True)
class SubscriptionFeature:
    key: str
    label: str
    group: str
    menu_keys: tuple[str, ...]
    path_prefixes: tuple[str, ...]
    default_standard: bool
    default_pro: bool
    default_enterprise: bool


SUBSCRIPTION_FEATURES: tuple[SubscriptionFeature, ...] = (
    SubscriptionFeature(
        key='sales',
        label='Πωλήσεις',
        group='Κυκλώματα',
        menu_keys=('stream_sales_documents', 'analytics_sales'),
        path_prefixes=(
            '/tenant/sales',
            '/tenant/sales-documents',
            '/tenant/pos',
            '/tenant/e-shop-analysis',
            '/tenant/exports/sellout',
            '/v1/kpi/sales',
            '/kpi/sales',
            '/v1/ingest/sales',
            '/v1/streams/sales',
            '/api/streams/sales',
        ),
        default_standard=True,
        default_pro=True,
        default_enterprise=True,
    ),
    SubscriptionFeature(
        key='purchases',
        label='Αγορές',
        group='Κυκλώματα',
        menu_keys=('stream_purchase_documents', 'analytics_purchases'),
        path_prefixes=(
            '/tenant/purchases',
            '/tenant/purchase-documents',
            '/v1/kpi/purchases',
            '/kpi/purchases',
            '/v1/ingest/purchases',
            '/v1/streams/purchases',
            '/api/streams/purchases',
        ),
        default_standard=False,
        default_pro=True,
        default_enterprise=True,
    ),
    SubscriptionFeature(
        key='inventory',
        label='Αποθήκη / Είδη',
        group='Κυκλώματα',
        menu_keys=('stream_inventory_documents', 'analytics_inventory'),
        path_prefixes=(
            '/tenant/inventory',
            '/tenant/items',
            '/tenant/warehouse-documents',
            '/v1/kpi/inventory',
            '/kpi/inventory',
            '/v1/streams/inventory',
            '/api/streams/inventory',
        ),
        default_standard=False,
        default_pro=False,
        default_enterprise=True,
    ),
    SubscriptionFeature(
        key='cashflows',
        label='Ταμείο / Χρηματοροές',
        group='Κυκλώματα',
        menu_keys=('stream_cash_transactions', 'analytics_cashflows'),
        path_prefixes=(
            '/tenant/cashflow',
            '/v1/kpi/cashflows',
            '/kpi/cashflows',
            '/v1/kpi/cashflow',
            '/kpi/cashflow',
            '/v1/streams/cash',
            '/api/streams/cash',
        ),
        default_standard=False,
        default_pro=False,
        default_enterprise=True,
    ),
    SubscriptionFeature(
        key='operating_expenses',
        label='Λειτουργικά έξοδα',
        group='Κυκλώματα',
        menu_keys=('stream_operating_expenses',),
        path_prefixes=(
            '/tenant/expense-documents',
            '/tenant/operating-expenses',
            '/v1/streams/expenses',
            '/api/streams/expenses',
        ),
        default_standard=False,
        default_pro=False,
        default_enterprise=True,
    ),
    SubscriptionFeature(
        key='supplier_balances',
        label='Υπόλοιπα προμηθευτών',
        group='Κυκλώματα',
        menu_keys=('stream_supplier_balances',),
        path_prefixes=('/tenant/suppliers', '/v1/streams/supplier', '/api/streams/supplier'),
        default_standard=False,
        default_pro=False,
        default_enterprise=True,
    ),
    SubscriptionFeature(
        key='customer_balances',
        label='Υπόλοιπα πελατών',
        group='Κυκλώματα',
        menu_keys=('stream_customer_balances', 'analytics_receivables_payables'),
        path_prefixes=('/tenant/customers', '/tenant/finance-dashboard', '/v1/streams/customer', '/api/streams/customer'),
        default_standard=False,
        default_pro=False,
        default_enterprise=True,
    ),
    SubscriptionFeature(
        key='supplier_targets',
        label='Στόχοι προμηθευτών',
        group='Analytics',
        menu_keys=('analytics_supplier_targets',),
        path_prefixes=('/tenant/supplier-targets',),
        default_standard=False,
        default_pro=False,
        default_enterprise=True,
    ),
    SubscriptionFeature(
        key='insights',
        label='Insights',
        group='Analytics',
        menu_keys=('insights',),
        path_prefixes=('/tenant/insights', '/v1/insights', '/api/insights'),
        default_standard=True,
        default_pro=True,
        default_enterprise=True,
    ),
    SubscriptionFeature(
        key='comparisons',
        label='Συγκρίσεις',
        group='Analytics',
        menu_keys=('comparisons',),
        path_prefixes=('/tenant/comparisons',),
        default_standard=True,
        default_pro=True,
        default_enterprise=True,
    ),
    SubscriptionFeature(
        key='exports',
        label='Exports / Reports',
        group='Exports',
        menu_keys=('exports',),
        path_prefixes=('/tenant/exports',),
        default_standard=True,
        default_pro=True,
        default_enterprise=True,
    ),
)

FEATURE_BY_KEY = {item.key: item for item in SUBSCRIPTION_FEATURES}
FEATURE_KEYS = tuple(FEATURE_BY_KEY.keys())
FEATURE_PATH_PREFIXES = {item.key: item.path_prefixes for item in SUBSCRIPTION_FEATURES}


def infer_subscription_feature_defaults(plan: PlanName) -> dict[str, bool]:
    defaults: dict[str, bool] = {}
    for item in SUBSCRIPTION_FEATURES:
        if plan == PlanName.standard:
            defaults[item.key] = item.default_standard
        elif plan == PlanName.pro:
            defaults[item.key] = item.default_pro
        else:
            defaults[item.key] = item.default_enterprise
    return defaults


def normalize_subscription_feature_flags(plan: PlanName, raw: dict | None) -> dict[str, bool]:
    values = infer_subscription_feature_defaults(plan)
    if isinstance(raw, dict):
        for key in FEATURE_KEYS:
            if key in raw:
                values[key] = bool(raw.get(key))
    return values


def menu_visibility_from_features(feature_flags: dict[str, bool]) -> dict[str, bool]:
    visibility: dict[str, bool] = {}
    for item in SUBSCRIPTION_FEATURES:
        enabled = bool(feature_flags.get(item.key, False))
        for menu_key in item.menu_keys:
            visibility[menu_key] = enabled
    return visibility


def feature_key_for_path(path: str) -> str | None:
    for key, prefixes in FEATURE_PATH_PREFIXES.items():
        if any(path.startswith(prefix) for prefix in prefixes):
            return key
    return None
