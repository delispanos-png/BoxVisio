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
    default_custom: bool
    minimum_plan: str
    addon: bool = False


SUBSCRIPTION_FEATURES: tuple[SubscriptionFeature, ...] = (
    SubscriptionFeature(
        key='sales',
        label='Πωλήσεις',
        group='Κυκλώματα',
        menu_keys=('analytics_sales',),
        path_prefixes=(
            '/tenant/sales',
            '/v1/kpi/sales',
            '/kpi/sales',
        ),
        default_standard=True,
        default_pro=True,
        default_enterprise=True,
        default_custom=True,
        minimum_plan='Standard',
    ),
    SubscriptionFeature(
        key='sales_documents',
        label='Παραστατικά Πωλήσεων',
        group='Παραστατικά',
        menu_keys=('stream_sales_documents',),
        path_prefixes=('/tenant/sales-documents', '/v1/ingest/sales', '/v1/streams/sales', '/api/streams/sales'),
        default_standard=True,
        default_pro=True,
        default_enterprise=True,
        default_custom=True,
        minimum_plan='Standard',
    ),
    SubscriptionFeature(
        key='pos',
        label='Φυσικό Σημείο / POS',
        group='Analytics',
        menu_keys=('analytics_pos',),
        path_prefixes=('/tenant/pos', '/v1/kpi/pos'),
        default_standard=True,
        default_pro=True,
        default_enterprise=True,
        default_custom=True,
        minimum_plan='Standard',
    ),
    SubscriptionFeature(
        key='purchases',
        label='Αγορές',
        group='Κυκλώματα',
        menu_keys=('analytics_purchases',),
        path_prefixes=(
            '/tenant/purchases',
            '/v1/kpi/purchases',
            '/kpi/purchases',
            '/v1/ingest/purchases',
            '/v1/streams/purchases',
            '/api/streams/purchases',
        ),
        default_standard=False,
        default_pro=True,
        default_enterprise=True,
        default_custom=True,
        minimum_plan='Pro',
    ),
    SubscriptionFeature(
        key='purchase_documents',
        label='Παραστατικά Αγορών',
        group='Παραστατικά',
        menu_keys=('stream_purchase_documents',),
        path_prefixes=('/tenant/purchase-documents',),
        default_standard=False,
        default_pro=True,
        default_enterprise=True,
        default_custom=True,
        minimum_plan='Pro',
    ),
    SubscriptionFeature(
        key='supplier_orders',
        label='Παραγγελίες Προμηθευτών',
        group='Κυκλώματα',
        menu_keys=('stream_supplier_orders',),
        path_prefixes=('/tenant/supplier-orders', '/v1/kpi/supplier-orders'),
        default_standard=False,
        default_pro=False,
        default_enterprise=True,
        default_custom=True,
        minimum_plan='Enterprise',
    ),
    SubscriptionFeature(
        key='eshop_analysis',
        label='E-Shop Analysis',
        group='Analytics',
        menu_keys=('analytics_eshop',),
        path_prefixes=('/tenant/e-shop-analysis', '/v1/kpi/eshop'),
        default_standard=False,
        default_pro=True,
        default_enterprise=True,
        default_custom=True,
        minimum_plan='Pro',
    ),
    SubscriptionFeature(
        key='sellout',
        label='Sell Out',
        group='Analytics',
        menu_keys=('analytics_sellout',),
        path_prefixes=('/tenant/exports/sellout', '/v1/reports/sellout'),
        default_standard=False,
        default_pro=True,
        default_enterprise=True,
        default_custom=True,
        minimum_plan='Pro',
    ),
    SubscriptionFeature(
        key='era_exploration_data',
        label='eRA Exploration Data',
        group='Analytics',
        menu_keys=('analytics_era_exploration_data',),
        path_prefixes=('/tenant/era-exploration-data', '/v1/kpi/era-exploration'),
        default_standard=False,
        default_pro=False,
        default_enterprise=False,
        default_custom=True,
        minimum_plan='Pro / Enterprise add-on ή Custom',
        addon=True,
    ),
    SubscriptionFeature(
        key='iqvia',
        label='IQVIA Market Data',
        group='Analytics',
        menu_keys=('analytics_iqvia',),
        path_prefixes=('/tenant/iqvia', '/v1/kpi/iqvia'),
        default_standard=False,
        default_pro=False,
        default_enterprise=False,
        default_custom=True,
        minimum_plan='Pro / Enterprise add-on ή Custom',
        addon=True,
    ),
    SubscriptionFeature(
        key='replenishment',
        label='Replenishment / Availability',
        group='Analytics',
        menu_keys=('analytics_replenishment',),
        path_prefixes=('/tenant/replenishment', '/v1/kpi/replenishment'),
        default_standard=False,
        default_pro=False,
        default_enterprise=False,
        default_custom=True,
        minimum_plan='Pro / Enterprise add-on ή Custom',
        addon=True,
    ),
    SubscriptionFeature(
        key='price_control',
        label='Έλεγχος Τιμών',
        group='Analytics',
        menu_keys=('analytics_price_control',),
        path_prefixes=('/tenant/price-control', '/v1/kpi/price-control'),
        default_standard=False,
        default_pro=True,
        default_enterprise=True,
        default_custom=True,
        minimum_plan='Pro',
    ),
    SubscriptionFeature(
        key='business_advisor',
        label='Σύμβουλος Επιχείρησης',
        group='Premium',
        menu_keys=('analytics_business_advisor',),
        path_prefixes=('/tenant/business-advisor', '/v1/kpi/business-advisor'),
        default_standard=False,
        default_pro=False,
        default_enterprise=False,
        default_custom=True,
        minimum_plan='Pro / Enterprise add-on ή Custom',
        addon=True,
    ),
    SubscriptionFeature(
        key='inventory',
        label='Απόθεμα',
        group='Κυκλώματα',
        menu_keys=('analytics_inventory',),
        path_prefixes=(
            '/tenant/inventory',
            '/v1/kpi/inventory',
            '/kpi/inventory',
            '/v1/streams/inventory',
            '/api/streams/inventory',
        ),
        default_standard=False,
        default_pro=False,
        default_enterprise=True,
        default_custom=True,
        minimum_plan='Enterprise',
    ),
    SubscriptionFeature(
        key='inventory_items',
        label='Είδη Αποθήκης',
        group='Κυκλώματα',
        menu_keys=('analytics_items',),
        path_prefixes=('/tenant/items',),
        default_standard=False,
        default_pro=False,
        default_enterprise=True,
        default_custom=True,
        minimum_plan='Enterprise',
    ),
    SubscriptionFeature(
        key='warehouse_documents',
        label='Παραστατικά Αποθήκης',
        group='Παραστατικά',
        menu_keys=('stream_inventory_documents',),
        path_prefixes=('/tenant/warehouse-documents',),
        default_standard=False,
        default_pro=False,
        default_enterprise=True,
        default_custom=True,
        minimum_plan='Enterprise',
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
        default_custom=True,
        minimum_plan='Enterprise',
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
        default_custom=True,
        minimum_plan='Enterprise',
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
        default_custom=True,
        minimum_plan='Enterprise',
    ),
    SubscriptionFeature(
        key='customer_balances',
        label='Υπόλοιπα πελατών',
        group='Κυκλώματα',
        menu_keys=('stream_customer_balances', 'analytics_receivables_payables', 'dashboard_finance'),
        path_prefixes=('/tenant/customers', '/tenant/finance-dashboard', '/v1/streams/customer', '/api/streams/customer'),
        default_standard=False,
        default_pro=False,
        default_enterprise=True,
        default_custom=True,
        minimum_plan='Enterprise',
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
        default_custom=True,
        minimum_plan='Enterprise',
    ),
    SubscriptionFeature(
        key='inventory_item_classification',
        label='Inventory Item Classification',
        group='Advanced',
        menu_keys=(),
        path_prefixes=(),
        default_standard=False,
        default_pro=False,
        default_enterprise=True,
        default_custom=True,
        minimum_plan='Enterprise',
    ),
    SubscriptionFeature(
        key='eshop_fulfillment',
        label='E-Shop Fulfillment',
        group='Advanced',
        menu_keys=(),
        path_prefixes=(),
        default_standard=False,
        default_pro=False,
        default_enterprise=True,
        default_custom=True,
        minimum_plan='Pro add-on ή Enterprise',
        addon=True,
    ),
    SubscriptionFeature(
        key='call_center_3cx',
        label='3CX Call Center KPIs',
        group='Add-ons',
        menu_keys=('analytics_call_center',),
        path_prefixes=('/tenant/call-center', '/v1/kpi/call-center'),
        default_standard=False,
        default_pro=False,
        default_enterprise=False,
        default_custom=True,
        minimum_plan='Pro / Enterprise add-on ή Custom',
        addon=True,
    ),
    SubscriptionFeature(
        key='custom_integrations',
        label='Custom Integrations',
        group='Add-ons',
        menu_keys=(),
        path_prefixes=(),
        default_standard=False,
        default_pro=False,
        default_enterprise=False,
        default_custom=True,
        minimum_plan='Enterprise add-on ή Custom',
        addon=True,
    ),
    SubscriptionFeature(
        key='dedicated_infrastructure',
        label='Dedicated Infrastructure',
        group='Add-ons',
        menu_keys=(),
        path_prefixes=(),
        default_standard=False,
        default_pro=False,
        default_enterprise=False,
        default_custom=True,
        minimum_plan='Custom',
        addon=True,
    ),
    SubscriptionFeature(
        key='advanced_automations',
        label='Advanced Automations',
        group='Add-ons',
        menu_keys=(),
        path_prefixes=(),
        default_standard=False,
        default_pro=False,
        default_enterprise=False,
        default_custom=True,
        minimum_plan='Pro / Enterprise add-on ή Custom',
        addon=True,
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
        default_custom=True,
        minimum_plan='Standard',
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
        default_custom=True,
        minimum_plan='Standard',
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
        default_custom=True,
        minimum_plan='Standard',
    ),
    SubscriptionFeature(
        key='copilot',
        label='Co-Pilot AI',
        group='Analytics',
        menu_keys=('copilot',),
        path_prefixes=('/tenant/copilot',),
        # Add-on, off by default — the admin opts each tenant in per client. The tenant
        # supplies its own LLM API key (billed to the tenant) in its own admin panel.
        default_standard=False,
        default_pro=False,
        default_enterprise=False,
        default_custom=False,
        minimum_plan='Standard',
        addon=True,
    ),
)

FEATURE_BY_KEY = {item.key: item for item in SUBSCRIPTION_FEATURES}
FEATURE_KEYS = tuple(FEATURE_BY_KEY.keys())
FEATURE_PATH_PREFIXES = {item.key: item.path_prefixes for item in SUBSCRIPTION_FEATURES}
ADD_ON_FEATURE_KEYS = tuple(item.key for item in SUBSCRIPTION_FEATURES if item.addon)
STANDARD_LOCKED_ADD_ON_KEYS = frozenset(ADD_ON_FEATURE_KEYS)
DEDICATED_INFRASTRUCTURE_KEY = 'dedicated_infrastructure'


def infer_subscription_feature_defaults(plan: PlanName) -> dict[str, bool]:
    defaults: dict[str, bool] = {}
    for item in SUBSCRIPTION_FEATURES:
        if plan == PlanName.standard:
            defaults[item.key] = item.default_standard
        elif plan == PlanName.pro:
            defaults[item.key] = item.default_pro
        elif plan == PlanName.enterprise:
            defaults[item.key] = item.default_enterprise
        else:
            defaults[item.key] = item.default_custom
    return defaults


def normalize_subscription_feature_flags(plan: PlanName, raw: dict | None) -> dict[str, bool]:
    values = infer_subscription_feature_defaults(plan)
    if isinstance(raw, dict):
        for key in FEATURE_KEYS:
            if key not in raw:
                continue
            if isinstance(raw.get(key), bool):
                values[key] = bool(raw.get(key))
    return values


def addon_allowed_for_plan(plan: PlanName, feature_key: str) -> bool:
    if feature_key not in ADD_ON_FEATURE_KEYS:
        return False
    if plan == PlanName.standard:
        return False
    if feature_key == DEDICATED_INFRASTRUCTURE_KEY:
        return plan == PlanName.custom
    if feature_key == 'custom_integrations':
        return plan in {PlanName.enterprise, PlanName.custom}
    return plan in {PlanName.pro, PlanName.enterprise, PlanName.custom}


def menu_visibility_from_features(feature_flags: dict[str, bool]) -> dict[str, bool]:
    """Subscription features should not hide menu entries.

    The commercial UX is deliberate: users can see higher-plan capabilities
    and get an upgrade message when they open one. Persona visibility still
    controls role-specific menu access in deps.py.
    """
    visibility: dict[str, bool] = {}
    for item in SUBSCRIPTION_FEATURES:
        for menu_key in item.menu_keys:
            visibility[menu_key] = True
    return visibility


def menu_locked_from_features(feature_flags: dict[str, bool]) -> dict[str, bool]:
    locked: dict[str, bool] = {}
    for item in SUBSCRIPTION_FEATURES:
        enabled = bool(feature_flags.get(item.key, False))
        for menu_key in item.menu_keys:
            locked[menu_key] = not enabled
    return locked


def feature_label(feature_key: str | None) -> str:
    if not feature_key:
        return 'Η δυνατότητα'
    item = FEATURE_BY_KEY.get(feature_key)
    return item.label if item else feature_key


def feature_minimum_plan(feature_key: str | None) -> str:
    if not feature_key:
        return 'μεγαλύτερο πακέτο'
    item = FEATURE_BY_KEY.get(feature_key)
    return item.minimum_plan if item else 'μεγαλύτερο πακέτο'


def feature_key_for_path(path: str) -> str | None:
    for key, prefixes in FEATURE_PATH_PREFIXES.items():
        if any(path.startswith(prefix) for prefix in prefixes):
            return key
    return None
