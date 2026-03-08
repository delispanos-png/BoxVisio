from __future__ import annotations

from dataclasses import dataclass
from typing import Awaitable, Callable

from sqlalchemy.ext.asyncio import AsyncSession

from app.services.intelligence.rules.inventory import (
    run_dead_stock_value,
    run_low_coverage_top_sellers,
    run_overstock_slow_movers,
    run_stock_aging_spike,
)
from app.services.intelligence.rules.cashflow import (
    run_cash_net_negative,
    run_cash_collections_drop,
    run_cash_payment_imbalance,
)
from app.services.intelligence.rules.purchases import (
    run_purchases_drop_period,
    run_purchases_margin_pressure,
    run_purchases_spike_period,
    run_supplier_cost_increase,
    run_supplier_dependency,
    run_supplier_overdue_exposure,
    run_supplier_price_volatility,
)
from app.services.intelligence.rules.receivables import (
    run_collection_delay_alert,
    run_customer_overdue_spike,
    run_receivables_growth_alert,
    run_top_customer_exposure,
)
from app.services.intelligence.rules.sales import (
    run_brand_drop,
    run_branch_margin_low,
    run_branch_underperform,
    run_category_drop,
    run_category_margin_erosion,
    run_margin_drop_points,
    run_profit_drop_period,
    run_sales_drop_period,
    run_sales_spike_period,
    run_sales_volatility_high,
    run_top_products_dependency,
    run_weekend_shift,
)
from app.services.intelligence.types import InsightCreate, RuleContext

RuleRunner = Callable[[AsyncSession, dict, RuleContext], Awaitable[list[InsightCreate]]]


@dataclass(slots=True)
class RuleSpec:
    code: str
    name: str
    description: str
    category: str
    severity_default: str
    scope: str
    schedule: str
    params_json: dict
    runner: RuleRunner


RULE_SPECS: list[RuleSpec] = [
    # Standard (Sales) S1-S12
    RuleSpec('SLS_DROP_PERIOD', 'Sales Drop Period', 'Turnover drop vs previous period.', 'sales', 'warning', 'tenant', 'daily', {'drop_pct': 10, 'window_days': 30}, run_sales_drop_period),
    RuleSpec('SLS_SPIKE_PERIOD', 'Sales Spike Period', 'Turnover increase vs previous period.', 'sales', 'info', 'tenant', 'daily', {'increase_pct': 15, 'window_days': 30}, run_sales_spike_period),
    RuleSpec('PRF_DROP_PERIOD', 'Profit Drop Period', 'Profit drop vs previous period.', 'sales', 'warning', 'tenant', 'daily', {'drop_pct': 12, 'window_days': 30}, run_profit_drop_period),
    RuleSpec('MRG_DROP_POINTS', 'Margin Drop Points', 'Margin points drop vs previous period.', 'sales', 'warning', 'tenant', 'daily', {'drop_points': 2.0, 'window_days': 30}, run_margin_drop_points),
    RuleSpec('BR_UNDERPERFORM', 'Branch Underperform', 'Branch turnover below tenant average.', 'sales', 'warning', 'branch', 'daily', {'under_pct': 15, 'window_days': 30}, run_branch_underperform),
    RuleSpec('BR_MARGIN_LOW', 'Branch Margin Low', 'Branch margin lower than tenant margin.', 'sales', 'warning', 'branch', 'daily', {'gap_points': 2.0, 'window_days': 30}, run_branch_margin_low),
    RuleSpec('CAT_DROP', 'Category Drop', 'Category turnover drop vs previous period.', 'sales', 'warning', 'category', 'daily', {'drop_pct': 12, 'min_baseline': 500, 'window_days': 30}, run_category_drop),
    RuleSpec('CAT_MARGIN_EROSION', 'Category Margin Erosion', 'Category margin points drop.', 'sales', 'warning', 'category', 'daily', {'drop_points': 2.0, 'window_days': 30}, run_category_margin_erosion),
    RuleSpec('BRAND_DROP', 'Brand Drop', 'Brand turnover drop vs previous period.', 'sales', 'warning', 'brand', 'daily', {'drop_pct': 12, 'min_baseline': 300, 'window_days': 30}, run_brand_drop),
    RuleSpec('TOP_DEPENDENCY', 'Top Products Dependency', 'Top products concentration share.', 'sales', 'warning', 'tenant', 'daily', {'top_n': 10, 'share_pct': 35, 'window_days': 30}, run_top_products_dependency),
    RuleSpec('SLS_VOLATILITY', 'Sales Volatility High', 'High daily sales volatility.', 'sales', 'info', 'tenant', 'daily', {'window_days': 14, 'volatility_threshold': 0.25}, run_sales_volatility_high),
    RuleSpec('WEEKEND_SHIFT', 'Weekend Shift', 'Weekend sales mix shift.', 'sales', 'info', 'tenant', 'daily', {'share_points': 10.0, 'weekends': 4, 'window_days': 30}, run_weekend_shift),

    # Pro (Purchases) P1-P6
    RuleSpec('PUR_SPIKE_PERIOD', 'Purchases Spike Period', 'Purchases increase vs previous period.', 'purchases', 'warning', 'tenant', 'daily', {'increase_pct': 15, 'window_days': 30}, run_purchases_spike_period),
    RuleSpec('PUR_DROP_PERIOD', 'Purchases Drop Period', 'Purchases drop vs previous period.', 'purchases', 'info', 'tenant', 'daily', {'drop_pct': 20, 'window_days': 30}, run_purchases_drop_period),
    RuleSpec('SUP_DEPENDENCY', 'Supplier Dependency', 'Top supplier concentration.', 'purchases', 'warning', 'supplier', 'daily', {'top_supplier_share_pct': 40, 'window_days': 30}, run_supplier_dependency),
    RuleSpec('SUP_COST_UP', 'Supplier Cost Increase', 'Supplier cost increase vs previous period.', 'purchases', 'warning', 'supplier', 'daily', {'increase_pct': 15, 'min_baseline': 500, 'window_days': 30}, run_supplier_cost_increase),
    RuleSpec('SUP_VOLATILITY', 'Supplier Price Volatility', 'High supplier cost volatility.', 'purchases', 'warning', 'supplier', 'daily', {'window_days': 30, 'volatility_threshold': 0.30, 'min_daily_mean': 20}, run_supplier_price_volatility),
    RuleSpec('PUR_MARGIN_PRESSURE', 'Purchases Margin Pressure', 'Purchases up while margin down.', 'purchases', 'warning', 'tenant', 'daily', {'purchases_up_pct': 10, 'margin_down_points': 1.5, 'window_days': 30}, run_purchases_margin_pressure),
    RuleSpec('SUP_OVERDUE_EXPOSURE', 'Supplier Overdue Exposure', 'Supplier overdue exposure from supplier balance stream.', 'purchases', 'warning', 'supplier', 'daily', {'min_open_balance': 1000, 'overdue_ratio_threshold': 0.35, 'min_overdue_delta': 200}, run_supplier_overdue_exposure),

    # Enterprise (Inventory) E1-E4
    RuleSpec('INV_DEAD_STOCK', 'Dead Stock Value', 'No-sales dead stock value.', 'inventory', 'critical', 'item', 'daily', {'no_sales_days': 60, 'min_value': 300, 'window_days': 30}, run_dead_stock_value),
    RuleSpec('INV_AGING_SPIKE', 'Stock Aging Spike', '90+ days aging value increase.', 'inventory', 'warning', 'tenant', 'daily', {'increase_pct': 15, 'aging_days': 90, 'window_days': 30}, run_stock_aging_spike),
    RuleSpec('INV_LOW_COVERAGE', 'Low Coverage Top Sellers', 'Low coverage for top sellers.', 'inventory', 'warning', 'item', 'daily', {'min_coverage_days': 7, 'top_n': 50, 'window_days': 30}, run_low_coverage_top_sellers),
    RuleSpec('INV_OVERSTOCK_SLOW', 'Overstock Slow Movers', 'Slow movers overstock value.', 'inventory', 'warning', 'tenant', 'daily', {'min_value': 1000, 'slow_qty': 2, 'window_days': 30}, run_overstock_slow_movers),

    # Cashflow (Cash Transactions) C1-C3
    RuleSpec('CASH_NET_NEG', 'Net Cash Negative', 'Negative net cash over period.', 'cashflow', 'warning', 'tenant', 'daily', {'min_negative': 500, 'window_days': 30}, run_cash_net_negative),
    RuleSpec('CASH_COLL_DROP', 'Collections Drop', 'Customer collections drop vs previous period.', 'cashflow', 'warning', 'tenant', 'daily', {'drop_pct': 15, 'window_days': 30}, run_cash_collections_drop),
    RuleSpec('CASH_OUT_IMBALANCE', 'Payments Over Collections', 'Supplier payments exceed collections.', 'cashflow', 'warning', 'tenant', 'daily', {'outflow_over_inflow_ratio': 1.2, 'window_days': 30}, run_cash_payment_imbalance),

    # Receivables (Customer Balances) R1-R4
    RuleSpec(
        'CUSTOMER_OVERDUE_SPIKE',
        'Customer Overdue Spike',
        'Overdue receivables spike vs previous period.',
        'receivables',
        'warning',
        'tenant',
        'daily',
        {'overdue_spike_pct': 20, 'min_overdue_baseline': 500, 'window_days': 30},
        run_customer_overdue_spike,
    ),
    RuleSpec(
        'RECEIVABLES_GROWTH_ALERT',
        'Receivables Growth Alert',
        'Open receivables growth vs previous period.',
        'receivables',
        'warning',
        'tenant',
        'daily',
        {'growth_pct': 15, 'min_open_baseline': 1000, 'window_days': 30},
        run_receivables_growth_alert,
    ),
    RuleSpec(
        'TOP_CUSTOMER_EXPOSURE',
        'Top Customer Exposure',
        'Outstanding receivables concentration on top customer.',
        'receivables',
        'warning',
        'customer',
        'daily',
        {'share_pct_threshold': 25, 'min_total_open': 1000, 'window_days': 30},
        run_top_customer_exposure,
    ),
    RuleSpec(
        'COLLECTION_DELAY_ALERT',
        'Collection Delay Alert',
        'Delayed customer collections with overdue exposure.',
        'receivables',
        'warning',
        'customer',
        'daily',
        {'days_since_last_collection': 45, 'min_overdue_balance': 300, 'top_n': 10, 'window_days': 30},
        run_collection_delay_alert,
    ),
]

RULE_SPEC_BY_CODE = {r.code: r for r in RULE_SPECS}
