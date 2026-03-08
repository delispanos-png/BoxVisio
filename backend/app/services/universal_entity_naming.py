from __future__ import annotations

# Canonical business-neutral entities used across streams, KPIs and insights.
CANONICAL_ENTITY_NAMING: dict[str, dict[str, str]] = {
    'product': {
        'physical_table': 'dim_item',
        'legacy_alias': 'item',
        'description': 'Sellable or stock-managed business object.',
    },
    'supplier': {
        'physical_table': 'dim_supplier',
        'legacy_alias': 'supplier',
        'description': 'Vendor or payables counterparty.',
    },
    'customer': {
        'physical_table': 'fact_sales.customer_* / fact_customer_balances',
        'legacy_alias': 'customer',
        'description': 'Receivables counterparty.',
    },
    'category': {
        'physical_table': 'dim_category',
        'legacy_alias': 'category',
        'description': 'Business grouping for products/services.',
    },
    'location': {
        'physical_table': 'dim_branch + dim_warehouse',
        'legacy_alias': 'branch/warehouse',
        'description': 'Operational place where transactions occur.',
    },
    'account': {
        'physical_table': 'fact_cashflows.account_id',
        'legacy_alias': 'cash/bank account',
        'description': 'Financial account for cash movement classification.',
    },
}
