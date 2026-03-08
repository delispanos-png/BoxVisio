# KPI Catalog

## Scope
This catalog groups KPIs into decision-oriented categories for Operational Intelligence Platform for Business tenants.

## Category: Sales

### Total Turnover
- Business meaning: Total net sales value in selected period.
- Formula: `SUM(net_value)`
- Data source: `agg_sales_daily` (or `fact_sales` fallback)
- Aggregation: daily/monthly/yearly
- Decision impact: Measures top-line commercial performance.

### Gross Profit
- Business meaning: Profit before operating expenses.
- Formula: `SUM(gross_value) - SUM(net_value)` (current model)
- Data source: `agg_sales_daily`
- Aggregation: daily/monthly/yearly
- Decision impact: Indicates pricing and portfolio profitability.

### Margin %
- Business meaning: Profitability ratio over gross value.
- Formula: `gross_profit / gross_value * 100`
- Data source: derived from `agg_sales_daily`
- Aggregation: period-level
- Decision impact: Alerts owner when margins erode.

### Quantity Sold
- Business meaning: Total sold units.
- Formula: `SUM(qty)`
- Data source: `agg_sales_daily`
- Aggregation: daily/monthly/yearly
- Decision impact: Tracks demand and product velocity.

### Avg Basket Value
- Business meaning: Average revenue per aggregated sales record.
- Formula: `SUM(net_value) / COUNT(records)`
- Data source: `agg_sales_daily`
- Aggregation: period-level
- Decision impact: Identifies ticket-size trends and promo effects.

### Growth % vs Previous Period
- Business meaning: Turnover change compared to previous equal-length period.
- Formula: `(turnover_current - turnover_prev) / turnover_prev * 100`
- Data source: `agg_sales_daily`
- Aggregation: period-level
- Decision impact: Detects acceleration/decline early.

### Sales by Branch
- Business meaning: Revenue contribution per branch.
- Formula: `SUM(net_value) GROUP BY branch_ext_id`
- Data source: `agg_sales_daily`
- Aggregation: daily -> period grouped by branch
- Decision impact: Resource allocation and branch performance ranking.

### Sales by Category
- Business meaning: Revenue distribution by product category.
- Formula: `SUM(net_value) GROUP BY category_ext_id`
- Data source: `agg_sales_daily`
- Aggregation: daily -> period grouped by category
- Decision impact: Category mix and assortment optimization.

### Sales by Brand
- Business meaning: Revenue distribution by brand.
- Formula: `SUM(net_value) GROUP BY brand_ext_id`
- Data source: `agg_sales_daily`
- Aggregation: daily -> period grouped by brand
- Decision impact: Supplier/brand negotiation leverage.

### Top Products
- Business meaning: Best-selling products by value/volume.
- Formula: `SUM(net_value), SUM(qty) GROUP BY item_code ORDER BY net_value DESC`
- Data source: `fact_sales`
- Aggregation: period grouped by item
- Decision impact: Stocking priorities and promo planning.

### Slow vs Fast Movers
- Business meaning: Products with highest/lowest movement speed.
- Formula: rank by `SUM(qty)` per item in period.
- Data source: `fact_sales`
- Aggregation: period grouped by item
- Decision impact: Prevent overstock/dead stock and stockouts.

## Category: Purchases

### Total Purchases
- Business meaning: Total purchase net value in selected period.
- Formula: `SUM(net_value)`
- Data source: `agg_purchases_daily`
- Aggregation: daily/monthly/yearly
- Decision impact: Controls procurement spending.

### Purchase Cost Trend
- Business meaning: Cost evolution over time.
- Formula: `SUM(cost_amount)` by period
- Data source: `agg_purchases_daily`, `agg_purchases_monthly`
- Aggregation: daily/monthly
- Decision impact: Detects inflationary pressure and timing opportunities.

### Supplier Distribution
- Business meaning: Spend distribution among suppliers.
- Formula: `SUM(net_value) GROUP BY supplier_ext_id`
- Data source: `agg_purchases_daily`
- Aggregation: period grouped by supplier
- Decision impact: Diversification and bargaining strategy.

### Margin by Supplier
- Business meaning: Downstream margin impact by supplier portfolio.
- Formula: derived by joining supplier-linked purchase and sales mix.
- Data source: `agg_purchases_daily` + sales aggregates (derived KPI layer)
- Aggregation: period-level
- Decision impact: Supplier portfolio optimization.

### Cost Change Detection
- Business meaning: Detect abrupt changes in purchase cost.
- Formula: `% change of avg cost` per item/supplier across windows
- Data source: `fact_purchases` / `agg_purchases_daily`
- Aggregation: rolling windows
- Decision impact: Early alert for repricing actions.

### Dependency on Top Suppliers
- Business meaning: Concentration risk on top suppliers.
- Formula: `top_supplier_spend / total_spend * 100`
- Data source: `agg_purchases_daily`
- Aggregation: period-level
- Decision impact: Reduces supply disruption risk.

## Category: Inventory (Enterprise)

### Current Stock Value
- Business meaning: Current inventory valuation.
- Formula: `SUM(value_amount)`
- Data source: `fact_inventory`
- Aggregation: snapshot date
- Decision impact: Working capital control.

### Stock Aging
- Business meaning: Stock value by aging buckets.
- Formula: bucket by days since last movement (0-30, 31-90, 90+)
- Data source: `fact_inventory` + movement dates
- Aggregation: snapshot-level buckets
- Decision impact: Prevents obsolescence.

### Dead Stock Value
- Business meaning: Value tied in non-moving inventory.
- Formula: `SUM(value_amount) where days_since_move > threshold`
- Data source: `fact_inventory`
- Aggregation: snapshot-level
- Decision impact: Liquidation and markdown decisions.

### Stock Coverage Days
- Business meaning: Days inventory can sustain demand.
- Formula: `on_hand_qty / avg_daily_sales_qty`
- Data source: `fact_inventory` + `agg_sales_daily`
- Aggregation: item/branch level
- Decision impact: Replenishment and safety-stock policies.

### Fast/Slow Moving Items (Inventory)
- Business meaning: Velocity classification for stocking policy.
- Formula: classify by rolling sold qty and stock turnover.
- Data source: `fact_inventory` + `fact_sales`
- Aggregation: item-level
- Decision impact: Shelf optimization and working-capital efficiency.

## Category: Cashflow (Enterprise)

### Cash In / Cash Out
- Business meaning: Inflows and outflows in selected period.
- Formula: `SUM(amount where entry_type='in')`, `SUM(amount where entry_type='out')`
- Data source: `fact_cashflows`
- Aggregation: daily/monthly/yearly
- Decision impact: Liquidity monitoring.

### Payment Methods Distribution
- Business meaning: Revenue composition by payment method.
- Formula: `SUM(amount) GROUP BY payment_method`
- Data source: `fact_cashflows` (if method available)
- Aggregation: period-level
- Decision impact: Fee optimization and channel strategy.

### Refund Trends
- Business meaning: Refund pattern and volatility.
- Formula: `SUM(refund_amount)` by period and ratio to sales
- Data source: `fact_cashflows`
- Aggregation: daily/monthly
- Decision impact: Detects quality/pricing issues.

### Net Daily Cash
- Business meaning: Daily net liquidity movement.
- Formula: `cash_in - cash_out`
- Data source: `fact_cashflows`
- Aggregation: daily
- Decision impact: Short-term treasury decisions.

## Derived Insight Layer (planned)
- Turnover drop alert: trigger when period growth `< -X%`.
- Margin erosion alert: trigger when margin drops over threshold.
- Supplier dependency alert: trigger when top supplier share `> 40%`.
