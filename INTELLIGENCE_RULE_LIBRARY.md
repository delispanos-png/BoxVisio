# INSIGHT RULES CATALOG (Deterministic)

## Plan Grouping
- Standard (Sales): 12 rules
- Pro (Purchases): +10 rules
- Enterprise (Inventory+Cashflow): +8 rules
- v1 implementation now: Sales S1-S12, Purchases P1-P6, Enterprise E1-E4 = 22 rules
- v2 backlog: Purchases P7-P10 + Enterprise E5-E8 = 8 rules

## STANDARD (Sales) - 12
1. `SLS_DROP_PERIOD` (S1, implemented_v1)
- category/scope: sales, tenant
- params: `{ "drop_pct": 10, "window_days": 30 }`
- source: `agg_sales_daily(doc_date, net_value)`
- formula: `turnover_A < turnover_prev_A * (1-drop_pct/100)`
- severity: warning (10-20%), critical (>20%)
- template: `Ο τζίρος έπεσε {delta_pct}% ({baseline}->{value}) σε σχέση με την προηγούμενη περίοδο.`
- drilldown: `/tenant/sales`

2. `SLS_SPIKE_PERIOD` (S2, implemented_v1)
- params: `{ "increase_pct": 15, "window_days": 30 }`
- source: `agg_sales_daily`
- formula: `turnover_A > turnover_prev_A * (1+increase_pct/100)`
- severity: info/warning
- template: `Απότομη αύξηση τζίρου {delta_pct}%...`
- drilldown: `/tenant/sales/compare`

3. `PRF_DROP_PERIOD` (S3, implemented_v1)
- params: `{ "drop_pct": 12, "window_days": 30 }`
- source: `agg_sales_daily(net_value,gross_value)`
- formula: `profit_A vs profit_prev_A`
- severity: warning/critical
- drilldown: `/tenant/sales`

4. `MRG_DROP_POINTS` (S4, implemented_v1)
- params: `{ "drop_points": 2.0, "window_days": 30 }`
- source: `agg_sales_daily(net_value,gross_value)`
- formula: `margin_points_drop > drop_points`
- severity: critical if >4 pts
- drilldown: `/tenant/sales/compare`

5. `BR_UNDERPERFORM` (S5, implemented_v1)
- scope: branch
- params: `{ "under_pct": 15, "window_days": 30 }`
- source: `agg_sales_daily(branch_ext_id, net_value)`
- formula: `branch_turnover < avg_branch_turnover*(1-under_pct)`
- template: `Το κατάστημα {branch} είναι {delta_pct}% κάτω από τον μέσο όρο.`
- drilldown: `/tenant/sales?branch={branch}`

6. `BR_MARGIN_LOW` (S6, implemented_v1)
- scope: branch
- params: `{ "gap_points": 2.0, "window_days": 30 }`
- source: `agg_sales_daily(branch_ext_id,net_value,gross_value)`
- formula: `branch_margin < tenant_margin - gap_points`
- severity: warning/critical by gap
- drilldown: `/tenant/sales?branch={branch}&metric=margin`

7. `CAT_DROP` (S7, implemented_v1)
- scope: category
- params: `{ "drop_pct": 12, "min_baseline": 500, "window_days": 30 }`
- source: `agg_sales_daily(category_ext_id, net_value)`
- formula: category turnover drop with baseline floor
- template: `Η κατηγορία {cat} έπεσε {delta_pct}%...`
- drilldown: `/tenant/sales?category={category}`

8. `CAT_MARGIN_EROSION` (S8, implemented_v1)
- scope: category
- params: `{ "drop_points": 2.0, "window_days": 30 }`
- source: `agg_sales_daily(category_ext_id, net_value, gross_value)`
- formula: category margin points drop > 2
- drilldown: `/tenant/sales?category={category}&metric=margin`

9. `BRAND_DROP` (S9, implemented_v1)
- scope: brand
- params: `{ "drop_pct": 12, "min_baseline": 300, "window_days": 30 }`
- source: `agg_sales_daily(brand_ext_id, net_value)`
- drilldown: `/tenant/sales?brand={brand}`

10. `TOP_DEPENDENCY` (S10, implemented_v1)
- params: `{ "top_n": 10, "share_pct": 35, "window_days": 30 }`
- source: `agg_sales_item_daily(item_external_id, net_value)`
- formula: top_n share of turnover > threshold
- template: `Υψηλή εξάρτηση: Top 10 προϊόντα = {share}% του τζίρου.`
- drilldown: `/tenant/sales?view=top-products`

11. `SLS_VOLATILITY` (S11, implemented_v1)
- params: `{ "window_days": 14, "volatility_threshold": 0.25 }`
- source: `agg_sales_daily(doc_date, net_value)`
- formula: `stddev(daily_turnover) / mean(daily_turnover) > 0.25`
- severity: info/warning
- drilldown: `/tenant/sales/trend`

12. `WEEKEND_SHIFT` (S12, implemented_v1)
- params: `{ "share_points": 10.0, "weekends": 4, "window_days": 30 }`
- source: `agg_sales_daily(doc_date, net_value)`
- formula: weekend share change > 10 points vs previous 4 weekends
- drilldown: `/tenant/sales/trend`

## PRO (Purchases) - 10
13. `PUR_SPIKE_PERIOD` (P1, implemented_v1)
14. `PUR_DROP_PERIOD` (P2, implemented_v1)
15. `SUP_DEPENDENCY` (P3, implemented_v1)
16. `SUP_COST_UP` (P4, implemented_v1)
17. `SUP_VOLATILITY` (P5, implemented_v1)
18. `PUR_MARGIN_PRESSURE` (P6, implemented_v1)
19. `SUP_ENDMONTH_PUSH` (P7, backlog_v2)
20. `CAT_COST_UP` (P8, backlog_v2)
21. `STOCKOUT_RISK` (P9, backlog_v2)
22. `SUP_TOP3_CONC` (P10, backlog_v2)

## ENTERPRISE (Inventory + Cashflow) - 8
23. `INV_DEAD_STOCK` (E1, implemented_v1)
24. `INV_AGING_SPIKE` (E2, implemented_v1)
25. `INV_LOW_COVERAGE` (E3, implemented_v1)
26. `INV_OVERSTOCK_SLOW` (E4, implemented_v1)
27. `INV_SHRINKAGE` (E5, backlog_v2)
28. `CF_NET_NEG_STREAK` (E6, backlog_v2)
29. `CF_PAYSHIFT` (E7, backlog_v2)
30. `CF_REFUND_SPIKE` (E8, backlog_v2)

## Data Source Requirements
- `agg_sales_daily`: date, branch/category/brand ids, turnover, cost, profit, qty, margin_pct derivable
- `agg_purchases_daily`: date, supplier_id, cost_total, qty_total
- Enterprise snapshots:
  - `agg_inventory_snapshot_daily`: item_id, stock_qty, stock_value
  - `agg_cashflow_daily` (v2): cash_in, cash_out, refunds, payment method mix
