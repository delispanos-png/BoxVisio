# Insight Rules by Module

Date: 2026-03-07

## Sales (Παραστατικά Πωλήσεων)
Rules from `backend/app/services/intelligence/rules/sales.py`:
- `SLS_DROP_PERIOD`
- `SLS_SPIKE_PERIOD`
- `PRF_DROP_PERIOD`
- `MRG_DROP_POINTS`
- `BR_UNDERPERFORM`
- `BR_MARGIN_LOW`
- `CAT_DROP`
- `CAT_MARGIN_EROSION`
- `BRAND_DROP`
- `TOP_DEPENDENCY`
- `SLS_VOLATILITY`
- `WEEKEND_SHIFT`

## Purchases (Παραστατικά Αγορών)
Rules from `backend/app/services/intelligence/rules/purchases.py`:
- `PUR_SPIKE_PERIOD`
- `PUR_DROP_PERIOD`
- `SUP_DEPENDENCY`
- `SUP_COST_UP`
- `SUP_VOLATILITY`
- `PUR_MARGIN_PRESSURE`
- `SUP_OVERDUE_EXPOSURE`

## Inventory (Είδη Αποθήκης)
Rules from `backend/app/services/intelligence/rules/inventory.py`:
- `INV_DEAD_STOCK`
- `INV_AGING_SPIKE`
- `INV_LOW_COVERAGE`
- `INV_OVERSTOCK_SLOW`

## Cash Transactions (Ταμειακές Συναλλαγές)
Rules from `backend/app/services/intelligence/rules/cashflow.py`:
- `CASH_NET_NEG`
- `CASH_COLL_DROP`
- `CASH_OUT_IMBALANCE`

## Customer Receivables (Υπόλοιπα Πελατών)
Rules from `backend/app/services/intelligence/rules/receivables.py`:
- `CUSTOMER_OVERDUE_SPIKE`
- `RECEIVABLES_GROWTH_ALERT`
- `TOP_CUSTOMER_EXPOSURE`
- `COLLECTION_DELAY_ALERT`
