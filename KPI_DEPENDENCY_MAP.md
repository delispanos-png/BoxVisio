# KPI Dependency Map (4 Stream Model)

Date: 2026-03-06

## 1) Minimal ingestion queries per stream

Each source query must output a canonical row-level contract.

Common required output columns:
- `document_id`
- `document_line_no`
- `document_type`
- `document_date`
- `branch_external_id`
- `entity_external_id`
- `item_external_id`
- `quantity`
- `net_amount`
- `cost_amount`
- `account_external_id`
- `external_id`
- `updated_at`

## 1.1 Sales documents query (minimal)

```sql
SELECT
  CAST(s.DocId AS nvarchar(128))          AS document_id,
  CAST(s.LineNo AS int)                   AS document_line_no,
  CAST(s.DocType AS nvarchar(32))         AS document_type,  -- invoice/receipt/credit_note/pos_sale
  CAST(s.DocDate AS date)                 AS document_date,
  CAST(s.BranchCode AS nvarchar(64))      AS branch_external_id,
  CAST(s.CustomerCode AS nvarchar(64))    AS entity_external_id,
  CAST(s.ItemCode AS nvarchar(128))       AS item_external_id,
  CAST(s.Qty AS decimal(18,4))            AS quantity,
  CAST(s.NetValue AS decimal(14,2))       AS net_amount,
  CAST(s.CostValue AS decimal(14,2))      AS cost_amount,
  CAST(NULL AS nvarchar(64))              AS account_external_id,
  CAST(s.LineId AS nvarchar(256))         AS external_id,
  CAST(s.UpdatedAt AS datetime2)          AS updated_at
FROM dbo.SalesDocumentLines s
WHERE s.UpdatedAt > @last_sync_ts
ORDER BY s.UpdatedAt, s.LineId;
```

## 1.2 Purchase documents query (minimal)

```sql
SELECT
  CAST(p.DocId AS nvarchar(128))          AS document_id,
  CAST(p.LineNo AS int)                   AS document_line_no,
  CAST(p.DocType AS nvarchar(32))         AS document_type,  -- supplier_invoice/purchase_receipt/purchase_credit_note
  CAST(p.DocDate AS date)                 AS document_date,
  CAST(p.BranchCode AS nvarchar(64))      AS branch_external_id,
  CAST(p.SupplierCode AS nvarchar(64))    AS entity_external_id,
  CAST(p.ItemCode AS nvarchar(128))       AS item_external_id,
  CAST(p.Qty AS decimal(18,4))            AS quantity,
  CAST(p.NetValue AS decimal(14,2))       AS net_amount,
  CAST(p.CostValue AS decimal(14,2))      AS cost_amount,
  CAST(NULL AS nvarchar(64))              AS account_external_id,
  CAST(p.LineId AS nvarchar(256))         AS external_id,
  CAST(p.UpdatedAt AS datetime2)          AS updated_at
FROM dbo.PurchaseDocumentLines p
WHERE p.UpdatedAt > @last_sync_ts
ORDER BY p.UpdatedAt, p.LineId;
```

## 1.3 Warehouse documents query (minimal)

```sql
SELECT
  CAST(w.DocId AS nvarchar(128))          AS document_id,
  CAST(w.LineNo AS int)                   AS document_line_no,
  CAST(w.DocType AS nvarchar(32))         AS document_type,  -- stock_transfer/stock_adjustment/stock_entry/stock_exit/inventory_correction
  CAST(w.DocDate AS date)                 AS document_date,
  CAST(w.BranchCode AS nvarchar(64))      AS branch_external_id,
  CAST(w.CounterpartyCode AS nvarchar(64))AS entity_external_id,
  CAST(w.ItemCode AS nvarchar(128))       AS item_external_id,
  CAST(w.Qty AS decimal(18,4))            AS quantity,
  CAST(w.NetValue AS decimal(14,2))       AS net_amount,
  CAST(w.CostValue AS decimal(14,2))      AS cost_amount,
  CAST(w.InventoryAccountCode AS nvarchar(64)) AS account_external_id,
  CAST(w.LineId AS nvarchar(256))         AS external_id,
  CAST(w.UpdatedAt AS datetime2)          AS updated_at
FROM dbo.WarehouseDocumentLines w
WHERE w.UpdatedAt > @last_sync_ts
ORDER BY w.UpdatedAt, w.LineId;
```

## 1.4 Cash transactions query (minimal)

```sql
SELECT
  CAST(c.DocId AS nvarchar(128))          AS document_id,
  CAST(c.LineNo AS int)                   AS document_line_no,
  CAST('cash_transaction' AS nvarchar(32))AS document_type,
  CAST(c.DocDate AS date)                 AS document_date,
  CAST(c.BranchCode AS nvarchar(64))      AS branch_external_id,
  CAST(c.CounterpartyCode AS nvarchar(64))AS entity_external_id,
  CAST(NULL AS nvarchar(128))             AS item_external_id,
  CAST(0 AS decimal(18,4))                AS quantity,
  CAST(c.Amount AS decimal(14,2))         AS net_amount,
  CAST(0 AS decimal(14,2))                AS cost_amount,
  CAST(c.AccountCode AS nvarchar(64))     AS account_external_id,
  CAST(c.EntryId AS nvarchar(256))        AS external_id,
  CAST(c.UpdatedAt AS datetime2)          AS updated_at,
  CAST(c.SubCategory AS nvarchar(64))     AS cash_subcategory  -- one of 5 required values
FROM dbo.CashTransactions c
WHERE c.UpdatedAt > @last_sync_ts
ORDER BY c.UpdatedAt, c.EntryId;
```

## 2) Derived KPI model

KPI derivation must be stream-only.

| KPI | Source stream(s) | Required fields |
|---|---|---|
| Sales turnover | `fact_sales_documents` | `document_date`, `net_amount`, `document_type`, `document_status` |
| Gross profit | `fact_sales_documents` | `net_amount`, `cost_amount` |
| Sales margin % | `fact_sales_documents` | `net_amount`, `cost_amount` |
| Sales by branch/category/brand/product | `fact_sales_documents` + dimensions | `branch_id`, `item_id`, `net_amount` |
| Purchase spend | `fact_purchase_documents` | `net_amount`, `document_date` |
| Supplier dependency | `fact_purchase_documents` | `entity_id`, `net_amount` |
| Purchase cost volatility | `fact_purchase_documents` | `entity_id`, `cost_amount`, `document_date` |
| Stock movement net | `fact_inventory_documents` | `document_type`, `quantity`, `movement_direction` |
| Inventory valuation | `fact_inventory_documents` | `item_id`, `cost_amount`, `document_date` |
| Stock aging / dead stock | `fact_inventory_documents` + `fact_sales_documents` | `item_id`, `document_date`, `quantity`, last sale date |
| Cash inflow/outflow | `fact_cash_transactions` | `cash_subcategory`, `cash_direction`, `net_amount` |
| Collections vs sales reconciliation | `fact_sales_documents` + `fact_cash_transactions` | sales `net_amount`, cash `cash_subcategory=customer_*` |
| Supplier payment coverage | `fact_purchase_documents` + `fact_cash_transactions` | purchase `net_amount`, cash `cash_subcategory=supplier_*` |
| Account transfer integrity | `fact_cash_transactions` | `cash_subcategory=financial_accounts`, `account_id`, `counterparty_account_id`, `net_amount` |

## 3) Intelligence rules mapping per stream

## 3.1 Sales stream rules
- `SLS_DROP_PERIOD`: sales net drop period-over-period.
- `SLS_SPIKE_PERIOD`: sales net spike.
- `PRF_DROP_PERIOD`: profit drop (`net_amount - cost_amount`).
- `MRG_DROP_POINTS`: margin deterioration.
- `BR_UNDERPERFORM`: branch underperformance.
- `CAT_DROP`, `CAT_MARGIN_EROSION`, `BRAND_DROP`.
- `TOP_DEPENDENCY`, `SLS_VOLATILITY`, `WEEKEND_SHIFT`.

## 3.2 Purchase stream rules
- `PUR_SPIKE_PERIOD`, `PUR_DROP_PERIOD`.
- `SUP_DEPENDENCY`.
- `SUP_COST_UP`.
- `SUP_VOLATILITY`.
- `PUR_MARGIN_PRESSURE` (purchase-cost pressure combined with sales margin trend).

## 3.3 Warehouse stream rules
- Dead stock (no movement + no sales window).
- Aging spike.
- Low coverage on top sellers.
- Overstock slow movers.
- New rules enabled by movement docs:
  - transfer imbalance anomalies
  - shrinkage/adjustment spikes
  - warehouse-level stock leakage indicators

## 3.4 Cash stream rules
- Daily net cash burn.
- Collections shortfall vs expected sales settlement.
- Supplier payment delay vs payable trend.
- Financial account transfer mismatch (debit/credit imbalance).
- Abnormal transfer concentration by account.

## 4) Rule for KPI and intelligence governance

A KPI or intelligence rule is valid only if:
1. It reads from one or more of the four stream facts.
2. It can be traced to canonical document rows by `document_id` and `external_id`.
3. It supports reconciliation with cash/account movements when financial impact exists.
