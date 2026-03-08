# Integration Examples

## 1) API Envelope Pattern
All API ingestion payloads use the same envelope:

```json
{
  "stream_code": "<stream_code>",
  "records": [
    {
      "...": "..."
    }
  ]
}
```

## 2) Sales Documents (`sales_documents`)
```json
{
  "stream_code": "sales_documents",
  "records": [
    {
      "external_id": "INV-2024-001",
      "document_type": "sales_invoice",
      "document_date": "2024-02-01",
      "branch_code": "ATH01",
      "customer_code": "CUST100",
      "product_code": "PRD001",
      "quantity": 2,
      "net_amount": 50.00,
      "cost_amount": 30.00,
      "brand": "BRAND_A",
      "category": "CATEGORY_A",
      "warehouse": "WH01",
      "payment_method": "CARD",
      "salesperson": "SP01"
    }
  ]
}
```

CSV:
```csv
external_id,document_type,document_date,branch_code,customer_code,product_code,quantity,net_amount,cost_amount,brand,category,warehouse,payment_method,salesperson
INV-2024-001,sales_invoice,2024-02-01,ATH01,CUST100,PRD001,2,50.00,30.00,BRAND_A,CATEGORY_A,WH01,CARD,SP01
```

## 3) Purchase Documents (`purchase_documents`)
```json
{
  "stream_code": "purchase_documents",
  "records": [
    {
      "external_id": "PUR-2024-0001",
      "document_id": "PDOC-0001",
      "document_date": "2024-02-01",
      "document_type": "supplier_invoice",
      "branch_id": "ATH01",
      "supplier_id": "SUP100",
      "product_id": "PRD001",
      "quantity": 20,
      "net_amount": 400.00,
      "cost_amount": 400.00,
      "updated_at": "2024-02-01T10:15:00Z"
    }
  ]
}
```

CSV:
```csv
external_id,document_id,document_date,document_type,branch_id,supplier_id,product_id,quantity,net_amount,cost_amount,updated_at
PUR-2024-0001,PDOC-0001,2024-02-01,supplier_invoice,ATH01,SUP100,PRD001,20,400.00,400.00,2024-02-01T10:15:00Z
```

## 4) Inventory Documents (`inventory_documents`)
```json
{
  "stream_code": "inventory_documents",
  "records": [
    {
      "external_id": "MOV-2024-001",
      "movement_id": "MV-0001",
      "movement_date": "2024-02-01",
      "movement_type": "entry",
      "branch_id": "ATH01",
      "warehouse_id": "WH01",
      "product_id": "PRD001",
      "quantity": 50,
      "value_amount": 1000.00,
      "updated_at": "2024-02-01T09:00:00Z"
    }
  ]
}
```

CSV:
```csv
external_id,movement_id,movement_date,movement_type,branch_id,warehouse_id,product_id,quantity,value_amount,updated_at
MOV-2024-001,MV-0001,2024-02-01,entry,ATH01,WH01,PRD001,50,1000.00,2024-02-01T09:00:00Z
```

## 5) Cash Transactions (`cash_transactions`)
```json
{
  "stream_code": "cash_transactions",
  "records": [
    {
      "transaction_id": "CASH-2024-0001",
      "transaction_date": "2024-02-01",
      "transaction_type": "inflow",
      "subcategory": "customer_collections",
      "branch_id": "ATH01",
      "account_id": "BANK01",
      "counterparty_type": "customer",
      "counterparty_id": "CUST100",
      "amount": 250.00,
      "external_id": "CASH-EXT-2024-0001",
      "updated_at": "2024-02-01T11:00:00Z"
    }
  ]
}
```

CSV:
```csv
transaction_id,transaction_date,transaction_type,subcategory,branch_id,account_id,counterparty_type,counterparty_id,amount,external_id,updated_at
CASH-2024-0001,2024-02-01,inflow,customer_collections,ATH01,BANK01,customer,CUST100,250.00,CASH-EXT-2024-0001,2024-02-01T11:00:00Z
```

## 6) Supplier Balances (`supplier_balances`)
```json
{
  "stream_code": "supplier_balances",
  "records": [
    {
      "supplier_id": "SUP100",
      "balance_date": "2024-02-01",
      "open_balance": 5000.00,
      "overdue_balance": 1200.00,
      "aging_bucket_0_30": 1800.00,
      "aging_bucket_31_60": 1000.00,
      "aging_bucket_61_90": 900.00,
      "aging_bucket_90_plus": 1300.00,
      "last_payment_date": "2024-01-28",
      "trend_vs_previous": 220.00
    }
  ]
}
```

CSV:
```csv
supplier_id,balance_date,open_balance,overdue_balance,aging_bucket_0_30,aging_bucket_31_60,aging_bucket_61_90,aging_bucket_90_plus,last_payment_date,trend_vs_previous
SUP100,2024-02-01,5000.00,1200.00,1800.00,1000.00,900.00,1300.00,2024-01-28,220.00
```

## 7) Customer Balances (`customer_balances`)
```json
{
  "stream_code": "customer_balances",
  "records": [
    {
      "customer_id": "CUST100",
      "balance_date": "2024-02-01",
      "open_balance": 2800.00,
      "overdue_balance": 700.00,
      "aging_bucket_0_30": 900.00,
      "aging_bucket_31_60": 600.00,
      "aging_bucket_61_90": 500.00,
      "aging_bucket_90_plus": 800.00,
      "last_collection_date": "2024-01-30",
      "trend_vs_previous": -100.00,
      "external_id": "CUSBAL-2024-02-01-CUST100",
      "updated_at": "2024-02-01T12:00:00Z"
    }
  ]
}
```

CSV:
```csv
customer_id,balance_date,open_balance,overdue_balance,aging_bucket_0_30,aging_bucket_31_60,aging_bucket_61_90,aging_bucket_90_plus,last_collection_date,trend_vs_previous,external_id,updated_at
CUST100,2024-02-01,2800.00,700.00,900.00,600.00,500.00,800.00,2024-01-30,-100.00,CUSBAL-2024-02-01-CUST100,2024-02-01T12:00:00Z
```

## 8) Quick cURL Examples
Sales:
```bash
curl -X POST "https://bi.boxvisio.com/v1/ingest/sales" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <token>" \
  -H "X-Tenant: <tenant_slug>" \
  -d @sales_payload.json
```

Purchases:
```bash
curl -X POST "https://bi.boxvisio.com/v1/ingest/purchases" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <token>" \
  -H "X-Tenant: <tenant_slug>" \
  -d @purchases_payload.json
```

## 9) Notes
- For exact required fields per stream, always validate against the `DATA_STRUCTURE_*` documents.
- If a field is optional in source, keep it out or send `null` only if connector mapping supports nullability.
- Keep `external_id` stable to guarantee idempotent re-runs.
