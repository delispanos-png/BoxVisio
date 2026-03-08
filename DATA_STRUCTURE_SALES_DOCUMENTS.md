# Data Structure - Sales Documents

## 1) Description
Sales Documents represent revenue-side commercial activity used for sales KPIs, margin analytics, trend reporting, and stream insights.

## 2) Required Fields
- `external_id`
- `document_type`
- `document_date`
- `branch_code`
- `customer_code`
- `product_code`
- `quantity`
- `net_amount`
- `cost_amount`

## 3) Optional Fields
- `document_id`
- `document_no`
- `document_series`
- `branch_id`
- `customer_id`
- `product_id`
- `warehouse_id`
- `warehouse`
- `category_code`
- `category`
- `brand_code`
- `brand`
- `payment_method_id`
- `payment_method`
- `salesperson`
- `currency`
- `gross_amount`
- `vat_amount`
- `discount_amount`
- `status`
- `updated_at`
- `source_connector_id`
- `metadata_json`

## 4) Field Definitions
- `external_id`: stable source row identifier for idempotency.
- `document_type`: source document classification (invoice, receipt, credit_note, pos_sale, etc.).
- `document_date`: commercial posting date.
- `branch_code`: branch/store code from source ERP.
- `customer_code`: customer code from source ERP.
- `product_code`: product/item code from source ERP.
- `quantity`: sold quantity.
- `net_amount`: net sales value (before VAT).
- `cost_amount`: cost of goods for margin/profit calculations.
- `brand`: brand code or brand name from source (optional enrichment).
- `category`: category code or category name from source (optional enrichment).
- `warehouse`: warehouse code/name from source (optional enrichment).
- `payment_method`: payment method code/name from source (optional enrichment).
- `salesperson`: sales representative code/name from source (optional enrichment).

## 5) Data Types
- string: `external_id`, `document_type`, `branch_code`, `customer_code`, `product_code`, `brand`, `category`, `warehouse`, `payment_method`, `salesperson`
- date: `document_date`
- decimal: `quantity`, `net_amount`, `cost_amount`

## 6) Example JSON Payload
```json
{
  "stream_code": "sales_documents",
  "records": [
    {
      "external_id": "SLS-ERP1-20260308-000123",
      "document_type": "invoice",
      "document_date": "2026-03-08",
      "branch_code": "BR-ATH-01",
      "customer_code": "CUS-1122",
      "product_code": "PRD-4501",
      "quantity": 2.0,
      "net_amount": 90.00,
      "cost_amount": 56.00,
      "brand": "APIVITA",
      "category": "DERMOCOSMETICS",
      "warehouse": "WH-ATH-01",
      "payment_method": "CARD",
      "salesperson": "SP-014"
    }
  ]
}
```

## 7) Example CSV Format
```csv
external_id,document_type,document_date,branch_code,customer_code,product_code,quantity,net_amount,cost_amount,brand,category,warehouse,payment_method,salesperson
SLS-ERP1-20260308-000123,invoice,2026-03-08,BR-ATH-01,CUS-1122,PRD-4501,2,90.00,56.00,APIVITA,DERMOCOSMETICS,WH-ATH-01,CARD,SP-014
```

## 8) Business Rules
- `external_id` must be unique per connector.
- `document_date` is mandatory and valid.
- `branch_code`, `customer_code`, `product_code` must be non-empty codes.
- Document sign handling is rule-driven (admin Business Rules):
  - invoices/receipts usually positive
  - credit notes usually negative
- `quantity`, `net_amount`, `cost_amount` must be numeric.
- If provided, `brand`, `category`, `warehouse`, `payment_method`, `salesperson` are normalized to their corresponding dimensions.

## 9) Mapping Notes
Typical ERP mapping:
- ERP `TRNTYPE` -> `document_type`
- ERP `TRNDATE` -> `document_date`
- ERP `STORE` / `BRANCH` -> `branch_code`
- ERP `CUSTOMER` / `TRDR` -> `customer_code`
- ERP `ITEMCODE` -> `product_code`
- ERP `QTY` -> `quantity`
- ERP `NET` -> `net_amount`
- ERP `COST` -> `cost_amount`
- ERP `BRAND` -> `brand`
- ERP `CATEGORY` -> `category`
- ERP `WHOUSE` / `WAREHOUSE` -> `warehouse`
- ERP `PAYMENT` -> `payment_method`
- ERP `SALESMAN` / `SALESPERSON` -> `salesperson`

Canonical mapping (internal transform after staging):
- `branch_code` -> `branch_id`/`branch_ext_id`
- `customer_code` -> `customer_id`/`customer_ext_id`
- `product_code` -> `product_id`/`item_code`
- `brand` -> `brand_id`/`brand_code`
- `category` -> `category_id`/`category_code`
- `warehouse` -> `warehouse_id`/`warehouse_code`
- `payment_method` -> `payment_method_id`/`payment_method_code`
- `salesperson` -> `salesperson_id`/`salesperson_code`
