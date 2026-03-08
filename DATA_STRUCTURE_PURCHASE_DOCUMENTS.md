# Data Structure - Purchase Documents

## 1) Description
Purchase Documents represent supplier-side procurement activity used for cost, supplier, purchase trend, and margin pressure analytics.

## 2) Required Fields
- `external_id`
- `document_id`
- `document_date`
- `document_type`
- `branch_id`
- `supplier_id`
- `product_id`
- `quantity`
- `net_amount`
- `cost_amount`
- `updated_at`

## 3) Optional Fields
- `document_no`
- `document_series`
- `warehouse_id`
- `category_id`
- `brand_id`
- `payment_method_id`
- `currency`
- `vat_amount`
- `discount_amount`
- `status`
- `source_connector_id`
- `metadata_json`

## 4) Field Definitions
- `external_id`: stable source row identifier.
- `document_id`: purchase document identifier.
- `document_date`: posting date.
- `document_type`: invoice, receipt, purchase_credit_note, etc.
- `branch_id`: organization location.
- `supplier_id`: supplier identifier.
- `product_id`: purchased product.
- `quantity`: purchased quantity.
- `net_amount`: net purchase value.
- `cost_amount`: cost value used in purchasing/cost analytics.
- `updated_at`: source row last update.

## 5) Data Types
- string: `external_id`, `document_id`, `document_type`, `branch_id`, `supplier_id`, `product_id`
- date: `document_date`
- decimal: `quantity`, `net_amount`, `cost_amount`
- datetime: `updated_at`

## 6) Example JSON Payload
```json
{
  "stream_code": "purchase_documents",
  "records": [
    {
      "external_id": "PUR-ERP1-20260308-000887",
      "document_id": "PDOC-55621",
      "document_no": "T-8854",
      "document_series": "T",
      "document_date": "2026-03-08",
      "document_type": "supplier_invoice",
      "branch_id": "BR-ATH-01",
      "warehouse_id": "WH-ATH-01",
      "supplier_id": "SUP-3009",
      "product_id": "PRD-4501",
      "category_id": "CAT-DERM",
      "brand_id": "BRN-LRP",
      "quantity": 24.0,
      "net_amount": 720.00,
      "cost_amount": 720.00,
      "vat_amount": 172.80,
      "currency": "EUR",
      "status": "posted",
      "updated_at": "2026-03-08T17:20:10Z"
    }
  ]
}
```

## 7) Example CSV Format
```csv
external_id,document_id,document_no,document_series,document_date,document_type,branch_id,warehouse_id,supplier_id,product_id,category_id,brand_id,quantity,net_amount,cost_amount,vat_amount,currency,status,updated_at
PUR-ERP1-20260308-000887,PDOC-55621,T-8854,T,2026-03-08,supplier_invoice,BR-ATH-01,WH-ATH-01,SUP-3009,PRD-4501,CAT-DERM,BRN-LRP,24,720.00,720.00,172.80,EUR,posted,2026-03-08T17:20:10Z
```

## 8) Business Rules
- `external_id` unique per connector.
- `document_date` mandatory.
- Purchase credit notes must follow sign rules from Business Rules configuration.
- `supplier_id` mandatory for supplier-level analytics.
- `cost_amount` mandatory for purchase trend and margin pressure calculations.

## 9) Mapping Notes
Typical ERP mapping:
- ERP `PUR_DOC_ID` -> `document_id`
- ERP `PUR_DATE` -> `document_date`
- ERP `SUPPLIER_CODE` -> `supplier_id`
- ERP `ITEM_CODE` -> `product_id`
- ERP `QTY` -> `quantity`
- ERP `NET_VALUE` -> `net_amount`
- ERP `COST_VALUE` -> `cost_amount`
- ERP `LAST_UPDATE` -> `updated_at`
