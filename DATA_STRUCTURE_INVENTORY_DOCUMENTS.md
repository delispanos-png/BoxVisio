# Data Structure - Inventory Documents

## 1) Description
Inventory Documents represent stock movements and stock-state events used for inventory KPIs (on-hand, aging, turnover, fast/slow movers).

## 2) Required Fields
- `external_id`
- `movement_id`
- `movement_date`
- `movement_type`
- `branch_id`
- `warehouse_id`
- `product_id`
- `quantity`
- `value_amount`
- `updated_at`

## 3) Optional Fields
- `document_id`
- `document_type`
- `from_warehouse_id`
- `to_warehouse_id`
- `reserved_quantity`
- `available_quantity`
- `unit_cost`
- `reason_code`
- `batch_no`
- `lot_no`
- `expiry_date`
- `category_id`
- `brand_id`
- `status`
- `source_connector_id`
- `metadata_json`

## 4) Field Definitions
- `external_id`: stable source key.
- `movement_id`: movement event id.
- `movement_date`: stock movement posting date.
- `movement_type`: `entry`, `exit`, `transfer`, `adjustment`, `snapshot`.
- `branch_id`: business location.
- `warehouse_id`: stock location.
- `product_id`: inventory item.
- `quantity`: movement quantity (signed or unsigned based on rule mapping).
- `value_amount`: movement value impact.
- `updated_at`: source update datetime.

## 5) Data Types
- string: `external_id`, `movement_id`, `movement_type`, `branch_id`, `warehouse_id`, `product_id`
- date: `movement_date`
- decimal: `quantity`, `value_amount`
- datetime: `updated_at`

## 6) Example JSON Payload
```json
{
  "stream_code": "inventory_documents",
  "records": [
    {
      "external_id": "INV-ERP1-20260308-00911",
      "movement_id": "MV-00911",
      "movement_date": "2026-03-08",
      "movement_type": "transfer",
      "branch_id": "BR-ATH-01",
      "warehouse_id": "WH-ATH-01",
      "from_warehouse_id": "WH-ATH-01",
      "to_warehouse_id": "WH-ATH-02",
      "product_id": "PRD-4501",
      "quantity": 10.0,
      "value_amount": 280.00,
      "unit_cost": 28.00,
      "updated_at": "2026-03-08T16:10:05Z"
    }
  ]
}
```

## 7) Example CSV Format
```csv
external_id,movement_id,movement_date,movement_type,branch_id,warehouse_id,from_warehouse_id,to_warehouse_id,product_id,quantity,value_amount,unit_cost,updated_at
INV-ERP1-20260308-00911,MV-00911,2026-03-08,transfer,BR-ATH-01,WH-ATH-01,WH-ATH-01,WH-ATH-02,PRD-4501,10,280.00,28.00,2026-03-08T16:10:05Z
```

## 8) Business Rules
- `movement_type` must be one of allowed values.
- Transfer can be modeled either:
  - one row with `from_warehouse_id` and `to_warehouse_id`, or
  - two rows (exit + entry), based on connector mapping policy.
- `movement_date` is mandatory.
- Snapshot rows should provide `available_quantity` and `reserved_quantity` when possible.

## 9) Mapping Notes
Typical ERP mapping:
- ERP `MOVE_ID` -> `movement_id`
- ERP `MOVE_DATE` -> `movement_date`
- ERP `MOVE_TYPE` -> `movement_type`
- ERP `WH_CODE` -> `warehouse_id`
- ERP `ITEM_CODE` -> `product_id`
- ERP `QTY` -> `quantity`
- ERP `VALUE` -> `value_amount`
- ERP `UNIT_COST` -> `unit_cost`
- ERP `UPDATED` -> `updated_at`
