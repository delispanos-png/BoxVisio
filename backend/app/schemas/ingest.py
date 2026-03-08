from datetime import date, datetime

from pydantic import BaseModel, Field


class IngestRecord(BaseModel):
    event_id: str = Field(min_length=1, max_length=128)
    doc_date: date
    document_type: str | None = None
    document_id: str | None = None
    branch_ext_id: str | None = None
    branch_external_id: str | None = None
    warehouse_ext_id: str | None = None
    warehouse_external_id: str | None = None
    entity_ext_id: str | None = None
    customer_ext_id: str | None = None
    brand_ext_id: str | None = None
    category_ext_id: str | None = None
    group_ext_id: str | None = None
    supplier_ext_id: str | None = None
    item_code: str | None = None
    item_external_id: str | None = None
    external_id: str | None = None
    updated_at: datetime | None = None
    qty: float = 0
    net_value: float = 0
    gross_value: float = 0
    cost_amount: float = 0
    qty_on_hand: float = 0
    qty_reserved: float = 0
    value_amount: float = 0
    entry_type: str | None = None
    amount: float = 0
    currency: str | None = None
    reference_no: str | None = None
    notes: str | None = None
    supplier_id: str | None = None
    balance_date: date | None = None
    open_balance: float = 0
    overdue_balance: float = 0
    aging_bucket_0_30: float = 0
    aging_bucket_31_60: float = 0
    aging_bucket_61_90: float = 0
    aging_bucket_90_plus: float = 0
    last_payment_date: date | None = None
    trend_vs_previous: float | None = None
    customer_id: str | None = None
    customer_name: str | None = None
    last_collection_date: date | None = None


class IngestBatchRequest(BaseModel):
    records: list[IngestRecord]
