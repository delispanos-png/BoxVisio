from datetime import date, datetime

from pydantic import BaseModel, Field


class IngestRecord(BaseModel):
    event_id: str = Field(min_length=1, max_length=128)
    doc_date: date
    branch_ext_id: str | None = None
    warehouse_ext_id: str | None = None
    brand_ext_id: str | None = None
    category_ext_id: str | None = None
    group_ext_id: str | None = None
    supplier_ext_id: str | None = None
    item_code: str | None = None
    external_id: str | None = None
    updated_at: datetime | None = None
    qty: float = 0
    net_value: float = 0
    gross_value: float = 0
    cost_amount: float = 0


class IngestBatchRequest(BaseModel):
    records: list[IngestRecord]
