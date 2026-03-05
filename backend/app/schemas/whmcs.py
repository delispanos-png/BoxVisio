from pydantic import BaseModel


class WhmcsPayload(BaseModel):
    service_id: str
    product_id: str | None = None
    tenant_name: str | None = None
    tenant_slug: str | None = None
    admin_email: str | None = None
