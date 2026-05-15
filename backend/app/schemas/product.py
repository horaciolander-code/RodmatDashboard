from datetime import datetime
from pydantic import BaseModel


class ProductCreate(BaseModel):
    sku: str
    name: str
    category: str | None = None
    price_sale: float | None = None
    price_cost: float | None = None
    supplier: str | None = None
    units_per_box: int | None = None
    status: str = "active"


class ProductUpdate(BaseModel):
    name: str | None = None
    category: str | None = None
    price_sale: float | None = None
    price_cost: float | None = None
    supplier: str | None = None
    units_per_box: int | None = None
    status: str | None = None


class ProductResponse(BaseModel):
    id: str
    store_id: str
    sku: str
    name: str
    category: str | None
    price_sale: float | None
    price_cost: float | None
    supplier: str | None
    units_per_box: int | None
    status: str
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}
