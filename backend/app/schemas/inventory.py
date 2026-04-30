from datetime import date, datetime
from pydantic import BaseModel


class InitialInventoryCreate(BaseModel):
    product_id: str
    quantity: int
    start_date: date
    location: str | None = None
    notes: str | None = None


class InitialInventoryResponse(BaseModel):
    id: str
    store_id: str
    product_id: str
    quantity: int
    start_date: date
    location: str | None
    notes: str | None

    model_config = {"from_attributes": True}


class IncomingStockCreate(BaseModel):
    product_id: str
    qty_ordered: int
    order_date: date | None = None
    expected_arrival: date | None = None
    status: str = "pending"
    supplier: str | None = None
    tracking: str | None = None
    cost: float | None = None
    notes: str | None = None


class IncomingStockResponse(BaseModel):
    id: str
    store_id: str
    product_id: str
    qty_ordered: int
    order_date: date | None
    expected_arrival: date | None
    actual_arrival: date | None
    status: str
    supplier: str | None
    tracking: str | None
    cost: float | None
    notes: str | None

    model_config = {"from_attributes": True}


class IncomingStockUpdate(BaseModel):
    qty_ordered: int | None = None
    order_date: date | None = None
    expected_arrival: date | None = None
    actual_arrival: date | None = None
    status: str | None = None
    supplier: str | None = None
    tracking: str | None = None
    cost: float | None = None
    notes: str | None = None
