from datetime import datetime
from pydantic import BaseModel


class ComboItemCreate(BaseModel):
    product_id: str
    quantity: int = 1


class ComboCreate(BaseModel):
    combo_sku: str
    combo_name: str
    items: list[ComboItemCreate] = []


class ComboItemResponse(BaseModel):
    id: str
    product_id: str
    quantity: int

    model_config = {"from_attributes": True}


class ComboResponse(BaseModel):
    id: str
    store_id: str
    combo_sku: str
    combo_name: str
    created_at: datetime
    items: list[ComboItemResponse] = []

    model_config = {"from_attributes": True}
