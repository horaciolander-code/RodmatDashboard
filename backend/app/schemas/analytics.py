from pydantic import BaseModel
from typing import Optional


class SalesByPeriod(BaseModel):
    period: str
    GMV: float = 0
    Orders: int = 0
    Units: int = 0


class FilteredOrdersResponse(BaseModel):
    total: int
    orders: list[dict]
