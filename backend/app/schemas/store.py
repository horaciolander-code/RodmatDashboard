from datetime import datetime
from pydantic import BaseModel, EmailStr


class StoreCreate(BaseModel):
    name: str
    owner_email: str
    currency: str = "USD"
    timezone: str = "America/New_York"
    settings: dict | None = None


class StoreUpdate(BaseModel):
    name: str | None = None
    owner_email: str | None = None
    currency: str | None = None
    timezone: str | None = None
    settings: dict | None = None


class StoreResponse(BaseModel):
    id: str
    name: str
    owner_email: str
    currency: str
    timezone: str
    settings: dict | None
    created_at: datetime

    model_config = {"from_attributes": True}
