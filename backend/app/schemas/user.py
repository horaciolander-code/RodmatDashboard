import re
from datetime import datetime
from pydantic import BaseModel, EmailStr, field_validator, Field


class UserCreate(BaseModel):
    email: EmailStr
    password: str
    store_id: str
    role: str = "viewer"

    @field_validator("password")
    @classmethod
    def password_strength(cls, v: str) -> str:
        if len(v) < 8:
            raise ValueError("Password must be at least 8 characters")
        if not re.search(r"\d", v):
            raise ValueError("Password must contain at least one digit")
        return v


class UserRegister(BaseModel):
    email: EmailStr
    password: str
    store_name: str = Field(min_length=2, max_length=100)

    @field_validator("password")
    @classmethod
    def password_strength(cls, v: str) -> str:
        if len(v) < 8:
            raise ValueError("Password must be at least 8 characters")
        if not re.search(r"\d", v):
            raise ValueError("Password must contain at least one digit")
        return v


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


class UserResponse(BaseModel):
    id: str
    email: str
    store_id: str
    role: str
    created_at: datetime

    model_config = {"from_attributes": True}
