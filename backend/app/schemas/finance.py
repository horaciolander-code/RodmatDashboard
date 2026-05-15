from datetime import date
from typing import Optional
from pydantic import BaseModel


class TransactionOut(BaseModel):
    id: str
    date: Optional[date]
    description: str
    amount: float
    running_balance: Optional[float]
    tipo: str
    clasificacion: str
    comentarios: str
    is_pending_review: bool
    classification_method: str
    classification_confidence: float

    class Config:
        from_attributes = True


class TransactionUpdate(BaseModel):
    tipo: str
    clasificacion: str
    comentarios: Optional[str] = None


class PreviewRow(BaseModel):
    fecha: Optional[str]
    description: str
    amount: float
    running_balance: Optional[float]
    tipo: str
    clasificacion: str
    confidence: float
    method: str


class ImportRow(BaseModel):
    fecha: Optional[str]
    description: str
    amount: float
    running_balance: Optional[float]
    tipo: str
    clasificacion: str


class ImportResult(BaseModel):
    added: int
    duplicates: int
    pending: int
