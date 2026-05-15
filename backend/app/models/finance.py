import uuid
from datetime import date, datetime, timezone

from sqlalchemy import Boolean, Date, DateTime, Float, ForeignKey, Index, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class BankTransaction(Base):
    __tablename__ = "bank_transactions"
    __table_args__ = (
        Index("ix_bank_transactions_store_id", "store_id"),
        Index("ix_bank_transactions_date", "date"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    store_id: Mapped[str] = mapped_column(String(36), ForeignKey("stores.id"), nullable=False)
    transaction_key: Mapped[str] = mapped_column(String(250), nullable=False)
    date: Mapped[date | None] = mapped_column(Date, nullable=True)
    description: Mapped[str] = mapped_column(Text, nullable=False, default="")
    amount: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    running_balance: Mapped[float | None] = mapped_column(Float, nullable=True)
    tipo: Mapped[str] = mapped_column(String(50), nullable=False, default="Pendiente")
    clasificacion: Mapped[str] = mapped_column(String(200), nullable=False, default="")
    comentarios: Mapped[str] = mapped_column(Text, nullable=False, default="")
    is_pending_review: Mapped[bool] = mapped_column(Boolean, default=True)
    classification_method: Mapped[str] = mapped_column(String(50), nullable=False, default="pending")
    classification_confidence: Mapped[float] = mapped_column(Float, default=0.0)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))
