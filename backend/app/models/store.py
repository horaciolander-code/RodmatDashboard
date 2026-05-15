import uuid
from datetime import datetime, timezone

from sqlalchemy import String, DateTime, JSON
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


class Store(Base):
    __tablename__ = "stores"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    owner_email: Mapped[str] = mapped_column(String(255), nullable=False)
    currency: Mapped[str] = mapped_column(String(10), default="USD")
    timezone: Mapped[str] = mapped_column(String(50), default="America/New_York")
    settings: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))

    users = relationship("User", back_populates="store", cascade="all, delete-orphan")
    products = relationship("Product", back_populates="store", cascade="all, delete-orphan")
    combos = relationship("Combo", back_populates="store", cascade="all, delete-orphan")
