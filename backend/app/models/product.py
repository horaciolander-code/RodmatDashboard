import uuid
from datetime import datetime, timezone

from sqlalchemy import String, DateTime, Float, Integer, ForeignKey, UniqueConstraint, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


class Product(Base):
    __tablename__ = "products"
    __table_args__ = (
        UniqueConstraint("store_id", "sku", name="uq_store_sku"),
        Index("ix_products_store_id", "store_id"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    store_id: Mapped[str] = mapped_column(String(36), ForeignKey("stores.id"), nullable=False)
    sku: Mapped[str] = mapped_column(String(100), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    category: Mapped[str | None] = mapped_column(String(100), nullable=True)
    price_sale: Mapped[float | None] = mapped_column(Float, nullable=True)
    price_cost: Mapped[float | None] = mapped_column(Float, nullable=True)
    supplier: Mapped[str | None] = mapped_column(String(255), nullable=True)
    units_per_box: Mapped[int | None] = mapped_column(Integer, nullable=True)
    status: Mapped[str] = mapped_column(String(20), default="active")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))

    store = relationship("Store", back_populates="products")
