import uuid
from datetime import datetime, date, timezone

from sqlalchemy import String, DateTime, Integer, Float, Date, Text, ForeignKey, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


class InitialInventory(Base):
    __tablename__ = "initial_inventory"
    __table_args__ = (
        Index("ix_initial_inventory_store_id", "store_id"),
        Index("ix_initial_inventory_product_id", "product_id"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    store_id: Mapped[str] = mapped_column(String(36), ForeignKey("stores.id"), nullable=False)
    product_id: Mapped[str] = mapped_column(String(36), ForeignKey("products.id"), nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    start_date: Mapped[date] = mapped_column(Date, nullable=False)
    location: Mapped[str | None] = mapped_column(String(100), nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    product = relationship("Product")


class IncomingStock(Base):
    __tablename__ = "incoming_stock"
    __table_args__ = (
        Index("ix_incoming_stock_store_id", "store_id"),
        Index("ix_incoming_stock_product_id", "product_id"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    store_id: Mapped[str] = mapped_column(String(36), ForeignKey("stores.id"), nullable=False)
    product_id: Mapped[str] = mapped_column(String(36), ForeignKey("products.id"), nullable=False)
    qty_ordered: Mapped[int] = mapped_column(Integer, nullable=False)
    order_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    expected_arrival: Mapped[date | None] = mapped_column(Date, nullable=True)
    actual_arrival: Mapped[date | None] = mapped_column(Date, nullable=True)
    status: Mapped[str] = mapped_column(String(20), default="pending")
    supplier: Mapped[str | None] = mapped_column(String(255), nullable=True)
    tracking: Mapped[str | None] = mapped_column(String(255), nullable=True)
    cost: Mapped[float | None] = mapped_column(Float, nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    product = relationship("Product")
