import uuid
from datetime import datetime, timezone

from sqlalchemy import String, DateTime, Integer, Float, JSON, Text, ForeignKey, UniqueConstraint, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


class SalesOrder(Base):
    __tablename__ = "sales_orders"
    __table_args__ = (
        UniqueConstraint("store_id", "tiktok_order_id", "sku", name="uq_store_order_sku"),
        Index("ix_sales_orders_store_id", "store_id"),
        Index("ix_sales_orders_store_date", "store_id", "order_date"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    store_id: Mapped[str] = mapped_column(String(36), ForeignKey("stores.id"), nullable=False)
    tiktok_order_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    order_date: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    sku: Mapped[str | None] = mapped_column(String(100), nullable=True)
    seller_sku: Mapped[str | None] = mapped_column(String(100), nullable=True)
    product_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    quantity: Mapped[int] = mapped_column(Integer, default=1)
    status: Mapped[str | None] = mapped_column(String(50), nullable=True)
    substatus: Mapped[str | None] = mapped_column(String(50), nullable=True)
    price: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Expanded columns for analytics
    shipped_time: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_time: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    sku_subtotal_after_discount: Mapped[float | None] = mapped_column(Float, nullable=True)
    order_amount: Mapped[float | None] = mapped_column(Float, nullable=True)
    order_refund_amount: Mapped[float | None] = mapped_column(Float, nullable=True)
    shipping_fee_after_discount: Mapped[float | None] = mapped_column(Float, nullable=True)
    sku_seller_discount: Mapped[float | None] = mapped_column(Float, nullable=True)
    sku_platform_discount: Mapped[float | None] = mapped_column(Float, nullable=True)
    cancelation_return_type: Mapped[str | None] = mapped_column(String(100), nullable=True)
    fulfillment_type: Mapped[str | None] = mapped_column(String(100), nullable=True)
    buyer_username: Mapped[str | None] = mapped_column(String(255), nullable=True)
    variation: Mapped[str | None] = mapped_column(String(255), nullable=True)
    recipient: Mapped[str | None] = mapped_column(String(255), nullable=True)
    city: Mapped[str | None] = mapped_column(String(255), nullable=True)
    state: Mapped[str | None] = mapped_column(String(100), nullable=True)

    raw_data: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    imported_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))


class AffiliateSale(Base):
    __tablename__ = "affiliate_sales"
    __table_args__ = (
        UniqueConstraint("store_id", "order_id", "sku", name="uq_store_affiliate_order_sku"),
        Index("ix_affiliate_sales_store_id", "store_id"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    store_id: Mapped[str] = mapped_column(String(36), ForeignKey("stores.id"), nullable=False)
    order_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    creator_username: Mapped[str | None] = mapped_column(String(255), nullable=True)
    product_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    sku: Mapped[str | None] = mapped_column(String(100), nullable=True)
    quantity: Mapped[int] = mapped_column(Integer, default=1)
    commission: Mapped[float | None] = mapped_column(Float, nullable=True)
    content_type: Mapped[str | None] = mapped_column(String(50), nullable=True)

    # Expanded columns
    payment_amount: Mapped[float | None] = mapped_column(Float, nullable=True)
    order_status: Mapped[str | None] = mapped_column(String(50), nullable=True)
    time_created: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    commission_rate: Mapped[float | None] = mapped_column(Float, nullable=True)
    est_commission_base: Mapped[float | None] = mapped_column(Float, nullable=True)

    raw_data: Mapped[dict | None] = mapped_column(JSON, nullable=True)
