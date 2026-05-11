import uuid
from datetime import datetime, timezone

from sqlalchemy import String, DateTime, Integer, ForeignKey, UniqueConstraint, Index
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class AmazonSkuMap(Base):
    __tablename__ = "amazon_sku_map"
    __table_args__ = (
        UniqueConstraint("store_id", "amazon_sku", name="uq_amazon_sku_store"),
        Index("ix_amazon_sku_map_store_id", "store_id"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    store_id: Mapped[str] = mapped_column(String(36), ForeignKey("stores.id"), nullable=False)
    amazon_sku: Mapped[str] = mapped_column(String(100), nullable=False)
    asin: Mapped[str | None] = mapped_column(String(20), nullable=True)
    amazon_product_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    product_id: Mapped[str | None] = mapped_column(String(36), ForeignKey("products.id"), nullable=True)
    units_per_sale: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))
