"""WalmartSkuMap — mapea Walmart SKU → product_id del catálogo + units_per_sale.

Misma estructura que AmazonSkuMap. Cuando Walmart vende AV-MD12 con qty=1 y
units_per_sale=12 → el parser inserta quantity=12 en sales_orders y se descuenta
del stock del producto base (Avon Mesmerize Deodorant roll-on)."""
import uuid
from datetime import datetime, timezone

from sqlalchemy import String, DateTime, Integer, ForeignKey, UniqueConstraint, Index
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class WalmartSkuMap(Base):
    __tablename__ = "walmart_sku_map"
    __table_args__ = (
        UniqueConstraint("store_id", "walmart_sku", name="uq_walmart_sku_store"),
        Index("ix_walmart_sku_map_store_id", "store_id"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    store_id: Mapped[str] = mapped_column(String(36), ForeignKey("stores.id"), nullable=False)
    walmart_sku: Mapped[str] = mapped_column(String(100), nullable=False)
    walmart_item_id: Mapped[str | None] = mapped_column(String(40), nullable=True)
    walmart_product_name: Mapped[str | None] = mapped_column(String(500), nullable=True)
    product_id: Mapped[str | None] = mapped_column(String(36), ForeignKey("products.id"), nullable=True)
    units_per_sale: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))
