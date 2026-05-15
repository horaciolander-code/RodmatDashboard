import uuid
from datetime import datetime, timezone

from sqlalchemy import String, DateTime, Integer, ForeignKey, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


class Combo(Base):
    __tablename__ = "combos"
    __table_args__ = (UniqueConstraint("store_id", "combo_sku", name="uq_store_combo_sku"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    store_id: Mapped[str] = mapped_column(String(36), ForeignKey("stores.id"), nullable=False)
    combo_sku: Mapped[str] = mapped_column(String(100), nullable=False)
    combo_name: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))

    store = relationship("Store", back_populates="combos")
    items = relationship("ComboItem", back_populates="combo", cascade="all, delete-orphan")


class ComboItem(Base):
    __tablename__ = "combo_items"
    __table_args__ = (UniqueConstraint("combo_id", "product_id", name="uq_combo_product"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    combo_id: Mapped[str] = mapped_column(String(36), ForeignKey("combos.id", ondelete="CASCADE"), nullable=False)
    product_id: Mapped[str] = mapped_column(String(36), ForeignKey("products.id"), nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, default=1)

    combo = relationship("Combo", back_populates="items")
    product = relationship("Product")
