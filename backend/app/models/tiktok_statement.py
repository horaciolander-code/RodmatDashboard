"""TikTok Merchant Statement (Profit & Loss) — datos de settlement bancario.
Fuente: extracción TikTok Seller Center → Merchant Statement → Profit and Loss.
Sirve para reconciliar ventas facturadas vs cobradas + tracking por brand."""
import uuid
from datetime import datetime, timezone, date

from sqlalchemy import String, DateTime, Integer, Numeric, Date, ForeignKey, UniqueConstraint, Index
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class TiktokStatement(Base):
    """Cabecera del payout (lo que llega al banco)."""
    __tablename__ = "tiktok_statements"
    __table_args__ = (
        UniqueConstraint("store_id", "statement_id", name="uq_tt_stmt"),
        Index("ix_tt_stmts_store", "store_id"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    store_id: Mapped[str] = mapped_column(String(36), ForeignKey("stores.id"), nullable=False)
    statement_id: Mapped[str] = mapped_column(String(50), nullable=False)
    payout_id: Mapped[str | None] = mapped_column(String(50), nullable=True)
    total_income: Mapped[float | None] = mapped_column(Numeric(14, 2), default=0)
    total_cost: Mapped[float | None] = mapped_column(Numeric(14, 2), default=0)
    total_margin: Mapped[float | None] = mapped_column(Numeric(14, 2), default=0)
    total_fees: Mapped[float | None] = mapped_column(Numeric(14, 2), default=0)
    total_orders: Mapped[int | None] = mapped_column(Integer, default=0)
    period_start: Mapped[date | None] = mapped_column(Date, nullable=True)
    period_end: Mapped[date | None] = mapped_column(Date, nullable=True)
    settled_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    imported_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    import_batch_id: Mapped[str | None] = mapped_column(String(36), nullable=True)


class TiktokStatementLine(Base):
    """Detalle por línea (order_id + sku_id interno TikTok)."""
    __tablename__ = "tiktok_statement_lines"
    __table_args__ = (
        UniqueConstraint("store_id", "order_id", "sku_id", name="uq_tt_line"),
        Index("ix_tt_lines_store_paid", "store_id", "order_paid_date"),
        Index("ix_tt_lines_store_settled", "store_id", "order_settled_date"),
        Index("ix_tt_lines_order", "order_id"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    store_id: Mapped[str] = mapped_column(String(36), ForeignKey("stores.id"), nullable=False)
    statement_id: Mapped[str | None] = mapped_column(String(50), nullable=True)
    payout_id: Mapped[str | None] = mapped_column(String(50), nullable=True)
    order_id: Mapped[str] = mapped_column(String(50), nullable=False)
    sku_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    product_name: Mapped[str | None] = mapped_column(String(500), nullable=True)
    order_income: Mapped[float | None] = mapped_column(Numeric(14, 2), default=0)
    order_cost: Mapped[float | None] = mapped_column(Numeric(14, 2), default=0)
    net_order_margin: Mapped[float | None] = mapped_column(Numeric(14, 2), default=0)
    sold_quantity: Mapped[int | None] = mapped_column(Integer, default=0)
    order_paid_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    order_shipment_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    order_delivery_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    order_settled_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    order_status: Mapped[str | None] = mapped_column(String(30), nullable=True)
    unsettled_reason: Mapped[str | None] = mapped_column(String(200), nullable=True)
    gross_sales: Mapped[float | None] = mapped_column(Numeric(14, 2), default=0)
    seller_discount: Mapped[float | None] = mapped_column(Numeric(14, 2), default=0)
    gross_sales_refund: Mapped[float | None] = mapped_column(Numeric(14, 2), default=0)
    referral_fee: Mapped[float | None] = mapped_column(Numeric(14, 2), default=0)
    smart_promo_fee: Mapped[float | None] = mapped_column(Numeric(14, 2), default=0)
    smart_promo_camp_fee: Mapped[float | None] = mapped_column(Numeric(14, 2), default=0)
    managed_service_fee: Mapped[float | None] = mapped_column(Numeric(14, 2), default=0)
    tiktok_shipping_fee: Mapped[float | None] = mapped_column(Numeric(14, 2), default=0)
    fbt_shipping_fee: Mapped[float | None] = mapped_column(Numeric(14, 2), default=0)
    tiktok_ship_incentive: Mapped[float | None] = mapped_column(Numeric(14, 2), default=0)
    affiliate_commission: Mapped[float | None] = mapped_column(Numeric(14, 2), default=0)
    customer_paid_ship: Mapped[float | None] = mapped_column(Numeric(14, 2), default=0)
    sku_subtotal_before: Mapped[float | None] = mapped_column(Numeric(14, 2), default=0)
    sku_subtotal_after: Mapped[float | None] = mapped_column(Numeric(14, 2), default=0)
    order_amount: Mapped[float | None] = mapped_column(Numeric(14, 2), default=0)
    taxes: Mapped[float | None] = mapped_column(Numeric(14, 2), default=0)
    brand_id: Mapped[str | None] = mapped_column(String(36), ForeignKey("brands.id"), nullable=True)
    imported_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    import_batch_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
