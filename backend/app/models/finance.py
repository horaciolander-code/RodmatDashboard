from sqlalchemy import Column, String, Date, Numeric, ForeignKey, DateTime, func, Index
from sqlalchemy.orm import relationship
from app.database import Base
import uuid


def _uuid():
    return str(uuid.uuid4())


class FinanceCustomLine(Base):
    """
    Líneas custom de la calculadora de Finance — alquileres, sueldos, gastos extras, etc.
    Una fila = una línea editable por Oralia para un mes concreto.
    amount > 0 = ingreso, amount < 0 = gasto.
    """
    __tablename__ = "finance_custom_lines"
    __table_args__ = (
        Index("ix_finance_custom_lines_store_period", "store_id", "year_month"),
    )

    id          = Column(String(36),  primary_key=True, default=_uuid)
    store_id    = Column(String(36),  ForeignKey("stores.id"), nullable=False, index=True)
    year_month  = Column(String(7),   nullable=False)   # 'YYYY-MM'
    description = Column(String(255), nullable=False)
    amount      = Column(Numeric(14, 2), nullable=False, default=0)
    sort_order  = Column(Numeric(8, 2),  nullable=False, default=0)
    created_at  = Column(DateTime(timezone=True), server_default=func.now())
    updated_at  = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    store = relationship("Store")
