"""Que ordenes ha abierto el almacen, para el contador diario.

Una fila por orden y dia. El indice unico es lo que hace que el numero
signifique algo: sin el, recargar la pagina sumaria otra vez.

No es un registro de auditoria. No guarda que se hizo con la orden, solo
que alguien la consulto ese dia.
"""
import uuid
from datetime import date, datetime, timezone

from sqlalchemy import Date, DateTime, Index, String
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class AlmacenConsulta(Base):
    __tablename__ = "almacen_consultas"
    __table_args__ = (
        Index("uq_almacen_consulta", "store_id", "user_id", "order_id", "dia",
              unique=True),
        Index("ix_almacen_consultas_dia", "store_id", "dia"),
    )

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    store_id: Mapped[str] = mapped_column(String(36), nullable=False)
    user_id: Mapped[str] = mapped_column(String(36), nullable=False)
    order_id: Mapped[str] = mapped_column(String(100), nullable=False)
    platform: Mapped[str | None] = mapped_column(String(40), nullable=True)
    dia: Mapped[date] = mapped_column(Date, nullable=False)
    consultado_en: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(timezone.utc))
