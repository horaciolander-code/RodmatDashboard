import uuid
from datetime import datetime, timezone

from sqlalchemy import String, DateTime, Integer, ForeignKey, Index
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class ImportHistory(Base):
    __tablename__ = "import_history"
    __table_args__ = (
        Index("ix_import_history_store_date", "store_id", "imported_at"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    store_id: Mapped[str] = mapped_column(String(36), ForeignKey("stores.id"), nullable=False)
    import_type: Mapped[str] = mapped_column(String(30), nullable=False)
    filename: Mapped[str | None] = mapped_column(String(255), nullable=True)
    rows_imported: Mapped[int] = mapped_column(Integer, default=0)
    rows_deleted: Mapped[int] = mapped_column(Integer, default=0)
    imported_by: Mapped[str | None] = mapped_column(String(100), nullable=True)
    imported_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
    )
