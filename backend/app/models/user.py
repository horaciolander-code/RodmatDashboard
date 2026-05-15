import uuid
from datetime import datetime, timezone

from sqlalchemy import String, DateTime, ForeignKey, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


class User(Base):
    __tablename__ = "users"
    __table_args__ = (
        Index("ix_users_store_id", "store_id"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    email: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    hashed_password: Mapped[str] = mapped_column(String(255), nullable=False)
    store_id: Mapped[str] = mapped_column(String(36), ForeignKey("stores.id"), nullable=False)
    role: Mapped[str] = mapped_column(String(20), default="viewer")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))

    store = relationship("Store", back_populates="users")
