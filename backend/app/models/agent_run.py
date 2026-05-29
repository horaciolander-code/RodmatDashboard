import uuid
from datetime import datetime, timezone

from sqlalchemy import String, DateTime, Text, ForeignKey, Index
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class AgentRun(Base):
    """Audit trail of per-agent run attempts. Populated by the scheduler
    (in-process + cron + post-import trigger) so we can answer:
      - did agent X already run today for store Y?
      - what was the most recent skip reason?
    Used by scheduled_jobs.trigger_pending_jobs to gate re-fires."""

    __tablename__ = "agent_runs"
    __table_args__ = (
        Index("ix_agent_runs_store_run_at", "store_id", "run_at"),
    )

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    store_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("stores.id"), nullable=False
    )
    agent_name: Mapped[str] = mapped_column(String(30), nullable=False)
    run_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    # 'sent' | 'skipped_stale' | 'skipped_other' | 'skipped_not_its_day' | 'error'
    status: Mapped[str] = mapped_column(String(30), nullable=False)
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    latest_import_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
