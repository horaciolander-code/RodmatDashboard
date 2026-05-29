"""
Data-freshness gate.

Single rule, by design: a store is "fresh" today if it has at least one
`import_history` row whose `imported_at` falls on TODAY in the store's
local timezone. The scheduled jobs (daily report + agents) use this gate
to decide whether to fire or skip+alert. When an upload eventually lands,
the imports endpoint calls `trigger_pending_jobs` (in scheduled_jobs.py)
which uses this same gate to decide whether to fire what was previously
skipped.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Sequence

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover — Python <3.9 not supported on Railway
    from backports.zoneinfo import ZoneInfo  # type: ignore

from sqlalchemy.orm import Session

from app.models.import_history import ImportHistory


DEFAULT_TYPES: tuple[str, ...] = ("tiktok", "amazon")


@dataclass(frozen=True)
class FreshnessResult:
    is_fresh: bool
    latest_import_at: Optional[datetime]
    latest_import_type: Optional[str]
    reason: str   # short machine label: 'fresh' | 'stale' | 'no_imports_ever'
    detail: str   # human-readable detail (safe to put in an email body)

    def __bool__(self) -> bool:
        return self.is_fresh


def _store_tz(store) -> ZoneInfo:
    """Resolve the store's timezone; fallback to America/New_York (EDT/EST)."""
    if store and getattr(store, "timezone", None):
        try:
            return ZoneInfo(store.timezone)
        except Exception:
            pass
    return ZoneInfo("America/New_York")


def check_data_freshness(
    db: Session,
    store_id: str,
    *,
    store=None,
    import_types: Sequence[str] = DEFAULT_TYPES,
    now: Optional[datetime] = None,
) -> FreshnessResult:
    """Return a FreshnessResult for this store.

    Fresh <=> at least one import_history row exists with imported_at on
    "today" in the store's local timezone.
    """
    if now is None:
        now = datetime.now(timezone.utc)
    elif now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)

    if store is None:
        from app.models.store import Store
        store = db.query(Store).filter(Store.id == store_id).first()

    tz = _store_tz(store)
    local_today = now.astimezone(tz).date()
    utc_day_start = datetime.combine(
        local_today, datetime.min.time(), tzinfo=tz
    ).astimezone(timezone.utc)

    q = (db.query(ImportHistory)
         .filter(ImportHistory.store_id == store_id)
         .filter(ImportHistory.imported_at >= utc_day_start)
         .order_by(ImportHistory.imported_at.desc()))
    if import_types:
        q = q.filter(ImportHistory.import_type.in_(list(import_types)))
    latest_today = q.first()

    if latest_today is not None:
        return FreshnessResult(
            is_fresh=True,
            latest_import_at=latest_today.imported_at,
            latest_import_type=latest_today.import_type,
            reason="fresh",
            detail=(
                f"Import '{latest_today.import_type}' at "
                f"{latest_today.imported_at.isoformat()} "
                f"(today {local_today.isoformat()} in {tz.key})."
            ),
        )

    # Not fresh -- surface the most recent import overall for the alert.
    q_any = (db.query(ImportHistory)
             .filter(ImportHistory.store_id == store_id)
             .order_by(ImportHistory.imported_at.desc()))
    if import_types:
        q_any = q_any.filter(ImportHistory.import_type.in_(list(import_types)))
    last_overall = q_any.first()

    if last_overall is None:
        return FreshnessResult(
            is_fresh=False,
            latest_import_at=None,
            latest_import_type=None,
            reason="no_imports_ever",
            detail=(f"No imports ever recorded for store "
                    f"(types={list(import_types)})."),
        )

    age_h = (now - last_overall.imported_at).total_seconds() / 3600.0
    return FreshnessResult(
        is_fresh=False,
        latest_import_at=last_overall.imported_at,
        latest_import_type=last_overall.import_type,
        reason="stale",
        detail=(
            f"Latest import is '{last_overall.import_type}' at "
            f"{last_overall.imported_at.isoformat()} ({age_h:.1f}h ago). "
            f"Today in {tz.key} is {local_today.isoformat()}, "
            f"no import yet."
        ),
    )
