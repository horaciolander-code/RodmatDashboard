"""
Scheduler / cron / post-import orchestration with freshness gates.

This module wraps the report and agent business logic that lives in
  - app.services.daily_report_service
  - app.services.agents.<name>_agent
and decides *when* / *whether* to fire them, by consulting
`check_data_freshness` and the run-audit tables (`report_logs`, `agent_runs`).

Three entry points:
  - run_scheduled_reports(db)  - called by main.py scheduler at 12:00 UTC
                                  and by POST /api/reports/run-all
  - run_scheduled_agents(db)   - called by main.py scheduler at 11:00 UTC
                                  and by POST /api/agents/run-all
  - trigger_pending_jobs(sid)  - called as a background task by
                                  POST /api/import/orders and /amazon
                                  after a successful import, to fire
                                  whatever was skipped earlier today.

All three log structured run records and (where applicable) send a single
consolidated freshness alert per store to OPERATIONS_EMAIL.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy.orm import Session

from app.models.store import Store
from app.models.report_log import ReportLog
from app.models.agent_run import AgentRun
from app.services.freshness import check_data_freshness
from app.services.alert_service import send_freshness_alert

logger = logging.getLogger("rodmat.scheduled")


AGENT_MODULES = {
    "prism":     "app.services.agents.prism_agent",
    "haiku":     "app.services.agents.haiku_agent",
    "faraway":   "app.services.agents.faraway_agent",
    "mesmerize": "app.services.agents.mesmerize_agent",
}


# ── audit helpers ────────────────────────────────────────────────────────────

def _today_start_utc() -> datetime:
    return datetime.combine(
        datetime.now(timezone.utc).date(),
        datetime.min.time(),
        tzinfo=timezone.utc,
    )


def _today_has_sent_report(db: Session, store_id: str) -> bool:
    start_naive = _today_start_utc().replace(tzinfo=None)  # ReportLog.sent_at is naive
    return db.query(ReportLog).filter(
        ReportLog.store_id == store_id,
        ReportLog.status == "sent",
        ReportLog.sent_at >= start_naive,
    ).first() is not None


def _today_has_agent_run(db: Session, store_id: str, agent_name: str,
                         status: str = "sent") -> bool:
    return db.query(AgentRun).filter(
        AgentRun.store_id == store_id,
        AgentRun.agent_name == agent_name,
        AgentRun.status == status,
        AgentRun.run_at >= _today_start_utc(),
    ).first() is not None


def _log_report(db: Session, store_id: str, status: str,
                recipients: str | None = None) -> None:
    log = ReportLog(store_id=store_id, status=status, recipients=recipients)
    db.add(log)
    db.commit()


def _log_agent_run(db: Session, store_id: str, agent_name: str, status: str,
                   reason: str | None = None,
                   latest_import_at: Optional[datetime] = None) -> None:
    rec = AgentRun(store_id=store_id, agent_name=agent_name, status=status,
                   reason=reason, latest_import_at=latest_import_at)
    db.add(rec)
    db.commit()


def _is_agent_day(agent_name: str, today: Optional[datetime] = None) -> bool:
    """Mirror of each agent's internal day check, used to decide whether a
    skip should be recorded as 'skipped_stale' (= it WAS its day) vs
    'skipped_not_its_day' (= not due today)."""
    if today is None:
        today = datetime.now()
    wd = today.weekday()
    if agent_name == "prism":     return wd == 0                      # Mon
    if agent_name == "haiku":     return wd == 2                      # Wed
    if agent_name == "faraway":   return wd == 4                      # Fri
    if agent_name == "mesmerize": return wd == 0 and today.day <= 7   # 1st Mon
    return False


# ── scheduled report (12:00 UTC) ─────────────────────────────────────────────

def run_scheduled_reports(db: Session) -> dict:
    """Run daily reports for every report-enabled store, gated by freshness.

    Behavior per store:
      - if not report_enabled            -> skipped_disabled
      - elif not fresh                   -> log skipped_stale + queue alert
      - else                             -> send report + log sent

    Returns dict mapping store_name -> status string.
    Sends one consolidated stale-data alert per store at the end.
    """
    from app.services.daily_report_service import run_store_report

    stores = db.query(Store).all()
    results: dict[str, str] = {}
    skipped_by_store: dict[str, list[dict]] = {}
    latest_by_store: dict[str, Optional[datetime]] = {}

    for store in stores:
        settings = store.settings or {}
        if not settings.get("report_enabled", False):
            results[store.name] = "skipped_disabled"
            continue

        f = check_data_freshness(db, store.id, store=store)
        if not f.is_fresh:
            _log_report(db, store.id, status="skipped_stale")
            results[store.name] = "skipped_stale"
            skipped_by_store.setdefault(store.name, []).append({
                "kind": "daily_report",
                "name": "Daily Report",
                "reason": f.detail,
            })
            latest_by_store[store.name] = f.latest_import_at
            logger.info("[scheduled_reports] %s skipped (stale): %s",
                        store.name, f.reason)
            continue

        try:
            ok = run_store_report(db, store.id)
        except Exception as exc:
            logger.exception("[scheduled_reports] %s failed: %s", store.name, exc)
            _log_report(db, store.id, status="failed")
            results[store.name] = "failed"
            continue

        if ok:
            recips = ", ".join(settings.get("report_recipients") or [])
            _log_report(db, store.id, status="sent", recipients=recips)
            results[store.name] = "sent"
        else:
            _log_report(db, store.id, status="failed")
            results[store.name] = "failed"

    for store_name, items in skipped_by_store.items():
        send_freshness_alert(
            store_name=store_name,
            skipped=items,
            latest_import_at=latest_by_store.get(store_name),
        )
    return results


# ── scheduled agents (11:00 UTC) ─────────────────────────────────────────────

def run_scheduled_agents(db: Session) -> dict:
    """Run all four agents for every store, gated by freshness.

    Per (store, agent):
      - if stale AND today is the agent's day -> log skipped_stale + alert
      - if stale AND today is NOT its day     -> log skipped_not_its_day (no alert)
      - if fresh                              -> call agent.run(), log result

    Sends one consolidated stale-data alert per store at the end.
    """
    import importlib

    stores = db.query(Store).all()
    results: dict[str, dict] = {}
    skipped_by_store: dict[str, list[dict]] = {}
    latest_by_store: dict[str, Optional[datetime]] = {}

    for store in stores:
        store_results: dict[str, str] = {}
        f = check_data_freshness(db, store.id, store=store)

        for agent_name, mod_path in AGENT_MODULES.items():
            try:
                if not f.is_fresh:
                    if _is_agent_day(agent_name):
                        _log_agent_run(
                            db, store.id, agent_name,
                            status="skipped_stale",
                            reason=f.detail,
                            latest_import_at=f.latest_import_at,
                        )
                        skipped_by_store.setdefault(store.name, []).append({
                            "kind": "agent",
                            "name": agent_name.upper(),
                            "reason": f.detail,
                        })
                        latest_by_store[store.name] = f.latest_import_at
                        store_results[agent_name] = "skipped_stale"
                    else:
                        # don't pollute the audit table with daily noise
                        store_results[agent_name] = "skipped_not_its_day"
                    continue

                agent = importlib.import_module(mod_path)
                ran = agent.run(db, store.id)
                if ran:
                    _log_agent_run(db, store.id, agent_name, status="sent",
                                   latest_import_at=f.latest_import_at)
                    store_results[agent_name] = "sent"
                else:
                    # agent.run() returned False -- either not its day, disabled,
                    # or no recipients. Don't audit every daily no-op.
                    if _is_agent_day(agent_name):
                        _log_agent_run(
                            db, store.id, agent_name,
                            status="skipped_other",
                            reason=("agent.run() returned False despite fresh data "
                                    "(disabled / no recipients / agent-internal gate)"),
                            latest_import_at=f.latest_import_at,
                        )
                    store_results[agent_name] = "skipped_other"
            except Exception as exc:
                logger.exception("[scheduled_agents] %s %s failed: %s",
                                 store.name, agent_name, exc)
                _log_agent_run(db, store.id, agent_name, status="error",
                               reason=str(exc)[:500],
                               latest_import_at=f.latest_import_at)
                store_results[agent_name] = f"error: {type(exc).__name__}"

        results[store.id] = store_results

    for store_name, items in skipped_by_store.items():
        send_freshness_alert(
            store_name=store_name,
            skipped=items,
            latest_import_at=latest_by_store.get(store_name),
        )
    return results


# ── post-import trigger ──────────────────────────────────────────────────────

def trigger_pending_jobs(store_id: str) -> dict:
    """Fire any daily report / agent run that is due today for this store
    and hasn't been sent yet. Called as a FastAPI background task after a
    successful import (tiktok / amazon).

    Uses its own DB session because background tasks don't share the request
    session, and FastAPI's session may already be closed by the time this
    runs.
    """
    import importlib
    from app.database import SessionLocal
    from app.services.daily_report_service import run_store_report

    out: dict[str, str] = {}
    db = SessionLocal()
    try:
        store = db.query(Store).filter(Store.id == store_id).first()
        if not store:
            return {"error": "store_not_found"}

        settings = store.settings or {}
        f = check_data_freshness(db, store_id, store=store)

        # 1) Daily report
        if not settings.get("report_enabled", False):
            out["daily_report"] = "disabled"
        elif _today_has_sent_report(db, store_id):
            out["daily_report"] = "already_sent"
        elif not f.is_fresh:
            # Edge case: import landed but freshness check still false (race
            # or filtered-out import_type). Don't fire; don't log noise either.
            out["daily_report"] = "still_stale"
        else:
            try:
                ok = run_store_report(db, store_id)
                if ok:
                    recips = ", ".join(settings.get("report_recipients") or [])
                    _log_report(db, store_id, status="sent", recipients=recips)
                    out["daily_report"] = "sent"
                else:
                    _log_report(db, store_id, status="failed")
                    out["daily_report"] = "failed"
            except Exception as exc:
                logger.exception("[trigger] daily report %s failed: %s",
                                 store_id[:8], exc)
                _log_report(db, store_id, status="failed")
                out["daily_report"] = f"error: {type(exc).__name__}"

        # 2) Agents — only if today is the agent's day AND not already sent
        for agent_name, mod_path in AGENT_MODULES.items():
            if not _is_agent_day(agent_name):
                out[agent_name] = "not_its_day"
                continue
            if _today_has_agent_run(db, store_id, agent_name, status="sent"):
                out[agent_name] = "already_sent"
                continue
            if not f.is_fresh:
                out[agent_name] = "still_stale"
                continue
            try:
                agent = importlib.import_module(mod_path)
                ran = agent.run(db, store_id)
                if ran:
                    _log_agent_run(db, store_id, agent_name, status="sent",
                                   latest_import_at=f.latest_import_at)
                    out[agent_name] = "sent"
                else:
                    _log_agent_run(
                        db, store_id, agent_name,
                        status="skipped_other",
                        reason="agent.run() returned False (disabled or internal gate)",
                        latest_import_at=f.latest_import_at,
                    )
                    out[agent_name] = "skipped_other"
            except Exception as exc:
                logger.exception("[trigger] agent %s %s failed: %s",
                                 store_id[:8], agent_name, exc)
                _log_agent_run(db, store_id, agent_name, status="error",
                               reason=str(exc)[:500],
                               latest_import_at=f.latest_import_at)
                out[agent_name] = f"error: {type(exc).__name__}"
    finally:
        db.close()
    logger.info("[trigger] store=%s result=%s", store_id[:8], out)
    return out
