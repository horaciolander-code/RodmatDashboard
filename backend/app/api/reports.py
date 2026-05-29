import logging

from fastapi import APIRouter, BackgroundTasks, Depends, Header, HTTPException, status
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session

from app.config import INTERNAL_API_KEY
from app.database import get_db
from app.models.user import User
from app.models.report_log import ReportLog
from app.dependencies import get_current_user
from app.services.daily_report_service import build_report, run_store_report

logger = logging.getLogger("rodmat.reports")

router = APIRouter(prefix="/api/reports", tags=["reports"])


def _require_internal_key(x_api_key: str = Header(...)):
    if not INTERNAL_API_KEY or x_api_key != INTERNAL_API_KEY:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid API key")


@router.get("/preview", response_class=HTMLResponse)
def preview_report(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    html, _ = build_report(db, user.store_id)
    return HTMLResponse(content=html)


def _send_report_bg(store_id: str, force: bool = False):
    """Build and send daily report in background (own DB session).

    Honors the freshness gate unless `force=True`. The gate is the same
    one the scheduler uses, so the manual /send-now endpoint behaves
    consistently with the scheduled job.
    """
    import os
    from app.database import SessionLocal
    from app.models.store import Store
    from app.services.daily_report_service import build_report, send_report
    from app.services.freshness import check_data_freshness

    db = SessionLocal()
    try:
        store = db.query(Store).filter(Store.id == store_id).first()
        settings = store.settings or {} if store else {}
        recipients = settings.get("report_recipients") or ([os.getenv("SMTP_USER")] if os.getenv("SMTP_USER") else [])
        if not recipients:
            logger.warning("No recipients for store %s", store_id[:8])
            return

        if not force:
            f = check_data_freshness(db, store_id, store=store)
            if not f.is_fresh:
                log = ReportLog(store_id=store_id, status="skipped_stale")
                db.add(log); db.commit()
                logger.warning("send-now skipped (stale) for %s: %s",
                               store_id[:8], f.reason)
                return

        html, subject = build_report(db, store_id)
        store_name = store.name if store else "Store"
        ok = send_report(html, recipients, store_name, subject)
        if ok:
            log = ReportLog(store_id=store_id, recipients=", ".join(recipients), status="sent")
            db.add(log)
            db.commit()
            logger.info("Daily report sent for store %s -> %s", store_id[:8], recipients)
        else:
            log = ReportLog(store_id=store_id, status="failed")
            db.add(log); db.commit()
            logger.error("Daily report SMTP failed for store %s", store_id[:8])
    except Exception as exc:
        logger.exception("Daily report bg failed for store %s: %s", store_id[:8], exc)
    finally:
        db.close()


@router.post("/send-now")
def send_report_now(
    background_tasks: BackgroundTasks,
    force: bool = False,
    user: User = Depends(get_current_user),
):
    """Queue daily report in background — returns immediately to avoid Railway 60s timeout.

    By default the freshness gate is enforced (skips if no upload today).
    Pass ?force=true to bypass the gate and send with whatever data is in DB.
    """
    background_tasks.add_task(_send_report_bg, user.store_id, force)
    return {"status": "queued", "store": user.store_id[:8], "force": force}


def _run_all_bg():
    """Build and send all store reports in a background thread (own DB session).

    Goes through the same freshness-gated path as the in-process scheduler.
    """
    from app.database import SessionLocal
    from app.services.scheduled_jobs import run_scheduled_reports
    db = SessionLocal()
    try:
        results = run_scheduled_reports(db)
        logger.info("run-all reports completed: %s", results)
    except Exception as exc:
        logger.exception("run-all bg failed: %s", exc)
    finally:
        db.close()


@router.post("/run-all")
def run_all_stores_report(
    background_tasks: BackgroundTasks,
    _: None = Depends(_require_internal_key),
):
    """Cron endpoint — returns immediately so cron-job.org doesn't timeout; builds in background."""
    background_tasks.add_task(_run_all_bg)
    return {"status": "queued"}


@router.get("/history")
def report_history(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    logs = db.query(ReportLog).filter(
        ReportLog.store_id == user.store_id
    ).order_by(ReportLog.sent_at.desc()).limit(50).all()
    return [{"id": l.id, "sent_at": str(l.sent_at), "recipients": l.recipients, "status": l.status} for l in logs]
