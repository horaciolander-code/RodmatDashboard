import logging

from fastapi import APIRouter, Depends, Header, HTTPException, status
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session

from app.config import INTERNAL_API_KEY
from app.database import get_db
from app.models.user import User
from app.models.report_log import ReportLog
from app.dependencies import get_current_user
from app.services.daily_report_service import build_report, run_store_report, run_all_reports

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
    html = build_report(db, user.store_id)
    return HTMLResponse(content=html)


@router.post("/send-now")
def send_report_now(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Manual trigger — bypasses report_enabled flag, falls back to SMTP_USER as recipient."""
    from app.models.store import Store
    from app.services.daily_report_service import build_report, send_report
    import os

    store = db.query(Store).filter(Store.id == user.store_id).first()
    settings = store.settings or {} if store else {}
    recipients = settings.get("report_recipients") or ([os.getenv("SMTP_USER")] if os.getenv("SMTP_USER") else [])
    if not recipients:
        return {"status": "failed", "detail": "No recipients configured"}

    html = build_report(db, user.store_id)
    store_name = store.name if store else "Store"
    success = send_report(html, recipients, store_name)
    if success:
        log = ReportLog(store_id=user.store_id, recipients=", ".join(recipients), status="sent")
        db.add(log)
        db.commit()
        return {"status": "sent", "recipients": recipients}
    return {"status": "failed", "detail": "SMTP error — check Railway SMTP_USER/SMTP_PASSWORD"}


@router.post("/run-all")
def run_all_stores_report(
    db: Session = Depends(get_db),
    _: None = Depends(_require_internal_key),
):
    """Cron endpoint — called daily by cron-job.org. Sends report to every store."""
    results = run_all_reports(db)
    logger.info("run-all reports completed: %s", results)
    return {"status": "done", "results": results}


@router.get("/history")
def report_history(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    logs = db.query(ReportLog).filter(
        ReportLog.store_id == user.store_id
    ).order_by(ReportLog.sent_at.desc()).limit(50).all()
    return [{"id": l.id, "sent_at": str(l.sent_at), "recipients": l.recipients, "status": l.status} for l in logs]
