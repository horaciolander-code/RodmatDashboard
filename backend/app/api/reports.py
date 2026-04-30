from fastapi import APIRouter, Depends
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.user import User
from app.models.report_log import ReportLog
from app.dependencies import get_current_user
from app.services.daily_report_service import build_report, run_store_report

router = APIRouter(prefix="/api/reports", tags=["reports"])


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
    success = run_store_report(db, user.store_id)
    if success:
        log = ReportLog(store_id=user.store_id, recipients="manual", status="sent")
        db.add(log)
        db.commit()
        return {"status": "sent"}
    return {"status": "failed", "detail": "Check SMTP config or store settings"}


@router.get("/history")
def report_history(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    logs = db.query(ReportLog).filter(
        ReportLog.store_id == user.store_id
    ).order_by(ReportLog.sent_at.desc()).limit(50).all()
    return [{"id": l.id, "sent_at": str(l.sent_at), "recipients": l.recipients, "status": l.status} for l in logs]
