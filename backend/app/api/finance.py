import logging
from datetime import date
from typing import Optional

from fastapi import APIRouter, Body, Depends, File, HTTPException, UploadFile, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.dependencies import get_current_user, require_admin
from app.models.user import User
from app.schemas.finance import TransactionUpdate
from app.services import finance_service as svc

logger = logging.getLogger("rodmat.finance")
router = APIRouter(prefix="/api/finance", tags=["finance"])


@router.get("/transactions")
def list_transactions(
    skip: int = 0,
    limit: int = 500,
    tipo: str = "Todos",
    estado: str = "Todas",
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    return svc.get_transactions(db, user.store_id, skip, limit, tipo, estado, date_from, date_to)


@router.patch("/transactions/{tx_id}")
def update_transaction(
    tx_id: str,
    payload: TransactionUpdate,
    user: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    result = svc.update_transaction(db, user.store_id, tx_id, payload.tipo, payload.clasificacion, payload.comentarios)
    if result is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Transaction not found")
    return result


@router.delete("/transactions/{tx_id}")
def delete_transaction(
    tx_id: str,
    user: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    if not svc.delete_transaction(db, user.store_id, tx_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Transaction not found")
    return {"status": "deleted"}


@router.post("/preview")
async def preview_import(
    file: UploadFile = File(...),
    user: User = Depends(require_admin),
):
    content = await file.read()
    result = svc.preview_file(content, file.filename or "upload.xlsx")
    return result


@router.post("/import")
def import_transactions(
    rows: list[dict] = Body(...),
    user: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    return svc.import_rows(db, user.store_id, rows)


@router.post("/reclassify-pending")
def reclassify_pending(
    user: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    return svc.reclassify_pending(db, user.store_id)


@router.post("/fix-dates")
def fix_inverted_dates(
    user: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    return svc.fix_inverted_dates(db, user.store_id)


@router.get("/dashboard")
def finance_dashboard(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    return svc.get_dashboard(db, user.store_id)


@router.get("/insights")
def finance_insights(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    return svc.get_insights(db, user.store_id)


@router.get("/classifications")
def get_classifications(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    return svc.get_classifications(db, user.store_id)


@router.put("/classifications")
def update_classifications(
    data: dict = Body(...),
    user: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    return svc.save_classifications(db, user.store_id, data)


@router.get("/pending-count")
def pending_count(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    return {"count": svc.get_pending_count(db, user.store_id)}
