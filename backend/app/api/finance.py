"""
Finance API — P&L estructurado + líneas custom (calculadora).
Sustituye el módulo legacy de bank_transactions.

Todas las rutas requieren user autenticado + módulo `finance` habilitado para el store.
"""
import logging
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.user import User
from app.dependencies import get_current_user, require_finance_enabled
from app.schemas.finance import (
    PLResponse, CustomLineOut, CustomLinesReplaceRequest,
)
from app.services import finance_service as svc

logger = logging.getLogger("rodmat.finance")

router = APIRouter(prefix="/api/finance", tags=["finance"])


def _target_store(user: User, store_id: str | None) -> str:
    if user.role == "superadmin" and store_id:
        return store_id
    return user.store_id


@router.get("/pl", response_model=PLResponse)
def get_pl(
    year:     int  = Query(...),
    period:   str  = Query(..., description="'YTD' or 'MM' (01..12)"),
    store_id: str  = Query(None),
    user:  User    = Depends(get_current_user),
    _:        None = Depends(require_finance_enabled),
    db:    Session = Depends(get_db),
):
    """Devuelve P&L estructurado para el período seleccionado (mes o YTD)."""
    try:
        target = _target_store(user, store_id)
        return svc.compute_pl(db, target, year, period)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/custom-lines")
def get_custom_lines(
    year:     int  = Query(...),
    period:   str  = Query(...),
    store_id: str  = Query(None),
    user:  User    = Depends(get_current_user),
    _:        None = Depends(require_finance_enabled),
    db:    Session = Depends(get_db),
):
    """Lista las líneas custom del período."""
    try:
        target = _target_store(user, store_id)
        rows = svc.list_custom_lines(db, target, year, period)
        return [
            {"id": r.id, "year_month": r.year_month, "description": r.description,
             "amount": float(r.amount), "sort_order": float(r.sort_order)}
            for r in rows
        ]
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.put("/custom-lines")
def replace_custom_lines(
    body:     CustomLinesReplaceRequest,
    year:     int  = Query(...),
    period:   str  = Query(..., description="'MM' — no YTD"),
    store_id: str  = Query(None),
    user:  User    = Depends(get_current_user),
    _:        None = Depends(require_finance_enabled),
    db:    Session = Depends(get_db),
):
    """Reemplaza atómicamente todas las líneas del período (mes)."""
    try:
        target = _target_store(user, store_id)
        new_rows = svc.replace_custom_lines(db, target, year, period, body.lines)
        logger.info("Replaced %d custom lines for store=%s year=%d period=%s",
                    len(new_rows), target[:8], year, period)
        return {"replaced": len(new_rows)}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/custom-lines/copy-from-previous")
def copy_from_previous_month(
    year:     int  = Query(...),
    period:   str  = Query(...),
    store_id: str  = Query(None),
    user:  User    = Depends(get_current_user),
    _:        None = Depends(require_finance_enabled),
    db:    Session = Depends(get_db),
):
    """Copia las líneas del mes anterior al mes actual (si el actual está vacío)."""
    try:
        target = _target_store(user, store_id)
        new_rows = svc.copy_from_previous_month(db, target, year, period)
        return {"copied": len(new_rows)}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
