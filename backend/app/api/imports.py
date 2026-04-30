from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File
from sqlalchemy.orm import Session

from app.config import MAX_UPLOAD_SIZE
from app.database import get_db
from app.models.user import User
from app.dependencies import get_current_user
from app.schemas.import_schemas import ImportResult
from app.services.import_service import (
    parse_orders_csv,
    parse_affiliate_csv,
    parse_products_excel,
    parse_combos_excel,
    parse_initial_inventory_excel,
    parse_pending_inventory_excel,
)

router = APIRouter(prefix="/api/import", tags=["import"])


def _validate_upload(file: UploadFile, content: bytes, allowed_ext: str):
    if len(content) > MAX_UPLOAD_SIZE:
        raise HTTPException(status_code=413, detail=f"File too large. Maximum size is {MAX_UPLOAD_SIZE // (1024*1024)}MB")
    filename = (file.filename or "").lower()
    if not filename.endswith(allowed_ext):
        raise HTTPException(status_code=415, detail=f"Invalid file type. Expected {allowed_ext}")


def _target_store(user: User, store_id: str | None) -> str:
    """Superadmin puede pasar store_id explícito; cualquier otro usa su propia tienda."""
    if user.role == "superadmin" and store_id:
        return store_id
    return user.store_id


@router.post("/orders", response_model=ImportResult)
async def import_orders(
    file: UploadFile = File(...),
    store_id: str | None = Query(None),
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    content = await file.read()
    _validate_upload(file, content, ".csv")
    return parse_orders_csv(content, _target_store(user, store_id), db)


@router.post("/affiliates", response_model=ImportResult)
async def import_affiliates(
    file: UploadFile = File(...),
    store_id: str | None = Query(None),
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    content = await file.read()
    _validate_upload(file, content, ".csv")
    return parse_affiliate_csv(content, _target_store(user, store_id), db)


@router.post("/products", response_model=ImportResult)
async def import_products(
    file: UploadFile = File(...),
    store_id: str | None = Query(None),
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    content = await file.read()
    _validate_upload(file, content, ".xlsx")
    return parse_products_excel(content, _target_store(user, store_id), db)


@router.post("/combos", response_model=ImportResult)
async def import_combos(
    file: UploadFile = File(...),
    store_id: str | None = Query(None),
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    content = await file.read()
    _validate_upload(file, content, ".xlsx")
    return parse_combos_excel(content, _target_store(user, store_id), db)


@router.post("/initial-inventory", response_model=ImportResult)
async def import_initial_inventory(
    file: UploadFile = File(...),
    store_id: str | None = Query(None),
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    content = await file.read()
    _validate_upload(file, content, ".xlsx")
    return parse_initial_inventory_excel(content, _target_store(user, store_id), db)


@router.post("/incoming-stock", response_model=ImportResult)
async def import_incoming_stock(
    file: UploadFile = File(...),
    store_id: str | None = Query(None),
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    content = await file.read()
    _validate_upload(file, content, ".xlsx")
    return parse_pending_inventory_excel(content, _target_store(user, store_id), db)
