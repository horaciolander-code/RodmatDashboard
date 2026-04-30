from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.store import Store
from app.models.user import User
from app.schemas.store import StoreUpdate, StoreResponse
from app.dependencies import get_current_user, require_admin

router = APIRouter(prefix="/api/stores", tags=["stores"])


@router.get("/me", response_model=StoreResponse)
def get_my_store(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    store = db.query(Store).filter(Store.id == user.store_id).first()
    if not store:
        raise HTTPException(status_code=404, detail="Store not found")
    return store


@router.put("/me", response_model=StoreResponse)
def update_my_store(
    payload: StoreUpdate,
    user: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    store = db.query(Store).filter(Store.id == user.store_id).first()
    if not store:
        raise HTTPException(status_code=404, detail="Store not found")
    for field, value in payload.model_dump(exclude_unset=True).items():
        setattr(store, field, value)
    db.commit()
    db.refresh(store)
    return store
