from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.inventory import InitialInventory, IncomingStock
from app.models.user import User
from app.schemas.inventory import (
    InitialInventoryCreate, InitialInventoryResponse,
    IncomingStockCreate, IncomingStockResponse,
    IncomingStockUpdate,
)
from app.dependencies import get_current_user

router = APIRouter(prefix="/api/inventory", tags=["inventory"])


@router.post("/initial", response_model=InitialInventoryResponse, status_code=201)
def create_initial_inventory(
    payload: InitialInventoryCreate,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    record = InitialInventory(store_id=user.store_id, **payload.model_dump())
    db.add(record)
    db.commit()
    db.refresh(record)
    return record


@router.get("/initial", response_model=list[InitialInventoryResponse])
def list_initial_inventory(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    return db.query(InitialInventory).filter(InitialInventory.store_id == user.store_id).all()


@router.post("/incoming", response_model=IncomingStockResponse, status_code=201)
def create_incoming_stock(
    payload: IncomingStockCreate,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    record = IncomingStock(store_id=user.store_id, **payload.model_dump())
    db.add(record)
    db.commit()
    db.refresh(record)
    return record


@router.get("/incoming", response_model=list[IncomingStockResponse])
def list_incoming_stock(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    return db.query(IncomingStock).filter(IncomingStock.store_id == user.store_id).all()


@router.put("/incoming/{record_id}", response_model=IncomingStockResponse)
def update_incoming_stock(
    record_id: str,
    payload: IncomingStockUpdate,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    record = db.query(IncomingStock).filter(
        IncomingStock.id == record_id, IncomingStock.store_id == user.store_id
    ).first()
    if not record:
        raise HTTPException(status_code=404, detail="Incoming stock record not found")
    for field, value in payload.model_dump(exclude_unset=True).items():
        setattr(record, field, value)
    db.commit()
    db.refresh(record)
    return record


@router.delete("/incoming/{record_id}", status_code=204)
def delete_incoming_stock(
    record_id: str,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    record = db.query(IncomingStock).filter(
        IncomingStock.id == record_id, IncomingStock.store_id == user.store_id
    ).first()
    if not record:
        raise HTTPException(status_code=404, detail="Incoming stock record not found")
    db.delete(record)
    db.commit()
