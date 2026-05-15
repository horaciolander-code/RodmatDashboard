from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.sales import SalesOrder, AffiliateSale
from app.models.user import User
from app.dependencies import get_current_user

router = APIRouter(prefix="/api/sales", tags=["sales"])


@router.get("/orders")
def list_sales_orders(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    return db.query(SalesOrder).filter(SalesOrder.store_id == user.store_id).all()


@router.get("/affiliates")
def list_affiliate_sales(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    return db.query(AffiliateSale).filter(AffiliateSale.store_id == user.store_id).all()
