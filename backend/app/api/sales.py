from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from typing import Optional

from app.database import get_db
from app.models.sales import SalesOrder, AffiliateSale
from app.models.user import User
from app.dependencies import get_current_user, get_user_brand_id

router = APIRouter(prefix="/api/sales", tags=["sales"])

# 2026-09-20 FIX FUGA: estos dos endpoints hacían .all() sobre sales_orders (26.789 filas)
# y affiliate_sales (4.307) sin filtro de marca ni paginación. Cualquier usuario
# autenticado, incluido uno brand-scoped, se llevaba la tabla entera de la tienda.
# Ahora: scoping obligatorio por marca (fail-closed) + límite duro.


@router.get("/orders")
def list_sales_orders(
    brand_slug: Optional[str] = None,
    limit: int = Query(500, le=5000),
    offset: int = Query(0, ge=0),
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    q = db.query(SalesOrder).filter(SalesOrder.store_id == user.store_id)
    bid = get_user_brand_id(user, db, brand_slug)
    if bid:
        q = q.filter(SalesOrder.brand_id == bid)
    total = q.count()
    rows = q.order_by(SalesOrder.order_date.desc()).offset(offset).limit(limit).all()
    return {"total": total, "limit": limit, "offset": offset, "rows": rows}


@router.get("/affiliates")
def list_affiliate_sales(
    brand_slug: Optional[str] = None,
    limit: int = Query(500, le=5000),
    offset: int = Query(0, ge=0),
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    q = db.query(AffiliateSale).filter(AffiliateSale.store_id == user.store_id)
    bid = get_user_brand_id(user, db, brand_slug)
    if bid:
        q = q.filter(AffiliateSale.brand_id == bid)
    total = q.count()
    rows = q.order_by(AffiliateSale.time_created.desc()).offset(offset).limit(limit).all()
    return {"total": total, "limit": limit, "offset": offset, "rows": rows}
