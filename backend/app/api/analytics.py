from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from typing import Optional

from app.database import get_db
from app.models.user import User
from app.dependencies import get_current_user
from app.services import analytics_service as svc

router = APIRouter(prefix="/api/analytics", tags=["analytics"])


@router.get("/overview")
def overview(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    return svc.get_overview_metrics(db, user.store_id)


@router.get("/sales-by-month")
def sales_by_month(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    return svc.get_sales_by_month(db, user.store_id, date_from, date_to)


@router.get("/sales-by-day")
def sales_by_day(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    return svc.get_sales_by_day(db, user.store_id, date_from, date_to)


@router.get("/stock-summary")
def stock_summary(
    coverage_days: int = 30,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    return svc.get_stock_summary(db, user.store_id, coverage_days)


@router.get("/stock-detail")
def stock_detail(
    coverage_days: int = 30,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    return svc.get_stock_detail(db, user.store_id, coverage_days)


@router.get("/reorder-list")
def reorder_list(
    coverage_days: int = 30,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    return svc.get_reorder_list(db, user.store_id, coverage_days)


@router.get("/creators/top")
def top_creators(
    n: int = 20,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    return svc.get_top_creators(db, user.store_id, n)


@router.get("/creators/by-type")
def creators_by_type(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    return svc.get_creator_by_type(db, user.store_id)


@router.get("/creators/by-month")
def creators_by_month(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    return svc.get_creator_by_month(db, user.store_id)


@router.get("/orders")
def filtered_orders(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[str] = None,
    sku: Optional[str] = None,
    buyer: Optional[str] = None,
    fulfillment: Optional[str] = None,
    order_id: Optional[str] = None,
    product_name: Optional[str] = None,
    limit: int = Query(500, le=5000),
    offset: int = 0,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    return svc.get_filtered_orders(
        db, user.store_id, date_from, date_to, status, sku,
        buyer, fulfillment, order_id, product_name, limit, offset
    )


@router.get("/frequent-buyers")
def frequent_buyers(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    return svc.get_frequent_buyers(db, user.store_id)


@router.get("/top-combos")
def top_combos(
    n: int = 15,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    return svc.get_top_combos(db, user.store_id, n)


@router.get("/finances")
def finances(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    return svc.get_finances(db, user.store_id)


@router.get("/unknown-combos")
def unknown_combos(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    return svc.get_unknown_combos(db, user.store_id)


@router.post("/clear-cache")
def clear_cache(user: User = Depends(get_current_user)):
    """Force-clear the backend analytics cache for this store."""
    keys_removed = [k for k in list(svc._cache.keys()) if k[0] == user.store_id]
    for k in keys_removed:
        del svc._cache[k]
    return {"cleared": len(keys_removed)}
