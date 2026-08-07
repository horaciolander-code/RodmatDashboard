"""Brands endpoints — multi-brand support dentro de un store."""
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import User, Brand
from app.dependencies import get_current_user

router = APIRouter(prefix="/api/brands", tags=["brands"])


@router.get("")
def list_brands(user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    """Lista brands activas del store del user."""
    rows = db.query(Brand).filter(
        Brand.store_id == user.store_id,
        Brand.is_active == True,
    ).order_by(Brand.slug).all()
    return [{
        "id": b.id,
        "slug": b.slug,
        "display_name": b.display_name,
        "brand_color": b.brand_color,
        "absorbs_shared_costs": b.absorbs_shared_costs,
    } for b in rows]


@router.get("/product-map")
def get_product_brand_map(user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    """Devuelve mapa SKU → brand_slug para el store. Usado por el dashboard
    Streamlit para filtrar client-side sin tocar todos los endpoints existentes."""
    from app.models import Product
    q = db.query(Product.sku, Brand.slug).outerjoin(
        Brand, Brand.id == Product.brand_id
    ).filter(Product.store_id == user.store_id)
    return {sku: (slug or None) for sku, slug in q.all()}
