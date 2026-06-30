"""SKU Maps router — unified CRUD for walmart_sku_map and amazon_sku_map.

Endpoints:
- GET  /api/sku-maps?platform=walmart|amazon|all   → list mappings (unified shape)
- POST /api/sku-maps                                → create mapping
- PUT  /api/sku-maps/{map_id}?platform=...          → update mapping
- DELETE /api/sku-maps/{map_id}?platform=...        → delete mapping

Unified response shape: {id, platform, external_sku, product_id, product_name, units_per_sale}
"""
from typing import Literal
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
from sqlalchemy import text as _text

from app.database import get_db
from app.models.user import User
from app.models.product import Product
from app.models.walmart_sku_map import WalmartSkuMap
from app.models.amazon_sku_map import AmazonSkuMap
from app.dependencies import get_current_user

router = APIRouter(prefix="/api/sku-maps", tags=["sku-maps"])

Platform = Literal["walmart", "amazon"]


class SkuMapCreate(BaseModel):
    platform: Platform
    external_sku: str = Field(min_length=1, max_length=100)
    product_id: str | None = None
    units_per_sale: int = Field(default=1, ge=1)


class SkuMapUpdate(BaseModel):
    external_sku: str | None = Field(default=None, min_length=1, max_length=100)
    product_id: str | None = None
    units_per_sale: int | None = Field(default=None, ge=1)


class SkuMapResponse(BaseModel):
    id: str
    platform: Platform
    external_sku: str
    product_id: str | None = None
    product_name: str | None = None
    units_per_sale: int


def _row_to_response(row, platform: Platform, pmap: dict) -> SkuMapResponse:
    external = row.walmart_sku if platform == "walmart" else row.amazon_sku
    return SkuMapResponse(
        id=row.id, platform=platform,
        external_sku=external, product_id=row.product_id,
        product_name=pmap.get(row.product_id) if row.product_id else None,
        units_per_sale=row.units_per_sale,
    )


def _validate_product(db: Session, product_id: str | None, store_id: str):
    if not product_id:
        return
    p = db.query(Product).filter(Product.id == product_id).first()
    if not p:
        raise HTTPException(status_code=404, detail=f"Product {product_id} not found")
    if p.store_id != store_id:
        raise HTTPException(status_code=403, detail="Cannot use product from another store")


@router.get("", response_model=list[SkuMapResponse])
def list_sku_maps(
    platform: Literal["walmart", "amazon", "all"] = Query("all"),
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    result: list[SkuMapResponse] = []
    product_ids: set[str] = set()
    walmart_rows: list = []
    amazon_rows: list = []

    if platform in ("walmart", "all"):
        walmart_rows = db.query(WalmartSkuMap).filter(WalmartSkuMap.store_id == user.store_id).all()
        product_ids.update(r.product_id for r in walmart_rows if r.product_id)
    if platform in ("amazon", "all"):
        amazon_rows = db.query(AmazonSkuMap).filter(AmazonSkuMap.store_id == user.store_id).all()
        product_ids.update(r.product_id for r in amazon_rows if r.product_id)

    pmap: dict = {}
    if product_ids:
        pmap = {p.id: p.name for p in db.query(Product).filter(Product.id.in_(list(product_ids))).all()}

    for r in walmart_rows:
        result.append(_row_to_response(r, "walmart", pmap))
    for r in amazon_rows:
        result.append(_row_to_response(r, "amazon", pmap))
    result.sort(key=lambda x: (x.platform, x.external_sku.lower()))
    return result


@router.post("", response_model=SkuMapResponse, status_code=201)
def create_sku_map(
    payload: SkuMapCreate,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    _validate_product(db, payload.product_id, user.store_id)

    if payload.platform == "walmart":
        existing = db.query(WalmartSkuMap).filter(
            WalmartSkuMap.store_id == user.store_id,
            WalmartSkuMap.walmart_sku == payload.external_sku,
        ).first()
        if existing:
            raise HTTPException(status_code=409, detail=f"Walmart SKU {payload.external_sku} already mapped")
        row = WalmartSkuMap(
            store_id=user.store_id, walmart_sku=payload.external_sku,
            product_id=payload.product_id, units_per_sale=payload.units_per_sale,
        )
    else:  # amazon
        existing = db.query(AmazonSkuMap).filter(
            AmazonSkuMap.store_id == user.store_id,
            AmazonSkuMap.amazon_sku == payload.external_sku,
        ).first()
        if existing:
            raise HTTPException(status_code=409, detail=f"Amazon SKU {payload.external_sku} already mapped")
        row = AmazonSkuMap(
            store_id=user.store_id, amazon_sku=payload.external_sku,
            product_id=payload.product_id, units_per_sale=payload.units_per_sale,
        )

    db.add(row)
    db.commit()
    db.refresh(row)

    pmap = {}
    if row.product_id:
        p = db.query(Product).filter(Product.id == row.product_id).first()
        if p: pmap[p.id] = p.name
    return _row_to_response(row, payload.platform, pmap)


def _get_row_or_404(db: Session, map_id: str, platform: Platform, store_id: str):
    Model = WalmartSkuMap if platform == "walmart" else AmazonSkuMap
    row = db.query(Model).filter(Model.id == map_id, Model.store_id == store_id).first()
    if not row:
        raise HTTPException(status_code=404, detail=f"{platform} SKU map {map_id} not found")
    return row


@router.put("/{map_id}", response_model=SkuMapResponse)
def update_sku_map(
    map_id: str,
    payload: SkuMapUpdate,
    platform: Platform = Query(...),
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    row = _get_row_or_404(db, map_id, platform, user.store_id)
    if payload.product_id is not None:
        _validate_product(db, payload.product_id, user.store_id)
        row.product_id = payload.product_id
    if payload.external_sku is not None:
        if platform == "walmart":
            row.walmart_sku = payload.external_sku
        else:
            row.amazon_sku = payload.external_sku
    if payload.units_per_sale is not None:
        row.units_per_sale = payload.units_per_sale

    db.commit()
    db.refresh(row)

    pmap = {}
    if row.product_id:
        p = db.query(Product).filter(Product.id == row.product_id).first()
        if p: pmap[p.id] = p.name
    return _row_to_response(row, platform, pmap)


@router.delete("/{map_id}", status_code=204)
def delete_sku_map(
    map_id: str,
    platform: Platform = Query(...),
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    row = _get_row_or_404(db, map_id, platform, user.store_id)
    db.delete(row)
    db.commit()
    return None
