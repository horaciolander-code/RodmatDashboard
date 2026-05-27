from collections import defaultdict

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.combo import Combo, ComboItem
from app.models.product import Product
from app.models.user import User
from app.schemas.combo import ComboCreate, ComboResponse
from app.dependencies import get_current_user

router = APIRouter(prefix="/api/combos", tags=["combos"])


@router.post("", response_model=ComboResponse, status_code=201)
def create_combo(
    payload: ComboCreate,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    # Verify all products belong to the user's store
    product_ids = [item.product_id for item in payload.items]
    products = db.query(Product).filter(Product.id.in_(product_ids)).all()
    found_ids = {p.id for p in products}
    for pid in product_ids:
        if pid not in found_ids:
            raise HTTPException(status_code=404, detail=f"Product {pid} not found")
    for p in products:
        if p.store_id != user.store_id:
            raise HTTPException(status_code=403, detail="Cannot use products from another store")

    # Deduplicate items: if the same product_id appears multiple times in the
    # payload, sum the quantities. The DB has a UNIQUE(combo_id, product_id)
    # constraint (uq_combo_product), so duplicate rows would 500 on flush.
    totals: dict[str, int] = defaultdict(int)
    for item in payload.items:
        totals[item.product_id] += item.quantity

    combo = Combo(store_id=user.store_id, combo_sku=payload.combo_sku, combo_name=payload.combo_name)
    db.add(combo)
    db.flush()
    for pid, qty in totals.items():
        db.add(ComboItem(combo_id=combo.id, product_id=pid, quantity=qty))
    db.commit()
    db.refresh(combo)
    return combo


@router.get("", response_model=list[ComboResponse])
def list_combos(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    combos = db.query(Combo).filter(Combo.store_id == user.store_id).all()
    product_ids = {item.product_id for combo in combos for item in combo.items}
    pmap = {p.id: p.name for p in db.query(Product).filter(Product.id.in_(product_ids)).all()} if product_ids else {}
    result = []
    for c in combos:
        result.append({
            "id": c.id,
            "store_id": c.store_id,
            "combo_sku": c.combo_sku,
            "combo_name": c.combo_name,
            "created_at": c.created_at,
            "items": [
                {"id": i.id, "product_id": i.product_id,
                 "product_name": pmap.get(i.product_id), "quantity": i.quantity}
                for i in c.items
            ],
        })
    return result
