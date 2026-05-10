"""
CSV/Excel Import Service
Ports V1 data loading logic to DB-based import with upsert support.
pandas/openpyxl imported lazily inside functions to reduce startup memory.
"""
from __future__ import annotations
import io
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import and_

from app.models.sales import SalesOrder, AffiliateSale
from app.models.product import Product
from app.models.combo import Combo, ComboItem
from app.models.inventory import InitialInventory, IncomingStock


def _detect_separator(content: bytes) -> str:
    first_line = content.split(b'\n')[0].decode('utf-8-sig', errors='replace')
    return '\t' if first_line.count('\t') > first_line.count(',') else ','


def _safe_float(val) -> float | None:
    try:
        v = float(val)
        return v if v == v else None  # NaN check without pandas
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> int:
    try:
        v = float(val)
        return int(v) if v == v else 0
    except (ValueError, TypeError):
        return 0


def _safe_str(val) -> str | None:
    if val is None:
        return None
    try:
        import math
        if isinstance(val, float) and math.isnan(val):
            return None
    except Exception:
        pass
    s = str(val).strip()
    return s if s and s != 'nan' else None


def _safe_datetime(val) -> datetime | None:
    try:
        import pandas as pd
        dt = pd.to_datetime(val, errors='coerce', format='mixed')
        return dt.to_pydatetime() if pd.notna(dt) else None
    except Exception:
        return None


def parse_orders_csv(content: bytes, store_id: str, db: Session) -> dict:
    """Parse TikTok orders CSV (AllBBDD format). Upserts by (store_id, order_id, sku)."""
    import uuid as _uuid
    import pandas as pd
    from sqlalchemy.dialects.postgresql import insert as pg_insert

    sep = _detect_separator(content)
    df = pd.read_csv(
        io.BytesIO(content), sep=sep,
        encoding='utf-8-sig', on_bad_lines='skip', engine='python'
    )
    df.columns = df.columns.str.strip()

    rows = []
    errors = 0

    for _, row in df.iterrows():
        try:
            order_id = _safe_str(row.get('Order ID'))
            if not order_id:
                errors += 1
                continue

            sku_id = _safe_str(row.get('SKU ID'))
            created_time = _safe_datetime(row.get('Created Time'))
            shipped_time = _safe_datetime(row.get('Shipped Time'))

            rows.append(dict(
                id=str(_uuid.uuid4()),
                store_id=store_id,
                tiktok_order_id=order_id,
                order_date=created_time,
                sku=sku_id,
                seller_sku=_safe_str(row.get('Seller SKU')),
                product_name=_safe_str(row.get('Product Name')),
                quantity=_safe_int(row.get('Quantity', 1)),
                status=_safe_str(row.get('Order Status')),
                substatus=_safe_str(row.get('Order Substatus')),
                price=_safe_float(row.get('SKU Unit Original Price')),
                shipped_time=shipped_time,
                created_time=created_time,
                sku_subtotal_after_discount=_safe_float(row.get('SKU Subtotal After Discount')),
                order_amount=_safe_float(row.get('Order Amount')),
                order_refund_amount=_safe_float(row.get('Order Refund Amount')),
                shipping_fee_after_discount=_safe_float(row.get('Shipping Fee After Discount')),
                sku_seller_discount=_safe_float(row.get('SKU Seller Discount')),
                sku_platform_discount=_safe_float(row.get('SKU Platform Discount')),
                cancelation_return_type=_safe_str(row.get('Cancelation/Return Type')),
                fulfillment_type=_safe_str(row.get('Fulfillment Type')),
                buyer_username=_safe_str(row.get('Buyer Username')),
                variation=_safe_str(row.get('Variation')),
                recipient=_safe_str(row.get('Recipient')),
                city=_safe_str(row.get('City')),
                state=_safe_str(row.get('State')),
                raw_data=None,
            ))
        except Exception:
            errors += 1

    # Full replace: delete all existing orders for this store, then bulk-insert fresh.
    # This prevents ghost rows from accumulating when orders are removed from the CSV.
    db.query(SalesOrder).filter(SalesOrder.store_id == store_id).delete()
    db.flush()

    BATCH = 3000
    total_processed = 0
    for i in range(0, len(rows), BATCH):
        batch = rows[i:i + BATCH]
        stmt = pg_insert(SalesOrder).values(batch)
        db.execute(stmt)
        db.flush()
        total_processed += len(batch)

    db.commit()

    return {"total_rows": len(df), "inserted": total_processed, "updated": 0, "errors": errors}


def parse_affiliate_csv(content: bytes, store_id: str, db: Session) -> dict:
    """Parse affiliate/creator CSV. Upserts by (store_id, order_id, sku)."""
    import pandas as pd
    df = pd.read_csv(io.BytesIO(content), encoding='utf-8-sig')
    df.columns = df.columns.str.strip()

    inserted, updated, errors = 0, 0, 0
    batch = []

    for _, row in df.iterrows():
        try:
            order_id = _safe_str(row.get('Order ID'))
            sku = _safe_str(row.get('SKU ID', row.get('Product SKU ID')))

            if not order_id:
                errors += 1
                continue

            existing = db.query(AffiliateSale).filter(and_(
                AffiliateSale.store_id == store_id,
                AffiliateSale.order_id == order_id,
                AffiliateSale.sku == sku,
            )).first()

            data = dict(
                store_id=store_id,
                order_id=order_id,
                creator_username=_safe_str(row.get('Creator Username')),
                product_name=_safe_str(row.get('Product Name')),
                sku=sku,
                quantity=_safe_int(row.get('Quantity', 1)),
                commission=_safe_float(row.get('Est. standard commission payment',
                                               row.get('Actual Commission Payment'))),
                content_type=_safe_str(row.get('Content Type')),
                payment_amount=_safe_float(row.get('Payment Amount')),
                order_status=_safe_str(row.get('Order Status')),
                time_created=_safe_datetime(row.get('Time Created')),
                commission_rate=_safe_float(row.get('Standard commission rate')),
                est_commission_base=_safe_float(row.get('Est. Commission Base')),
                raw_data=None,
            )

            if existing:
                for k, v in data.items():
                    if k != 'store_id':
                        setattr(existing, k, v)
                updated += 1
            else:
                batch.append(AffiliateSale(**data))
                inserted += 1

            if len(batch) >= 1000:
                db.bulk_save_objects(batch)
                db.flush()
                batch = []
        except Exception:
            errors += 1

    if batch:
        db.bulk_save_objects(batch)
    db.commit()

    return {"total_rows": len(df), "inserted": inserted, "updated": updated, "errors": errors}


def parse_products_excel(content: bytes, store_id: str, db: Session) -> dict:
    """Parse Productos individualizados.xlsx. Upserts by (store_id, sku)."""
    import pandas as pd
    df = pd.read_excel(io.BytesIO(content))
    df.columns = df.columns.str.strip()

    inserted, updated, errors = 0, 0, 0

    for _, row in df.iterrows():
        try:
            producto = _safe_str(row.get('Producto'))
            if not producto:
                errors += 1
                continue

            sku = producto.strip()
            existing = db.query(Product).filter(and_(
                Product.store_id == store_id,
                Product.sku == sku,
            )).first()

            price_cost = _safe_float(row.get('Coste'))
            price_sale = _safe_float(row.get('PRECIO', row.get('Precio')))
            units_per_box = _safe_int(row.get('UNIDADES POR CAJA', row.get('Unidades por caja', 1)))
            category = _safe_str(row.get('Tipo'))
            supplier = _safe_str(row.get('Proveedor'))

            if existing:
                existing.name = sku
                existing.price_cost = price_cost
                existing.price_sale = price_sale
                existing.units_per_box = units_per_box if units_per_box else existing.units_per_box
                existing.category = category or existing.category
                existing.supplier = supplier or existing.supplier
                updated += 1
            else:
                product = Product(
                    store_id=store_id,
                    sku=sku,
                    name=sku,
                    category=category,
                    price_sale=price_sale,
                    price_cost=price_cost,
                    supplier=supplier,
                    units_per_box=units_per_box if units_per_box else 1,
                )
                db.add(product)
                inserted += 1
        except Exception:
            errors += 1

    db.commit()
    return {"total_rows": len(df), "inserted": inserted, "updated": updated, "errors": errors}


def parse_combos_excel(content: bytes, store_id: str, db: Session) -> dict:
    """Parse Listado de combos tiktok.xlsx. Creates combo + items."""
    import pandas as pd
    df = pd.read_excel(io.BytesIO(content))
    df.columns = df.columns.str.strip()

    sku_col = 'SKU SELLER' if 'SKU SELLER' in df.columns else 'SKUID'
    product_cols = [c for c in df.columns if c.startswith('Product')]

    inserted, updated, errors = 0, 0, 0

    # Build product SKU -> ID map for this store
    products = db.query(Product).filter(Product.store_id == store_id).all()
    product_map = {p.sku.lower(): p.id for p in products}
    for p in products:
        product_map[p.name.lower()] = p.id

    for _, row in df.iterrows():
        try:
            combo_sku = _safe_str(row.get(sku_col))
            if not combo_sku:
                errors += 1
                continue

            components = []
            for pcol in product_cols:
                val = _safe_str(row.get(pcol))
                if val:
                    components.append(val)

            if not components:
                errors += 1
                continue

            existing = db.query(Combo).filter(and_(
                Combo.store_id == store_id,
                Combo.combo_sku == combo_sku,
            )).first()

            combo_name = _safe_str(row.get('Nombre combo', row.get('NOMBRE', ''))) or combo_sku

            # Count repeated products correctly: Product1=A, Product2=A → quantity=2 for A
            # This matches V1 data_model.py behavior where each ProductN column = 1 unit
            component_qty: dict[str, int] = {}
            for comp in components:
                pid = product_map.get(comp.lower())
                if pid:
                    component_qty[pid] = component_qty.get(pid, 0) + 1

            if existing:
                existing.combo_name = combo_name
                db.query(ComboItem).filter(ComboItem.combo_id == existing.id).delete()
                db.flush()
                for pid, qty in component_qty.items():
                    db.add(ComboItem(combo_id=existing.id, product_id=pid, quantity=qty))
                updated += 1
            else:
                combo = Combo(store_id=store_id, combo_sku=combo_sku, combo_name=combo_name)
                db.add(combo)
                db.flush()
                for pid, qty in component_qty.items():
                    db.add(ComboItem(combo_id=combo.id, product_id=pid, quantity=qty))
                inserted += 1
        except Exception:
            errors += 1

    db.commit()
    return {"total_rows": len(df), "inserted": inserted, "updated": updated, "errors": errors}


def parse_initial_inventory_excel(content: bytes, store_id: str, db: Session) -> dict:
    """Parse Inventario inicial.xlsx. Full replace for store."""
    import pandas as pd
    df = pd.read_excel(io.BytesIO(content))
    df.columns = df.columns.str.strip()

    products = db.query(Product).filter(Product.store_id == store_id).all()
    product_map = {p.sku.lower(): p.id for p in products}
    for p in products:
        product_map[p.name.lower()] = p.id

    inserted, errors = 0, 0

    db.query(InitialInventory).filter(InitialInventory.store_id == store_id).delete()
    db.flush()

    for _, row in df.iterrows():
        try:
            # Support both column names: "Producto" (generic) and "ProductoNombre" (V1 inventory rotation)
            producto = _safe_str(row.get('Producto') or row.get('ProductoNombre'))
            if not producto:
                errors += 1
                continue

            product_id = product_map.get(producto.lower())
            quantity = _safe_int(row.get('Initial_Stock', row.get('total', row.get('Total', row.get('Cantidad', 0)))))

            if not product_id:
                product = Product(store_id=store_id, sku=producto, name=producto)
                db.add(product)
                db.flush()
                product_id = product.id
                product_map[producto.lower()] = product_id

            from datetime import date as date_type
            start_date = date_type(2026, 1, 1)

            record = InitialInventory(
                store_id=store_id,
                product_id=product_id,
                quantity=quantity,
                start_date=start_date,
            )
            db.add(record)
            inserted += 1
        except Exception:
            errors += 1

    db.commit()
    return {"total_rows": len(df), "inserted": inserted, "updated": 0, "errors": errors}


def parse_pending_inventory_excel(content: bytes, store_id: str, db: Session) -> dict:
    """Parse Inventario pendiente de recibir.xlsx. Full replace for store."""
    import pandas as pd
    df = pd.read_excel(io.BytesIO(content))
    df.columns = df.columns.str.strip()

    products = db.query(Product).filter(Product.store_id == store_id).all()
    product_map = {p.sku.lower(): p.id for p in products}
    for p in products:
        product_map[p.name.lower()] = p.id

    db.query(IncomingStock).filter(IncomingStock.store_id == store_id).delete()
    db.flush()

    inserted, errors = 0, 0

    for _, row in df.iterrows():
        try:
            producto = _safe_str(row.get('Producto'))
            if not producto:
                errors += 1
                continue

            product_id = product_map.get(producto.lower())
            if not product_id:
                product = Product(store_id=store_id, sku=producto, name=producto)
                db.add(product)
                db.flush()
                product_id = product.id
                product_map[producto.lower()] = product_id

            qty = _safe_int(row.get('Unidades pedidas', row.get('Cantidad', 0)))
            status_val = _safe_str(row.get('Status', row.get('Estado', 'pending'))) or 'pending'
            supplier = _safe_str(row.get('Proveedor'))
            tracking = _safe_str(row.get('Tracking'))
            cost = _safe_float(row.get('Coste', row.get('Precio')))
            notes = _safe_str(row.get('Notas', row.get('Notes')))

            order_date_val = row.get('Fecha pedido', row.get('Fecha', None))
            order_date = None
            if order_date_val is not None and pd.notna(order_date_val):
                try:
                    dt = pd.to_datetime(order_date_val)
                    if pd.notna(dt):
                        order_date = dt.date()
                except Exception:
                    pass

            record = IncomingStock(
                store_id=store_id,
                product_id=product_id,
                qty_ordered=qty,
                order_date=order_date,
                status=status_val,
                supplier=supplier,
                tracking=tracking,
                cost=cost,
                notes=notes,
            )
            db.add(record)
            inserted += 1
        except Exception:
            errors += 1

    db.commit()
    return {"total_rows": len(df), "inserted": inserted, "updated": 0, "errors": errors}
