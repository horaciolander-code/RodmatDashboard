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


def _safe_datetime(val, dayfirst: bool = False) -> datetime | None:
    try:
        import pandas as pd
        dt = pd.to_datetime(val, errors='coerce', dayfirst=dayfirst)
        return dt.to_pydatetime() if pd.notna(dt) else None
    except Exception:
        return None


def parse_orders_csv(content: bytes, store_id: str, db: Session) -> dict:
    """Parse TikTok orders CSV (AllBBDD format). Replaces TikTok orders only (keeps Amazon)."""
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
                original_shipping_fee=_safe_float(row.get('Original Shipping Fee')),
                sku_seller_discount=_safe_float(row.get('SKU Seller Discount')),
                sku_platform_discount=_safe_float(row.get('SKU Platform Discount')),
                cancelation_return_type=_safe_str(row.get('Cancelation/Return Type')),
                fulfillment_type=_safe_str(row.get('Fulfillment Type')),
                buyer_username=_safe_str(row.get('Buyer Username')),
                variation=_safe_str(row.get('Variation')),
                recipient=_safe_str(row.get('Recipient')),
                city=_safe_str(row.get('City')),
                state=_safe_str(row.get('State')),
                platform='tiktok',
                raw_data=None,
            ))
        except Exception:
            errors += 1

    # Replace TikTok orders only — Amazon orders (platform='amazon') are preserved.
    from sqlalchemy import text as _text
    db.execute(_text("DELETE FROM sales_orders WHERE store_id = :sid AND COALESCE(platform, 'tiktok') = 'tiktok'"), {"sid": store_id})
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
    """Parse affiliate/creator CSV. Upserts by (store_id, order_id, sku) using bulk INSERT ON CONFLICT."""
    import uuid
    import pandas as pd
    from sqlalchemy.dialects.postgresql import insert as pg_insert

    df = pd.read_csv(io.BytesIO(content), encoding='utf-8-sig')
    df.columns = df.columns.str.strip()

    rows = []
    errors = 0

    for _, row in df.iterrows():
        try:
            order_id = _safe_str(row.get('Order ID'))
            if not order_id:
                errors += 1
                continue

            sku = _safe_str(row.get('SKU ID', row.get('Product SKU ID')))
            # Commission rate is exported as "15%" — strip the percent sign
            comm_rate_raw = str(row.get('Standard commission rate', '') or '').replace('%', '').strip()

            rows.append(dict(
                id=str(uuid.uuid4()),
                store_id=store_id,
                order_id=order_id,
                creator_username=_safe_str(row.get('Creator Username')),
                product_name=_safe_str(row.get('Product Name')),
                sku=sku,
                quantity=_safe_int(row.get('Quantity', 1)),
                commission=_safe_float(
                    row.get('Est. standard commission payment',
                            row.get('Actual Commission Payment'))
                ),
                content_type=_safe_str(row.get('Content Type')),
                payment_amount=_safe_float(row.get('Payment Amount')),
                order_status=_safe_str(row.get('Order Status')),
                # TikTok affiliate CSVs use DD/MM/YYYY format
                time_created=_safe_datetime(row.get('Time Created'), dayfirst=True),
                commission_rate=_safe_float(comm_rate_raw),
                est_commission_base=_safe_float(row.get('Est. Commission Base')),
                raw_data=None,
            ))
        except Exception:
            errors += 1

    if not rows:
        return {"total_rows": len(df), "inserted": 0, "updated": 0, "errors": errors}

    # Bulk upsert: INSERT ... ON CONFLICT (store_id, order_id, sku) DO UPDATE
    # No N+1 SELECT queries — one batch per 1000 rows
    update_cols = ['creator_username', 'product_name', 'quantity', 'commission',
                   'content_type', 'payment_amount', 'order_status', 'time_created',
                   'commission_rate', 'est_commission_base']
    BATCH = 1000
    total = 0
    for i in range(0, len(rows), BATCH):
        batch = rows[i:i + BATCH]
        stmt = pg_insert(AffiliateSale).values(batch)
        stmt = stmt.on_conflict_do_update(
            constraint='uq_store_affiliate_order_sku',
            set_={col: getattr(stmt.excluded, col) for col in update_cols},
        )
        db.execute(stmt)
        db.flush()
        total += len(batch)

    db.commit()
    return {"total_rows": len(df), "inserted": total, "updated": 0, "errors": errors}


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


def parse_amazon_txt(content: bytes, store_id: str, db: Session) -> dict:
    """Parse Amazon order report TXT (tab-separated). Replaces Amazon orders for this store.

    Quantity is expanded by units_per_sale from amazon_sku_map (AV-ID/5 with qty=1 → quantity=5).
    product_name is set to the DB product name so stock_calculator deducts from unified inventory.
    """
    import uuid as _uuid
    import pandas as pd
    from sqlalchemy import text as _text
    from sqlalchemy.dialects.postgresql import insert as pg_insert

    # Load SKU map for this store
    sku_rows = db.execute(_text("""
        SELECT m.amazon_sku, p.name AS product_name, m.units_per_sale
        FROM amazon_sku_map m
        LEFT JOIN products p ON p.id = m.product_id
        WHERE m.store_id = :sid
    """), {"sid": store_id}).fetchall()
    sku_map = {
        r.amazon_sku: {"product_name": r.product_name, "units_per_sale": r.units_per_sale or 1}
        for r in sku_rows
    }

    df = pd.read_csv(
        io.BytesIO(content), sep='\t', encoding='utf-8-sig',
        on_bad_lines='skip', engine='python'
    )
    df.columns = df.columns.str.strip()

    rows = []
    errors = 0

    for _, row in df.iterrows():
        try:
            order_id = _safe_str(row.get('amazon-order-id'))
            if not order_id:
                errors += 1
                continue

            item_status = _safe_str(row.get('item-status')) or ''
            order_status_raw = _safe_str(row.get('order-status')) or ''

            if 'Cancel' in item_status or 'Cancel' in order_status_raw:
                status = 'Cancelled'
            elif item_status == 'Shipped':
                status = 'Shipped'
            elif item_status == 'Unshipped':
                status = 'Awaiting Shipment'
            else:
                status = item_status or order_status_raw or 'Pending'

            amazon_sku = _safe_str(row.get('sku')) or ''
            qty_ordered = _safe_int(row.get('quantity', 1))

            mapping = sku_map.get(amazon_sku, {})
            mapped_product_name = mapping.get('product_name')
            units_per_sale = mapping.get('units_per_sale', 1)
            expanded_qty = qty_ordered * units_per_sale

            item_price = _safe_float(row.get('item-price')) or 0.0
            shipping_price = _safe_float(row.get('shipping-price')) or 0.0
            promo_discount = _safe_float(row.get('item-promotion-discount')) or 0.0
            purchase_date = _safe_datetime(row.get('purchase-date'))

            rows.append(dict(
                id=str(_uuid.uuid4()),
                store_id=store_id,
                tiktok_order_id=order_id,
                order_date=purchase_date,
                sku=_safe_str(row.get('asin')),
                seller_sku=amazon_sku,
                product_name=mapped_product_name or _safe_str(row.get('product-name')),
                quantity=expanded_qty,
                status=status,
                substatus=item_status,
                price=item_price / max(qty_ordered, 1),
                shipped_time=None,
                created_time=purchase_date,
                sku_subtotal_after_discount=item_price,
                order_amount=item_price + shipping_price,
                order_refund_amount=0.0,
                shipping_fee_after_discount=shipping_price,
                original_shipping_fee=shipping_price,
                sku_seller_discount=0.0,
                sku_platform_discount=promo_discount,
                cancelation_return_type=None,
                fulfillment_type='Merchant',
                buyer_username=None,
                variation=None,
                recipient=None,
                city=_safe_str(row.get('ship-city')),
                state=_safe_str(row.get('ship-state')),
                platform='amazon',
                raw_data=None,
            ))
        except Exception:
            errors += 1

    # Replace Amazon orders only — keep TikTok orders intact
    db.execute(_text("DELETE FROM sales_orders WHERE store_id = :sid AND platform = 'amazon'"), {"sid": store_id})
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
