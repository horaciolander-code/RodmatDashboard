"""
Stock Calculator - Ported from V1 data_model.py
Handles combo decomposition, shipped component aggregation, and stock KPI calculation.
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy.orm import Session

from app.models.sales import SalesOrder
from app.models.combo import Combo, ComboItem
from app.models.product import Product
from app.models.inventory import InitialInventory, IncomingStock


def _build_combo_dict(db: Session, store_id: str) -> dict:
    """Build {seller_sku: [{product_name, product_id, units}]} from DB combos."""
    combos = db.query(Combo).filter(Combo.store_id == store_id).all()
    combo_dict = {}
    for combo in combos:
        items = []
        for item in combo.items:
            product = db.query(Product).filter(Product.id == item.product_id).first()
            if product:
                items.append({
                    "product": product.name,
                    "product_id": product.id,
                    "units": item.quantity,
                })
        if items:
            combo_dict[combo.combo_sku] = items
    return combo_dict


def _load_orders_df(db: Session, store_id: str) -> pd.DataFrame:
    """Load all sales orders for a store into a DataFrame."""
    orders = db.query(SalesOrder).filter(SalesOrder.store_id == store_id).all()
    if not orders:
        return pd.DataFrame()

    rows = []
    for o in orders:
        rows.append({
            "Order ID": o.tiktok_order_id,
            "SKU ID": o.sku,
            "Seller SKU": o.seller_sku,
            "Product Name": o.product_name,
            "Quantity": o.quantity or 1,
            "Order Status": o.status,
            "Order Substatus": o.substatus,
            "Order_Date": o.order_date,
            "Created Time": o.created_time,
            "Shipped Time": o.shipped_time,
            "SKU Subtotal After Discount": o.sku_subtotal_after_discount or 0,
            "Order Amount": o.order_amount or 0,
            "Order Refund Amount": o.order_refund_amount or 0,
            "Shipping Fee After Discount": o.shipping_fee_after_discount or 0,
            "SKU Seller Discount": o.sku_seller_discount or 0,
            "SKU Platform Discount": o.sku_platform_discount or 0,
            "Cancelation/Return Type": o.cancelation_return_type,
            "Fulfillment Type": o.fulfillment_type,
            "Buyer Username": o.buyer_username,
            "Variation": o.variation,
        })
    df = pd.DataFrame(rows)
    df["Order_Date"] = pd.to_datetime(df["Order_Date"], errors="coerce")
    df["Created Time"] = pd.to_datetime(df["Created Time"], errors="coerce")
    df["Shipped Time"] = pd.to_datetime(df["Shipped Time"], errors="coerce")
    df["SKU_ID_Clean"] = df["Seller SKU"].fillna(df["SKU ID"]).astype(str).str.strip()
    return df


def decompose_orders(df: pd.DataFrame, combo_dict: dict) -> pd.DataFrame:
    """Decompose orders by combo mapping. Returns df with ComponentKey, ComponentQty."""
    if not combo_dict or df.empty:
        df = df.copy()
        df["ComponentKey"] = df.get("Product Name", df.get("SKU_ID_Clean", ""))
        df["ComponentQty"] = df["Quantity"]
        df["Is_Combo"] = False
        return df

    rows = []
    for _, order in df.iterrows():
        sku = order["SKU_ID_Clean"]
        qty = order["Quantity"]
        if sku in combo_dict:
            for comp in combo_dict[sku]:
                row = order.copy()
                row["ComponentKey"] = comp["product"]
                row["ComponentQty"] = qty * comp["units"]
                row["Is_Combo"] = True
                rows.append(row)
        else:
            row = order.copy()
            pname = order.get("Product Name", sku)
            row["ComponentKey"] = pname if pd.notna(pname) and str(pname).strip() else sku
            row["ComponentQty"] = qty
            row["Is_Combo"] = False
            rows.append(row)
    return pd.DataFrame(rows)


def build_shipped_components(decomposed: pd.DataFrame, initial_date: pd.Timestamp) -> pd.DataFrame:
    """Aggregate shipped component quantities since initial_date."""
    df = decomposed.copy()
    df = df[df["Order_Date"] >= initial_date]

    shipped_statuses = ["Delivered", "Shipped", "Completed"]
    if "Order Status" in df.columns:
        mask_status = df["Order Status"].astype(str).str.strip().isin(shipped_statuses)
    else:
        mask_status = pd.Series(True, index=df.index)

    if "Shipped Time" in df.columns:
        mask_shipped = df["Shipped Time"].notna()
        df = df[mask_shipped | mask_status]
    else:
        df = df[mask_status]

    if "Cancelation/Return Type" in df.columns:
        df = df[df["Cancelation/Return Type"].isin(["nan", "", "None", None]) |
                df["Cancelation/Return Type"].isna()]

    shipped = df.groupby("ComponentKey").agg(QtyShipped=("ComponentQty", "sum")).reset_index()
    return shipped


def get_unknown_combo_skus(db: Session, store_id: str) -> list:
    """Detect seller SKUs in orders that are not in combos or product catalog.
    Returns list of dicts: {seller_sku, product_name, order_count, total_qty}."""
    orders_df = _load_orders_df(db, store_id)
    if orders_df.empty:
        return []

    combo_dict = _build_combo_dict(db, store_id)

    # Known combo SKUs (lowered)
    known_combo_lower = {k.strip().lower() for k in combo_dict.keys()}

    # Known product names (lowered)
    products = db.query(Product).filter(Product.store_id == store_id).all()
    known_product_lower = {p.name.strip().lower() for p in products}

    known_lower = known_combo_lower | known_product_lower

    # All unique SKUs from orders
    order_skus = orders_df['SKU_ID_Clean'].dropna().unique()
    sku_map = {s.strip().lower(): s for s in order_skus if s and s != 'nan'}

    unknown_originals = [orig for lower, orig in sku_map.items() if lower not in known_lower]

    if not unknown_originals:
        return []

    df = orders_df[orders_df['SKU_ID_Clean'].isin(unknown_originals)].copy()
    summary = df.groupby('SKU_ID_Clean').agg(
        product_name=('Product Name', 'first'),
        order_count=('Order ID', 'nunique'),
        total_qty=('Quantity', 'sum'),
    ).reset_index().rename(columns={'SKU_ID_Clean': 'seller_sku'})

    summary = summary.sort_values('order_count', ascending=False)
    return summary.to_dict(orient='records')


def calculate_stock(db: Session, store_id: str, coverage_days: int = 30) -> pd.DataFrame:
    """Full stock calculation pipeline. Returns stock summary DataFrame with KPIs."""
    combo_dict = _build_combo_dict(db, store_id)
    orders_df = _load_orders_df(db, store_id)

    # Get initial_inventory_date from store settings
    from app.models.store import Store
    store = db.query(Store).filter(Store.id == store_id).first()
    initial_date_str = "2026-01-01"
    if store and store.settings and "initial_inventory_date" in store.settings:
        initial_date_str = store.settings["initial_inventory_date"]
    initial_date = pd.to_datetime(initial_date_str)

    # Decompose
    if not orders_df.empty:
        decomposed = decompose_orders(orders_df, combo_dict)
        shipped = build_shipped_components(decomposed, initial_date)
    else:
        decomposed = pd.DataFrame()
        shipped = pd.DataFrame(columns=["ComponentKey", "QtyShipped"])

    # Initial inventory from DB
    inv_records = db.query(InitialInventory).filter(InitialInventory.store_id == store_id).all()
    inv_rows = []
    for r in inv_records:
        product = db.query(Product).filter(Product.id == r.product_id).first()
        if product:
            inv_rows.append({
                "ProductKey": product.name.strip().lower(),
                "ProductoNombre": product.name,
                "Initial_Stock": r.quantity,
                "Tipo": product.category,
                "product_id": product.id,
            })

    if inv_rows:
        inv_stock = pd.DataFrame(inv_rows).groupby("ProductKey").agg(
            Initial_Stock=("Initial_Stock", "sum"),
            Tipo=("Tipo", "first"),
            ProductoNombre=("ProductoNombre", "first"),
            product_id=("product_id", "first"),
        ).reset_index()
    else:
        inv_stock = pd.DataFrame(columns=["ProductKey", "Initial_Stock", "Tipo", "ProductoNombre", "product_id"])

    # Pending inventory
    incoming = db.query(IncomingStock).filter(IncomingStock.store_id == store_id).all()
    if incoming:
        inc_rows = []
        for r in incoming:
            product = db.query(Product).filter(Product.id == r.product_id).first()
            if product:
                inc_rows.append({
                    "_pk": product.name.strip().lower(),
                    "_status": (r.status or "pending").strip(),
                    "qty": r.qty_ordered,
                })
        pending_df = pd.DataFrame(inc_rows)
    else:
        pending_df = pd.DataFrame(columns=["_pk", "_status", "qty"])

    # Add Recibido to initial stock
    if not pending_df.empty:
        recibido = pending_df[pending_df["_status"] == "Recibido"]
        if not recibido.empty:
            rec_agg = recibido.groupby("_pk")["qty"].sum().reset_index()
            rec_agg.columns = ["ProductKey", "Recibido_Add"]
            inv_stock = inv_stock.merge(rec_agg, on="ProductKey", how="left")
            inv_stock["Recibido_Add"] = inv_stock["Recibido_Add"].fillna(0)
            inv_stock["Initial_Stock"] = inv_stock["Initial_Stock"] + inv_stock["Recibido_Add"]
            inv_stock.drop(columns=["Recibido_Add"], inplace=True)

        pendiente_active = pending_df[pending_df["_status"] != "Recibido"]
        if not pendiente_active.empty:
            pend_agg = pendiente_active.groupby("_pk")["qty"].sum().reset_index()
            pend_agg.columns = ["ProductKey", "PedidosPendiente"]
        else:
            pend_agg = pd.DataFrame(columns=["ProductKey", "PedidosPendiente"])
    else:
        pend_agg = pd.DataFrame(columns=["ProductKey", "PedidosPendiente"])

    # Shipped components
    shipped["ProductKey"] = shipped["ComponentKey"].str.strip().str.lower()
    shipped_agg = shipped.groupby("ProductKey")["QtyShipped"].sum().reset_index()

    # Merge
    stock = inv_stock.merge(shipped_agg, on="ProductKey", how="left")
    stock["QtyShipped"] = stock["QtyShipped"].fillna(0)
    stock["StockActualizado"] = stock["Initial_Stock"] - stock["QtyShipped"]

    if not pend_agg.empty:
        stock = stock.merge(pend_agg, on="ProductKey", how="left")
    else:
        stock["PedidosPendiente"] = 0
    stock["PedidosPendiente"] = stock["PedidosPendiente"].fillna(0)
    stock["StockConPedidos"] = stock["StockActualizado"] + stock["PedidosPendiente"]

    if stock.empty:
        return stock

    # Product catalog info
    products = db.query(Product).filter(Product.store_id == store_id).all()
    cat_map = {p.name.strip().lower(): {
        "Coste": p.price_cost or 0,
        "PRECIO": p.price_sale or 0,
        "UNIDADES POR CAJA": p.units_per_box or 1,
    } for p in products}

    stock["Coste"] = stock["ProductKey"].map(lambda k: cat_map.get(k, {}).get("Coste", 0))
    stock["PRECIO"] = stock["ProductKey"].map(lambda k: cat_map.get(k, {}).get("PRECIO", 0))
    stock["UNIDADES POR CAJA"] = stock["ProductKey"].map(lambda k: cat_map.get(k, {}).get("UNIDADES POR CAJA", 1))
    stock["ValorInventario"] = stock["StockActualizado"] * stock["Coste"]

    # KPIs
    if not decomposed.empty:
        today = pd.Timestamp.now()
        active = decomposed[decomposed["Order_Date"] >= initial_date].copy()
        if "Order Status" in active.columns:
            active = active[~active["Order Status"].astype(str).str.contains("Cancel", case=False, na=False)]
        active["_pk"] = active["ComponentKey"].str.strip().str.lower()

        def sales_in_period(days):
            cutoff = today - timedelta(days=days)
            return active[active["Order_Date"] >= cutoff].groupby("_pk")["ComponentQty"].sum()

        s7 = sales_in_period(7)
        s30 = sales_in_period(30)
        s60 = sales_in_period(60)
        total_sales = active.groupby("_pk")["ComponentQty"].sum()

        stock["Sales_7d"] = stock["ProductKey"].map(s7).fillna(0)
        stock["Sales_30d"] = stock["ProductKey"].map(s30).fillna(0)
        stock["Sales_60d"] = stock["ProductKey"].map(s60).fillna(0)
        stock["DaysSelectedTotal"] = stock["ProductKey"].map(total_sales).fillna(0)
    else:
        for col in ["Sales_7d", "Sales_30d", "Sales_60d", "DaysSelectedTotal"]:
            stock[col] = 0

    # Ensure numeric types
    for col in ["Sales_7d", "Sales_30d", "Sales_60d", "DaysSelectedTotal",
                "Initial_Stock", "QtyShipped", "StockActualizado", "PedidosPendiente",
                "StockConPedidos", "Coste", "PRECIO", "UNIDADES POR CAJA", "ValorInventario"]:
        if col in stock.columns:
            stock[col] = pd.to_numeric(stock[col], errors="coerce").fillna(0)

    stock["AvgVentas30d"] = (stock["Sales_30d"] / 30).round(2)
    stock["AvgVentas60d"] = (stock["Sales_60d"] / 60).round(2)
    stock["WeeklyAvg_30d"] = (stock["Sales_30d"] / 4.28).round(2)

    stock["Days_Coverage"] = np.where(
        stock["AvgVentas30d"] > 0,
        (stock["StockActualizado"] / stock["AvgVentas30d"]).round(1),
        999,
    )

    stock["Inv_deseado"] = (stock["AvgVentas30d"] * coverage_days).round(0)
    stock["Unid_a_comprar"] = np.maximum(0, stock["Inv_deseado"] - stock["StockActualizado"]).round(0)
    stock["Cajas_a_comprar"] = np.where(
        stock["UNIDADES POR CAJA"] > 0,
        np.ceil(stock["Unid_a_comprar"] / stock["UNIDADES POR CAJA"]),
        stock["Unid_a_comprar"],
    )
    stock["Importe_a_comprar"] = stock["Unid_a_comprar"] * stock["Coste"]

    stock["SellThroughRate"] = np.where(
        (stock["DaysSelectedTotal"] + stock["StockActualizado"]) > 0,
        (stock["DaysSelectedTotal"] / (stock["DaysSelectedTotal"] + stock["StockActualizado"]) * 100).round(1),
        0,
    )

    total_products = len(stock[stock["Initial_Stock"] > 0])
    low_count = len(stock[(stock["Days_Coverage"] < 7) & (stock["Initial_Stock"] > 0)])
    stock["Ruptura_7d_Pct_Global"] = (low_count / total_products * 100) if total_products > 0 else 0

    return stock
