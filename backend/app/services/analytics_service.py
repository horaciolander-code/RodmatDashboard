"""
Analytics Service - Ported from V1 data_model.py query methods.
pandas imported lazily inside functions to reduce Railway startup memory.
"""
from __future__ import annotations
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from typing import Optional

from app.models.sales import SalesOrder, AffiliateSale
from app.models.store import Store
from app.services.stock_calculator import _load_orders_df, _build_combo_dict, decompose_orders, calculate_stock, get_unknown_combo_skus

_cache: dict = {}
_CACHE_TTL = 300


def _get_cached(store_id: str, key: str):
    cache_key = (store_id, key)
    if cache_key in _cache:
        ts, data = _cache[cache_key]
        if (datetime.now() - ts).total_seconds() < _CACHE_TTL:
            return data
    return None


def _set_cache(store_id: str, key: str, data):
    _cache[(store_id, key)] = (datetime.now(), data)


def get_overview_metrics(db: Session, store_id: str) -> dict:
    import pandas as pd
    cached = _get_cached(store_id, "overview")
    if cached:
        return cached

    df = _load_orders_df(db, store_id)
    if df.empty:
        return {}

    active_mask = ~df["Order Status"].astype(str).str.contains("Cancel", case=False, na=False)
    net_orders = df.loc[active_mask, "Order ID"].nunique()
    total_orders = df["Order ID"].nunique()
    gmv = df["SKU Subtotal After Discount"].sum()

    order_level = df.drop_duplicates(subset="Order ID")
    net_order_amount = (order_level["Order Amount"] - order_level["Order Refund Amount"]).sum()
    shipping_fees = order_level["Shipping Fee After Discount"].sum()
    net_wo_shipping = net_order_amount - shipping_fees
    seller_discount = df["SKU Seller Discount"].sum()
    platform_discount = df["SKU Platform Discount"].sum()

    affiliates = db.query(AffiliateSale).filter(AffiliateSale.store_id == store_id).all()
    creator_commission = sum(a.commission or 0 for a in affiliates)
    creator_payment = sum(a.payment_amount or 0 for a in affiliates)
    creator_order_count = len(set(a.order_id for a in affiliates if a.order_id))

    referral_fees = gmv * 0.06

    today = pd.Timestamp.now()
    month_start = today.replace(day=1)
    prev_start = month_start - pd.DateOffset(months=1)
    dom = today.day

    curr_sales = df[(df["Order_Date"] >= month_start) & (df["Order_Date"] <= today)]["SKU Subtotal After Discount"].sum()
    prev_sales = df[(df["Order_Date"] >= prev_start) & (df["Order_Date"] < prev_start + pd.DateOffset(days=dom))]["SKU Subtotal After Discount"].sum()
    pct_vs_prev = ((curr_sales - prev_sales) / prev_sales * 100) if prev_sales > 0 else 0

    result = {
        "netOrder": int(net_orders),
        "totalOrders": int(total_orders),
        "TITKOKGMVOrderAmount": round(float(gmv), 2),
        "NetOrderAmount": round(float(net_order_amount), 2),
        "ShippingFees": round(float(shipping_fees), 2),
        "netOrderWOUshipping": round(float(net_wo_shipping), 2),
        "SellerDiscount": round(float(seller_discount), 2),
        "PlatformDiscount": round(float(platform_discount), 2),
        "CreatorCommission": round(float(creator_commission), 2),
        "CreatorPayment": round(float(creator_payment), 2),
        "CreatorOrderCount": int(creator_order_count),
        "RefferarFees": round(float(referral_fees), 2),
        "PctVsPrevMonth": round(float(pct_vs_prev), 1),
    }
    _set_cache(store_id, "overview", result)
    return result


def get_sales_by_month(db: Session, store_id: str,
                       date_from: Optional[str] = None, date_to: Optional[str] = None) -> list:
    import pandas as pd
    df = _load_orders_df(db, store_id)
    if df.empty:
        return []
    if date_from:
        df = df[df["Order_Date"] >= pd.to_datetime(date_from)]
    if date_to:
        df = df[df["Order_Date"] <= pd.to_datetime(date_to)]
    df["Month"] = df["Order_Date"].dt.to_period("M").astype(str)
    monthly = df.groupby("Month").agg(
        GMV=("SKU Subtotal After Discount", "sum"),
        Orders=("Order ID", "nunique"),
        Units=("Quantity", "sum"),
    ).reset_index()
    return monthly.to_dict(orient="records")


def get_sales_by_day(db: Session, store_id: str,
                     date_from: Optional[str] = None, date_to: Optional[str] = None) -> list:
    import pandas as pd
    df = _load_orders_df(db, store_id)
    if df.empty:
        return []
    if date_from:
        df = df[df["Order_Date"] >= pd.to_datetime(date_from)]
    if date_to:
        df = df[df["Order_Date"] <= pd.to_datetime(date_to)]
    df["Day"] = df["Order_Date"].dt.date.astype(str)
    daily = df.groupby("Day").agg(
        GMV=("SKU Subtotal After Discount", "sum"),
        Orders=("Order ID", "nunique"),
        Units=("Quantity", "sum"),
    ).reset_index()
    return daily.to_dict(orient="records")


def get_stock_summary(db: Session, store_id: str, coverage_days: int = 30) -> list:
    cached = _get_cached(store_id, f"stock_{coverage_days}")
    if cached:
        return cached
    stock = calculate_stock(db, store_id, coverage_days)
    if stock.empty:
        return []
    cols = ["ProductoNombre", "Tipo", "Initial_Stock", "QtyShipped", "StockActualizado",
            "PedidosPendiente", "StockConPedidos", "Sales_7d", "Sales_30d", "Sales_60d",
            "AvgVentas30d", "WeeklyAvg_30d", "Days_Coverage", "SellThroughRate",
            "Coste", "PRECIO", "ValorInventario"]
    available = [c for c in cols if c in stock.columns]
    result = stock[available].fillna(0).to_dict(orient="records")
    _set_cache(store_id, f"stock_{coverage_days}", result)
    return result


def get_stock_detail(db: Session, store_id: str, coverage_days: int = 30) -> list:
    stock = calculate_stock(db, store_id, coverage_days)
    if stock.empty:
        return []
    cols = ["ProductoNombre", "Tipo", "Initial_Stock", "QtyShipped", "StockActualizado",
            "PedidosPendiente", "StockConPedidos", "Sales_7d", "Sales_30d", "Sales_60d",
            "AvgVentas30d", "AvgVentas60d", "WeeklyAvg_30d", "Days_Coverage",
            "Inv_deseado", "Unid_a_comprar", "Cajas_a_comprar", "Importe_a_comprar",
            "SellThroughRate", "Coste", "PRECIO", "UNIDADES POR CAJA", "ValorInventario",
            "Ruptura_7d_Pct_Global"]
    available = [c for c in cols if c in stock.columns]
    return stock[available].fillna(0).to_dict(orient="records")


def get_reorder_list(db: Session, store_id: str, coverage_days: int = 30) -> list:
    stock = calculate_stock(db, store_id, coverage_days)
    if stock.empty:
        return []
    reorder = stock[stock["Unid_a_comprar"] > 0].copy()
    cols = ["ProductoNombre", "StockActualizado", "AvgVentas30d", "Days_Coverage",
            "Inv_deseado", "Unid_a_comprar", "Cajas_a_comprar", "Importe_a_comprar",
            "Coste", "UNIDADES POR CAJA"]
    available = [c for c in cols if c in reorder.columns]
    return reorder[available].fillna(0).sort_values("Unid_a_comprar", ascending=False).to_dict(orient="records")


def get_top_creators(db: Session, store_id: str, n: int = 20) -> list:
    import pandas as pd
    affiliates = db.query(AffiliateSale).filter(AffiliateSale.store_id == store_id).all()
    if not affiliates:
        return []
    rows = [{"Creator Username": a.creator_username, "Payment Amount": a.payment_amount or 0,
             "Commission": a.commission or 0, "Order ID": a.order_id} for a in affiliates]
    cr = pd.DataFrame(rows)
    top = cr.groupby("Creator Username").agg(
        GMV=("Payment Amount", "sum"),
        Commission=("Commission", "sum"),
        Orders=("Order ID", "nunique"),
    ).reset_index().sort_values("GMV", ascending=False).head(n)
    return top.to_dict(orient="records")


def get_creator_by_type(db: Session, store_id: str) -> list:
    import pandas as pd
    affiliates = db.query(AffiliateSale).filter(AffiliateSale.store_id == store_id).all()
    if not affiliates:
        return []
    rows = [{"Content Type": a.content_type, "Payment Amount": a.payment_amount or 0,
             "Order ID": a.order_id, "Creator Username": a.creator_username} for a in affiliates]
    cr = pd.DataFrame(rows)
    result = cr.groupby("Content Type").agg(
        GMV=("Payment Amount", "sum"),
        Orders=("Order ID", "nunique"),
        Creators=("Creator Username", "nunique"),
    ).reset_index().sort_values("GMV", ascending=False)
    return result.to_dict(orient="records")


def get_creator_by_month(db: Session, store_id: str) -> list:
    import pandas as pd
    affiliates = db.query(AffiliateSale).filter(AffiliateSale.store_id == store_id).all()
    if not affiliates:
        return []
    rows = [{"Time Created": a.time_created, "Creator Username": a.creator_username,
             "Payment Amount": a.payment_amount or 0, "Order ID": a.order_id} for a in affiliates]
    cr = pd.DataFrame(rows)
    cr["Time Created"] = pd.to_datetime(cr["Time Created"], errors="coerce")
    cr = cr.dropna(subset=["Time Created"])
    if cr.empty:
        return []
    cr["Month"] = cr["Time Created"].dt.to_period("M").astype(str)
    result = cr.groupby(["Month", "Creator Username"]).agg(
        GMV=("Payment Amount", "sum"),
        Orders=("Order ID", "nunique"),
    ).reset_index()
    return result.to_dict(orient="records")


def get_filtered_orders(db: Session, store_id: str,
                        date_from: Optional[str] = None, date_to: Optional[str] = None,
                        status: Optional[str] = None, sku: Optional[str] = None,
                        buyer: Optional[str] = None, fulfillment: Optional[str] = None,
                        order_id: Optional[str] = None, product_name: Optional[str] = None,
                        limit: int = 500, offset: int = 0) -> dict:
    q = db.query(SalesOrder).filter(SalesOrder.store_id == store_id)
    if date_from:
        q = q.filter(SalesOrder.order_date >= date_from)
    if date_to:
        q = q.filter(SalesOrder.order_date <= date_to)
    if status:
        q = q.filter(SalesOrder.status == status)
    if sku:
        q = q.filter(SalesOrder.seller_sku.contains(sku))
    if buyer:
        q = q.filter(SalesOrder.buyer_username.contains(buyer))
    if fulfillment:
        q = q.filter(SalesOrder.fulfillment_type == fulfillment)
    if order_id:
        q = q.filter(SalesOrder.tiktok_order_id.contains(order_id))
    if product_name:
        q = q.filter(SalesOrder.product_name.contains(product_name))

    total = q.count()
    orders = q.order_by(SalesOrder.order_date.desc()).offset(offset).limit(limit).all()

    rows = []
    for o in orders:
        rows.append({
            "order_id": o.tiktok_order_id,
            "order_date": str(o.order_date) if o.order_date else None,
            "sku": o.sku,
            "seller_sku": o.seller_sku,
            "product_name": o.product_name,
            "quantity": o.quantity,
            "status": o.status,
            "substatus": o.substatus,
            "price": o.price,
            "sku_subtotal_after_discount": o.sku_subtotal_after_discount,
            "order_amount": o.order_amount,
            "order_refund_amount": o.order_refund_amount,
            "shipping_fee": o.shipping_fee_after_discount,
            "fulfillment_type": o.fulfillment_type,
            "buyer_username": o.buyer_username,
            "variation": o.variation,
            "shipped_time": str(o.shipped_time) if o.shipped_time else None,
            "cancelation_return_type": o.cancelation_return_type,
            "city": o.city,
            "state": o.state,
        })
    return {"total": total, "orders": rows}


def get_frequent_buyers(db: Session, store_id: str) -> list:
    df = _load_orders_df(db, store_id)
    if df.empty or "Buyer Username" not in df.columns:
        return []
    buyers = df.groupby("Buyer Username").agg(
        GMV=("SKU Subtotal After Discount", "sum"),
        OrderCount=("Order ID", "nunique"),
        OrderAmount=("Order Amount", "sum"),
    ).reset_index().sort_values("OrderCount", ascending=False)
    return buyers.to_dict(orient="records")


def get_top_combos(db: Session, store_id: str, n: int = 15) -> list:
    df = _load_orders_df(db, store_id)
    if df.empty:
        return []
    if "Order Status" in df.columns:
        df = df[~df["Order Status"].astype(str).str.contains("Cancel", case=False, na=False)]
    group_cols = ["SKU ID", "Seller SKU", "Product Name"]
    available = [c for c in group_cols if c in df.columns]
    if not available:
        return []
    combos = df.groupby(available).agg(
        OrderCount=("Order ID", "nunique"),
        GMV=("SKU Subtotal After Discount", "sum"),
        Units=("Quantity", "sum"),
    ).reset_index().sort_values("OrderCount", ascending=False).head(n)
    combos["WeeklyAvg"] = (combos["GMV"] / 4.28).round(2)
    return combos.to_dict(orient="records")


def get_finances(db: Session, store_id: str) -> list:
    stock = calculate_stock(db, store_id)
    if stock.empty:
        return []
    cols = ["ProductoNombre", "Tipo", "StockActualizado", "Coste", "PRECIO", "ValorInventario"]
    available = [c for c in cols if c in stock.columns]
    result = stock[available].fillna(0).copy()
    result["ValorRetail"] = result["StockActualizado"] * result.get("PRECIO", 0)
    return result.to_dict(orient="records")


def get_unknown_combos(db: Session, store_id: str) -> list:
    return get_unknown_combo_skus(db, store_id)
