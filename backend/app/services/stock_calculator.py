"""
Stock Calculator - Ported from V1 data_model.py
Handles combo decomposition, shipped component aggregation, and stock KPI calculation.
pandas/numpy imported lazily inside functions to reduce Railway startup memory.
"""
from __future__ import annotations
from datetime import datetime, timedelta
from sqlalchemy.orm import Session

from app.models.sales import SalesOrder
from app.models.combo import Combo, ComboItem
from app.models.product import Product
from app.models.inventory import InitialInventory, IncomingStock


def _build_combo_dict(db: Session, store_id: str) -> dict:
    from sqlalchemy import text
    rows = db.execute(text("""
        SELECT c.combo_sku, p.name AS product_name, p.id AS product_id, ci.quantity
        FROM combos c
        JOIN combo_items ci ON ci.combo_id = c.id
        JOIN products p ON p.id = ci.product_id
        WHERE c.store_id = :sid
    """), {"sid": store_id}).fetchall()
    combo_dict: dict = {}
    for r in rows:
        combo_dict.setdefault(r.combo_sku, []).append({
            "product": r.product_name,
            "product_id": r.product_id,
            "units": r.quantity,
        })
    return combo_dict


def _load_orders_df(db: Session, store_id: str):
    import pandas as pd
    from sqlalchemy import text
    result = db.execute(text("""
        SELECT
            tiktok_order_id          AS "Order ID",
            sku                      AS "SKU ID",
            seller_sku               AS "Seller SKU",
            product_name             AS "Product Name",
            COALESCE(quantity, 1)    AS "Quantity",
            status                   AS "Order Status",
            substatus                AS "Order Substatus",
            order_date               AS "Order_Date",
            created_time             AS "Created Time",
            shipped_time             AS "Shipped Time",
            COALESCE(sku_subtotal_after_discount, 0) AS "SKU Subtotal After Discount",
            COALESCE(order_amount, 0)                AS "Order Amount",
            COALESCE(order_refund_amount, 0)         AS "Order Refund Amount",
            COALESCE(shipping_fee_after_discount, 0) AS "Shipping Fee After Discount",
            COALESCE(original_shipping_fee, 0)       AS "Original Shipping Fee",
            COALESCE(sku_seller_discount, 0)         AS "SKU Seller Discount",
            COALESCE(sku_platform_discount, 0)       AS "SKU Platform Discount",
            cancelation_return_type  AS "Cancelation/Return Type",
            fulfillment_type         AS "Fulfillment Type",
            buyer_username           AS "Buyer Username",
            variation                AS "Variation",
            state                    AS "State",
            city                     AS "City",
            COALESCE(platform, 'tiktok') AS "Platform"
        FROM sales_orders
        WHERE store_id = :sid
    """), {"sid": store_id})
    rows = result.fetchall()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=result.keys())
    df["Order_Date"]    = pd.to_datetime(df["Order_Date"],    errors="coerce")
    df["Created Time"]  = pd.to_datetime(df["Created Time"],  errors="coerce")
    df["Shipped Time"]  = pd.to_datetime(df["Shipped Time"],  errors="coerce")
    df["SKU_ID_Clean"]  = df["Seller SKU"].fillna(df["SKU ID"]).astype(str).str.strip()
    return df


def decompose_orders(df, combo_dict: dict):
    import pandas as pd
    if df.empty:
        df = df.copy()
        df["ComponentKey"] = df["SKU_ID_Clean"]
        df["ComponentQty"] = df["Quantity"]
        df["Is_Combo"] = False
        return df

    is_combo = df["SKU_ID_Clean"].isin(combo_dict)

    # Non-combo rows — vectorized, no iteration
    non_combo = df[~is_combo].copy()
    pname = non_combo["Product Name"].astype(str).str.strip()
    non_combo["ComponentKey"] = pname.where(pname.ne("") & pname.ne("nan"), non_combo["SKU_ID_Clean"])
    non_combo["ComponentQty"] = non_combo["Quantity"]
    non_combo["Is_Combo"] = False

    # Combo rows — iterate only this small subset
    combo_rows = []
    for _, order in df[is_combo].iterrows():
        for comp in combo_dict[order["SKU_ID_Clean"]]:
            row = order.copy()
            row["ComponentKey"] = comp["product"]
            row["ComponentQty"] = order["Quantity"] * comp["units"]
            row["Is_Combo"] = True
            combo_rows.append(row)

    if combo_rows:
        return pd.concat([non_combo, pd.DataFrame(combo_rows)], ignore_index=True)
    return non_combo


def build_shipped_components(decomposed, initial_date):
    import pandas as pd
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
    orders_df = _load_orders_df(db, store_id)
    if orders_df.empty:
        return []

    combo_dict = _build_combo_dict(db, store_id)
    known_combo_lower = {k.strip().lower() for k in combo_dict.keys()}

    products = db.query(Product).filter(Product.store_id == store_id).all()
    known_product_lower = {p.name.strip().lower() for p in products}
    known_lower = known_combo_lower | known_product_lower

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

    return summary.sort_values('order_count', ascending=False).to_dict(orient='records')


def calculate_stock(db: Session, store_id: str, coverage_days: int = 30):
    import pandas as pd
    import numpy as np

    combo_dict = _build_combo_dict(db, store_id)
    orders_df = _load_orders_df(db, store_id)

    from app.models.store import Store
    store = db.query(Store).filter(Store.id == store_id).first()
    initial_date_str = "2026-01-01"
    if store and store.settings and "initial_inventory_date" in store.settings:
        initial_date_str = store.settings["initial_inventory_date"]
    initial_date = pd.to_datetime(initial_date_str)
    today = pd.Timestamp.now()
    days_since_start = max(1, (today - initial_date).days)
    actual_30d = min(30, days_since_start)
    actual_60d = min(60, days_since_start)
    weeks_30d = max(1, actual_30d / 7)
    weeks_60d = max(1, actual_60d / 7)

    if not orders_df.empty:
        decomposed = decompose_orders(orders_df, combo_dict)
        if "Fulfillment Type" in decomposed.columns:
            decomposed["Is_FBT"] = decomposed["Fulfillment Type"].astype(str).str.lower().str.contains("tiktok", na=False)
        else:
            decomposed["Is_FBT"] = False
        shipped = build_shipped_components(decomposed, initial_date)
    else:
        decomposed = pd.DataFrame()
        shipped = pd.DataFrame(columns=["ComponentKey", "QtyShipped"])

    from sqlalchemy import text as _text
    _inv = db.execute(_text("""
        SELECT p.name AS product_name, p.id AS product_id, p.category, ii.quantity
        FROM initial_inventory ii
        JOIN products p ON p.id = ii.product_id
        WHERE ii.store_id = :sid
    """), {"sid": store_id}).fetchall()
    if _inv:
        inv_rows = [{"ProductKey": r.product_name.strip().lower(),
                     "ProductoNombre": r.product_name,
                     "Initial_Stock": r.quantity,
                     "Tipo": r.category,
                     "product_id": r.product_id} for r in _inv]
        inv_stock = pd.DataFrame(inv_rows).groupby("ProductKey").agg(
            Initial_Stock=("Initial_Stock", "sum"),
            Tipo=("Tipo", "first"),
            ProductoNombre=("ProductoNombre", "first"),
            product_id=("product_id", "first"),
        ).reset_index()
    else:
        inv_stock = pd.DataFrame(columns=["ProductKey", "Initial_Stock", "Tipo", "ProductoNombre", "product_id"])
    _inc = db.execute(_text("""
        SELECT p.name AS product_name, COALESCE(i.status, 'pending') AS status, i.qty_ordered
        FROM incoming_stock i
        JOIN products p ON p.id = i.product_id
        WHERE i.store_id = :sid
    """), {"sid": store_id}).fetchall()
    if _inc:
        inc_rows = [{"_pk": r.product_name.strip().lower(), "_status": r.status.strip(), "qty": r.qty_ordered}
                    for r in _inc]
        pending_df = pd.DataFrame(inc_rows)
    else:
        pending_df = pd.DataFrame(columns=["_pk", "_status", "qty"])

    if not pending_df.empty:
        pending_df["_status_lower"] = pending_df["_status"].str.lower().str.strip()
        # V1 adds both Recibido and Ajuste (Ajuste can be negative) to Initial_Stock
        recibido = pending_df[pending_df["_status_lower"].isin(["recibido", "ajuste"])]
        if not recibido.empty:
            rec_agg = recibido.groupby("_pk")["qty"].sum().reset_index()
            rec_agg.columns = ["ProductKey", "Recibido_Add"]
            inv_stock = inv_stock.merge(rec_agg, on="ProductKey", how="left")
            inv_stock["Recibido_Add"] = inv_stock["Recibido_Add"].fillna(0)
            inv_stock["Initial_Stock"] = inv_stock["Initial_Stock"] + inv_stock["Recibido_Add"]
            inv_stock.drop(columns=["Recibido_Add"], inplace=True)

        # Exclude received, cancelled, and adjustment entries — only active orders count
        EXCLUDE = {"recibido", "cancelado", "cancelled", "ajuste"}
        pendiente_active = pending_df[~pending_df["_status_lower"].isin(EXCLUDE)]
        pend_agg = pendiente_active.groupby("_pk")["qty"].sum().reset_index() if not pendiente_active.empty else pd.DataFrame(columns=["_pk", "qty"])
        pend_agg.columns = ["ProductKey", "PedidosPendiente"] if not pend_agg.empty else ["ProductKey", "PedidosPendiente"]
    else:
        pend_agg = pd.DataFrame(columns=["ProductKey", "PedidosPendiente"])

    shipped["ProductKey"] = shipped["ComponentKey"].str.strip().str.lower()
    shipped_agg = shipped.groupby("ProductKey")["QtyShipped"].sum().reset_index()

    stock = inv_stock.merge(shipped_agg, on="ProductKey", how="left")
    stock["QtyShipped"] = stock["QtyShipped"].fillna(0)
    stock["StockActualizado"] = stock["Initial_Stock"] - stock["QtyShipped"]

    # FBT inventory split: Stock_Warehouse and Stock_FBT
    _fbt_rows = db.execute(_text("""
        SELECT goods_name, SUM(total_units) AS total_units
        FROM fbt_inventory
        WHERE store_id = :sid AND goods_name IS NOT NULL
        GROUP BY goods_name
    """), {"sid": store_id}).fetchall()
    fbt_inv_map = {r.goods_name.strip().lower(): r.total_units for r in _fbt_rows} if _fbt_rows else {}
    stock["FBT_Sent"] = stock["ProductKey"].map(fbt_inv_map).fillna(0)

    if not decomposed.empty:
        _sd = decomposed[decomposed["Order_Date"] >= initial_date].copy()
        _s_statuses = {"Delivered", "Shipped", "Completed"}
        if "Order Status" in _sd.columns:
            _mask_s = _sd["Order Status"].astype(str).str.strip().isin(_s_statuses)
            if "Shipped Time" in _sd.columns:
                _sd = _sd[_sd["Shipped Time"].notna() | _mask_s]
            else:
                _sd = _sd[_mask_s]
        if "Cancelation/Return Type" in _sd.columns:
            _sd = _sd[_sd["Cancelation/Return Type"].isin(["nan", "", "None", None]) | _sd["Cancelation/Return Type"].isna()]
        _is_fbt_mask = _sd["Is_FBT"] if "Is_FBT" in _sd.columns else pd.Series(False, index=_sd.index)
        _wh_agg = _sd[~_is_fbt_mask].groupby("ComponentKey").agg(QtyShipped_WH=("ComponentQty", "sum")).reset_index()
        _wh_agg["ProductKey"] = _wh_agg["ComponentKey"].str.strip().str.lower()
        _fbt_agg = _sd[_is_fbt_mask].groupby("ComponentKey").agg(QtyShipped_FBT=("ComponentQty", "sum")).reset_index()
        _fbt_agg["ProductKey"] = _fbt_agg["ComponentKey"].str.strip().str.lower()
        stock = stock.merge(_wh_agg[["ProductKey", "QtyShipped_WH"]], on="ProductKey", how="left")
        stock["QtyShipped_WH"] = stock["QtyShipped_WH"].fillna(0)
        stock = stock.merge(_fbt_agg[["ProductKey", "QtyShipped_FBT"]], on="ProductKey", how="left")
        stock["QtyShipped_FBT"] = stock["QtyShipped_FBT"].fillna(0)
    else:
        stock["QtyShipped_WH"] = 0
        stock["QtyShipped_FBT"] = 0

    has_fbt = stock["FBT_Sent"] > 0
    stock["Stock_Warehouse"] = np.where(
        has_fbt,
        stock["Initial_Stock"] - stock["FBT_Sent"] - stock["QtyShipped_WH"],
        stock["Initial_Stock"] - stock["QtyShipped"],
    )
    stock["Stock_FBT"] = np.where(has_fbt, stock["FBT_Sent"] - stock["QtyShipped_FBT"], 0)

    if not pend_agg.empty:
        stock = stock.merge(pend_agg, on="ProductKey", how="left")
    else:
        stock["PedidosPendiente"] = 0
    stock["PedidosPendiente"] = stock["PedidosPendiente"].fillna(0)
    stock["StockConPedidos"] = stock["StockActualizado"] + stock["PedidosPendiente"]

    if stock.empty:
        return stock

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

    if not decomposed.empty:
        active = decomposed[decomposed["Order_Date"] >= initial_date].copy()
        if "Order Status" in active.columns:
            active = active[~active["Order Status"].astype(str).str.contains("Cancel", case=False, na=False)]
        active["_pk"] = active["ComponentKey"].str.strip().str.lower()

        def sales_in_period(days):
            cutoff = today - timedelta(days=days)
            return active[active["Order_Date"] >= cutoff].groupby("_pk")["ComponentQty"].sum()

        s7  = sales_in_period(7)
        s30 = sales_in_period(30)
        s60 = sales_in_period(60)
        total_sales = active.groupby("_pk")["ComponentQty"].sum()

        stock["Sales_7d"]  = stock["ProductKey"].map(s7).fillna(0)
        stock["Sales_30d"] = stock["ProductKey"].map(s30).fillna(0)
        stock["Sales_60d"] = stock["ProductKey"].map(s60).fillna(0)
        stock["DaysSelectedTotal"] = stock["ProductKey"].map(total_sales).fillna(0)

        # WH/FBT 30d sales split for Days_Cov_WH / Days_Cov_FBT
        if "Is_FBT" in active.columns:
            _a30 = active[active["Order_Date"] >= today - timedelta(days=30)]
            stock["Sales_30d_WH"]  = stock["ProductKey"].map(_a30[~_a30["Is_FBT"]].groupby("_pk")["ComponentQty"].sum()).fillna(0)
            stock["Sales_30d_FBT"] = stock["ProductKey"].map(_a30[_a30["Is_FBT"]].groupby("_pk")["ComponentQty"].sum()).fillna(0)
        else:
            stock["Sales_30d_WH"]  = stock["Sales_30d"]
            stock["Sales_30d_FBT"] = 0
    else:
        for col in ["Sales_7d", "Sales_30d", "Sales_60d", "DaysSelectedTotal", "Sales_30d_WH", "Sales_30d_FBT"]:
            stock[col] = 0

    for col in ["Sales_7d", "Sales_30d", "Sales_60d", "DaysSelectedTotal",
                "Initial_Stock", "QtyShipped", "StockActualizado", "PedidosPendiente",
                "StockConPedidos", "Coste", "PRECIO", "UNIDADES POR CAJA", "ValorInventario"]:
        if col in stock.columns:
            stock[col] = pd.to_numeric(stock[col], errors="coerce").fillna(0)

    stock["AvgVentas30d"]   = (stock["Sales_30d"] / actual_30d).round(2)
    stock["AvgVentas60d"]   = (stock["Sales_60d"] / actual_60d).round(2)
    stock["WeeklyAvg_30d"]  = (stock["Sales_30d"] / weeks_30d).round(2)
    stock["WeeklyAvg_60d"]  = (stock["Sales_60d"] / weeks_60d).round(2)

    stock["Days_Coverage"] = np.where(
        stock["AvgVentas30d"] > 0,
        (stock["StockActualizado"] / stock["AvgVentas30d"]).round(1),
        999,
    )

    # WH/FBT coverage days
    avg_wh  = (stock["Sales_30d_WH"]  / actual_30d).clip(lower=0)
    avg_fbt = (stock["Sales_30d_FBT"] / actual_30d).clip(lower=0)
    stock["Days_Cov_WH"]  = np.where(avg_wh  > 0, np.minimum(stock["Stock_Warehouse"] / avg_wh,  365), -999)
    stock["Days_Cov_FBT"] = np.where(avg_fbt > 0, np.minimum(stock["Stock_FBT"]       / avg_fbt, 365), -999)
    stock["Days_Cov_WH"]  = stock["Days_Cov_WH"].round(0).astype(int)
    stock["Days_Cov_FBT"] = stock["Days_Cov_FBT"].round(0).astype(int)

    stock["Inv_deseado"]      = (stock["AvgVentas30d"] * coverage_days).round(0)
    stock["Unid_a_comprar"]   = np.maximum(0, stock["Inv_deseado"] - stock["StockActualizado"]).round(0)
    stock["Cajas_a_comprar"]  = np.where(
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
