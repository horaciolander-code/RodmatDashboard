"""
Rodmat Dashboard V2 - Multi-tenant Streamlit Dashboard
Full parity with V1: all pages, filters, charts, and features.
"""
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go

from api_client import api_get, api_post, api_put, api_patch, api_delete, login, register

st.set_page_config(
    page_title="Rodmat Dashboard V2",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    .block-container {padding-top: 1rem; padding-bottom: 1rem;}
    [data-testid="stMetric"] {
        background-color: #f0f2f6; padding: 0.75rem;
        border-radius: 0.5rem; border-left: 4px solid #1f77b4;
    }
    [data-testid="stMetricLabel"] p { color: #31333F !important; font-weight: 600; }
    [data-testid="stMetricValue"] { color: #0e1117 !important; font-size: 1.3rem; }
    [data-testid="stMetricDelta"] { color: #31333F !important; }
    @media (prefers-color-scheme: dark) {
        [data-testid="stMetric"] { background-color: #262730; border-left: 4px solid #4da6ff; }
        [data-testid="stMetricLabel"] p { color: #fafafa !important; }
        [data-testid="stMetricValue"] { color: #ffffff !important; }
        [data-testid="stMetricDelta"] { color: #c0c0c0 !important; }
    }
    @media (max-width: 768px) {
        [data-testid="stHorizontalBlock"] { flex-wrap: wrap !important; }
        [data-testid="stHorizontalBlock"] > div { flex: 1 1 45% !important; min-width: 45% !important; }
        [data-testid="stMetric"] { padding: 0.5rem; margin-bottom: 0.25rem; }
        [data-testid="stMetricValue"] { font-size: 1rem; }
        [data-testid="stMetricLabel"] p { font-size: 0.75rem !important; }
        .block-container { padding-left: 0.5rem; padding-right: 0.5rem; padding-top: 0.5rem; }
        [data-testid="stDataFrame"] { overflow-x: auto !important; }
        button[data-baseweb="tab"] { font-size: 0.75rem !important; padding: 0.4rem 0.5rem !important; }
    }
    @media (max-width: 480px) {
        [data-testid="stHorizontalBlock"] > div { flex: 1 1 100% !important; min-width: 100% !important; }
        [data-testid="stMetricValue"] { font-size: 0.9rem; }
    }
</style>
""", unsafe_allow_html=True)


# ================================================================== #
#  CACHED API CALLS
# ================================================================== #
@st.cache_data(ttl=300)
def fetch_overview(date_from=None, date_to=None):
    params = {}
    if date_from: params["date_from"] = str(date_from)
    if date_to: params["date_to"] = str(date_to)
    return api_get("/analytics/overview", params) or {}

@st.cache_data(ttl=300)
def fetch_sales_by_month(date_from=None, date_to=None):
    params = {}
    if date_from: params["date_from"] = str(date_from)
    if date_to: params["date_to"] = str(date_to)
    return api_get("/analytics/sales-by-month", params) or []

@st.cache_data(ttl=300)
def fetch_sales_by_day(date_from=None, date_to=None):
    params = {}
    if date_from: params["date_from"] = str(date_from)
    if date_to: params["date_to"] = str(date_to)
    return api_get("/analytics/sales-by-day", params) or []

@st.cache_data(ttl=300)
def fetch_stock_summary(coverage_days=30):
    return api_get("/analytics/stock-summary", {"coverage_days": coverage_days}) or []

@st.cache_data(ttl=300)
def fetch_stock_detail(coverage_days=30):
    return api_get("/analytics/stock-detail", {"coverage_days": coverage_days}) or []

@st.cache_data(ttl=300)
def fetch_top_creators(n=20):
    return api_get("/analytics/creators/top", {"n": n}) or []

@st.cache_data(ttl=300)
def fetch_creator_by_type():
    return api_get("/analytics/creators/by-type") or []

@st.cache_data(ttl=300)
def fetch_creator_by_month():
    return api_get("/analytics/creators/by-month") or []

@st.cache_data(ttl=300)
def fetch_frequent_buyers():
    return api_get("/analytics/frequent-buyers") or []

@st.cache_data(ttl=300)
def fetch_top_combos(n=15):
    return api_get("/analytics/top-combos", {"n": n}) or []

@st.cache_data(ttl=300)
def fetch_finances():
    return api_get("/analytics/finances") or []

@st.cache_data(ttl=300)
def fetch_incoming_stock():
    return api_get("/inventory/incoming") or []

@st.cache_data(ttl=300)
def fetch_fbt_inventory():
    return api_get("/inventory/fbt") or []

@st.cache_data(ttl=300)
def fetch_unknown_combos():
    return api_get("/analytics/unknown-combos") or []

@st.cache_data(ttl=300)
def fetch_combos():
    return api_get("/combos") or []

@st.cache_data(ttl=300)
def fetch_products():
    return api_get("/products") or []

@st.cache_data(ttl=300)
def fetch_combo_sales(date_from=None, date_to=None):
    params = {}
    if date_from: params["date_from"] = str(date_from)
    if date_to: params["date_to"] = str(date_to)
    return api_get("/analytics/combo-sales", params) or []

@st.cache_data(ttl=300)
def fetch_product_monthly_sales(product_name=None):
    params = {}
    if product_name: params["product_name"] = product_name
    return api_get("/analytics/product-monthly-sales", params) or []


# ================================================================== #
#  PAGE 1: OVERVIEW
# ================================================================== #
def page_overview():
    st.header("Resumen General")

    unknown = fetch_unknown_combos()
    if unknown:
        st.warning(f"{len(unknown)} SKU(s) en pedidos sin combo asignado. Ve a Gestion > Gestion Combos para revisarlos.")

    # Slicers
    col_s1, col_s2, col_s3 = st.columns(3)
    with col_s1:
        date_range = st.date_input("Período", value=[], key="ov_date")
    with col_s2:
        substatus_filter = st.text_input("Buscar Subestado", "", key="ov_substatus")
    with col_s3:
        order_search = st.text_input("Buscar Orden", "", key="ov_order")

    date_from = str(date_range[0]) if len(date_range) >= 1 else None
    date_to = str(date_range[1]) if len(date_range) == 2 else (str(date_range[0]) if len(date_range) == 1 else None)

    m = fetch_overview(date_from, date_to)
    if not m:
        st.warning("No hay datos disponibles.")
        return

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Monto Neto", f"${m.get('NetOrderAmount', 0):,.2f}")
    c2.metric("GMV (Subtotal SKU)", f"${m.get('TITKOKGMVOrderAmount', 0):,.2f}")
    c3.metric("Comisión Pagada Creadores", f"${m.get('CreatorCommission', 0):,.2f}")
    c4.metric("GMV Afiliados (completado)", f"${m.get('CreatorPayment', 0):,.2f}")

    c5, c6, c7, c8 = st.columns(4)
    c5.metric("Órdenes Netas", f"{m.get('netOrder', 0):,}")
    c6.metric("Neto sin Envío", f"${m.get('netOrderWOUshipping', 0):,.2f}")
    c7.metric("Gastos de Envío", f"${m.get('ShippingFees', 0):,.2f}")
    c8.metric("% vs Mes Anterior", f"{m.get('PctVsPrevMonth', 0):+.1f}%")

    c9, c10, c11, c12 = st.columns(4)
    c9.metric("Dto. Vendedor+Plataforma", f"${m.get('SellerDiscount', 0) + m.get('PlatformDiscount', 0):,.2f}")
    c10.metric("Comis.+Ref.+DescPlat", f"${m.get('CreatorCommission', 0) + m.get('RefferarFees', 0) + m.get('PlatformDiscount', 0):,.2f}")
    c11.metric("Comis. Referidos (est.)", f"${m.get('RefferarFees', 0):,.2f}")
    c12.metric("Órdenes Afiliados", f"{m.get('CreatorOrderCount', 0):,}")

    st.markdown("---")

    monthly = fetch_sales_by_month(date_from, date_to)
    st.subheader("GMV por Mes")
    if monthly:
        df_m = pd.DataFrame(monthly)
        fig = px.bar(df_m, x="Month", y="GMV", text_auto="$.2s")
        fig.update_layout(height=350, margin=dict(t=10, b=10))
        st.plotly_chart(fig, use_container_width=True, key="ov_monthly")

    # Month filter buttons for daily chart
    st.subheader("GMV por Día")
    month_opts = sorted([r["Month"] for r in monthly]) if monthly else []
    if month_opts:
        if "ov_selected_month" not in st.session_state:
            st.session_state["ov_selected_month"] = "All"
        all_btns = ["All"] + month_opts
        btn_cols = st.columns(len(all_btns))
        for i, label in enumerate(all_btns):
            with btn_cols[i]:
                btn_type = "primary" if st.session_state["ov_selected_month"] == label else "secondary"
                if st.button(label, key=f"ov_btn_{label}", type=btn_type):
                    st.session_state["ov_selected_month"] = label
                    st.rerun()
        selected_month = st.session_state["ov_selected_month"]
    else:
        selected_month = "All"

    daily_raw = fetch_sales_by_day(date_from, date_to)
    if daily_raw:
        df_d = pd.DataFrame(daily_raw)
        if selected_month != "All" and not df_d.empty:
            df_d["_m"] = df_d["Day"].astype(str).str[:7].str.replace("-", "/").apply(
                lambda s: s[:4] + "-" + s[5:7] if "/" in s else s
            )
            # Filter by period string match
            df_d["_period"] = pd.to_datetime(df_d["Day"], errors="coerce").dt.to_period("M").astype(str)
            df_d = df_d[df_d["_period"] == selected_month]
        if not df_d.empty:
            fig = px.line(df_d, x="Day", y="GMV", markers=True, text="GMV")
            fig.update_traces(textposition="top center", texttemplate="$%{text:,.0f}")
            fig.update_layout(height=350, margin=dict(t=10, b=10))
            st.plotly_chart(fig, use_container_width=True, key="ov_daily")

    col3, col4 = st.columns(2)
    with col3:
        st.subheader("Top 10 Creadores por GMV")
        creators = fetch_top_creators(10)
        if creators:
            df_c = pd.DataFrame(creators)
            fig = px.bar(df_c, x="GMV", y="Creator Username", orientation="h",
                         color="GMV", color_continuous_scale="Blues")
            fig.update_layout(height=400, yaxis={"categoryorder": "total ascending"}, margin=dict(t=10))
            st.plotly_chart(fig, use_container_width=True, key="ov_creators")
    with col4:
        st.subheader("Distribución por Contenido")
        ct = fetch_creator_by_type()
        if ct:
            df_ct = pd.DataFrame(ct)
            fig = px.pie(df_ct, values="GMV", names="Content Type")
            fig.update_layout(height=400, margin=dict(t=10))
            st.plotly_chart(fig, use_container_width=True, key="ov_content")


# ================================================================== #
#  PAGE 2: INVENTARIO SUMMARY
# ================================================================== #
def page_inventario_summary():
    st.header("Resumen de Inventario")

    data = fetch_stock_summary()
    if not data:
        st.warning("Sin datos de inventario.")
        return
    df = pd.DataFrame(data)

    col_s1, col_s2, col_s3 = st.columns(3)
    with col_s1:
        tipo_opts = ["Todos"] + sorted(df["Tipo"].dropna().unique().tolist()) if "Tipo" in df.columns else ["Todos"]
        tipo_filter = st.selectbox("Tipo de Producto", tipo_opts, key="inv_tipo")
    with col_s2:
        df_for_opts = df[df["Tipo"] == tipo_filter] if tipo_filter != "Todos" else df
        comp_opts = ["Todos"] + sorted(df_for_opts["ProductoNombre"].dropna().unique().tolist())
        comp_filter = st.selectbox("Producto", comp_opts, key="inv_comp")
    with col_s3:
        days_filter = st.number_input("Días cobertura mínimos", min_value=0, value=0, key="inv_days")

    if tipo_filter != "Todos" and "Tipo" in df.columns:
        df = df[df["Tipo"] == tipo_filter]
    if comp_filter != "Todos":
        df = df[df["ProductoNombre"] == comp_filter]
    if days_filter > 0 and "Days_Coverage" in df.columns:
        df = df[df["Days_Coverage"] >= days_filter]

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Stock Actual", f"{int(df['StockActualizado'].sum()):,}")
    c2.metric("Valor Inventario", f"${df['ValorInventario'].sum():,.2f}")
    low_count = len(df[(df["Days_Coverage"] >= 0) & (df["Days_Coverage"] < 7) & (df["Initial_Stock"] > 0)]) if "Days_Coverage" in df.columns else 0
    c3.metric("Productos Stock Bajo", f"{low_count}")
    c4.metric("Stock Almacén", f"{int(df['Stock_Warehouse'].sum()):,}" if "Stock_Warehouse" in df.columns else "N/A")
    c5.metric("Stock FBT", f"{int(df['Stock_FBT'].sum()):,}" if "Stock_FBT" in df.columns else "N/A")

    if "Days_Coverage" in df.columns:
        low_3 = df[(df["Days_Coverage"] >= 0) & (df["Days_Coverage"] < 999)].nsmallest(3, "Days_Coverage")
        if not low_3.empty:
            st.warning(f"Menor cobertura: {', '.join(low_3['ProductoNombre'].tolist())}")

    st.markdown("---")
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Stock vs Ventas 30d")
        chart = df[["ProductoNombre", "StockActualizado", "Sales_30d"]].sort_values("StockActualizado", ascending=False).head(20)
        fig = go.Figure()
        fig.add_trace(go.Bar(name="Stock", x=chart["ProductoNombre"], y=chart["StockActualizado"],
                             text=chart["StockActualizado"], textposition="outside"))
        fig.add_trace(go.Bar(name="Ventas 30d", x=chart["ProductoNombre"], y=chart["Sales_30d"],
                             text=chart["Sales_30d"], textposition="outside"))
        fig.update_layout(barmode="group", height=400, margin=dict(t=10), xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True, key="inv_stock_vs_sales")
    with col2:
        st.subheader("Valor Inventario por Tipo")
        if "Tipo" in df.columns:
            val = df.groupby("Tipo")["ValorInventario"].sum().reset_index()
            fig = px.bar(val, x="Tipo", y="ValorInventario", color="Tipo", text_auto="$.2s")
            fig.update_layout(height=400, margin=dict(t=10))
            st.plotly_chart(fig, use_container_width=True, key="inv_valor_tipo")

    st.subheader("Stock Actual por Producto")
    display_cols = ["ProductoNombre", "Tipo", "Initial_Stock", "QtyShipped",
                    "StockActualizado", "PedidosPendiente", "StockConPedidos",
                    "Sales_30d", "Days_Coverage", "ValorInventario"]
    available = [c for c in display_cols if c in df.columns]
    display_df = df[available].sort_values("StockActualizado", ascending=True).copy()
    if "Days_Coverage" in display_df.columns:
        display_df["Days_Coverage"] = display_df["Days_Coverage"].apply(
            lambda v: "—" if isinstance(v, (int, float)) and v <= -999 else v
        )
    st.dataframe(display_df, use_container_width=True, height=400)


# ================================================================== #
#  PAGE 3: RESTOCK ANALYSIS
# ================================================================== #
def page_restock_analysis():
    st.header("Análisis de Reabastecimiento")

    col_s1, col_s2 = st.columns(2)
    with col_s1:
        coverage = st.number_input("Días de cobertura objetivo", min_value=7, max_value=180, value=30, key="ra_cov")
    with col_s2:
        data_all = fetch_stock_detail(30)
        tipo_opts = ["Todos"]
        if data_all:
            df_all = pd.DataFrame(data_all)
            if "Tipo" in df_all.columns:
                tipo_opts += sorted(df_all["Tipo"].dropna().unique().tolist())
        tipo_filter = st.selectbox("Tipo de Producto", tipo_opts, key="ra_tipo")

    data = fetch_stock_detail(coverage)
    if not data:
        st.warning("Sin datos de stock.")
        return
    df = pd.DataFrame(data)

    if tipo_filter != "Todos" and "Tipo" in df.columns:
        df = df[df["Tipo"] == tipo_filter]

    df["Inv_deseado_custom"] = (df["AvgVentas30d"] * coverage).round(0)
    df["Unid_a_comprar_custom"] = np.maximum(0, df["Inv_deseado_custom"] - df["StockConPedidos"]).round(0)
    if "UNIDADES POR CAJA" in df.columns:
        df["Cajas_custom"] = np.where(
            df["UNIDADES POR CAJA"] > 0,
            np.ceil(df["Unid_a_comprar_custom"] / df["UNIDADES POR CAJA"]),
            df["Unid_a_comprar_custom"]
        ).astype(int)
    if "Coste" in df.columns:
        df["Importe_custom"] = (df["Unid_a_comprar_custom"] * df["Coste"]).round(2)
        st.metric("Total Importe a Comprar", f"${df['Importe_custom'].sum():,.2f}")

    st.markdown("---")
    st.subheader("Análisis Total Stock")
    table_cols = ["ProductoNombre", "Tipo", "QtyShipped", "StockActualizado", "StockConPedidos",
                  "WeeklyAvg_30d", "Inv_deseado_custom", "Unid_a_comprar_custom",
                  "Days_Coverage", "SellThroughRate"]
    available = [c for c in table_cols if c in df.columns]

    def color_coverage(val):
        if not isinstance(val, (int, float)):
            return ""
        if val <= -999:
            return "background-color: #d0d0d0; color: #555555"
        elif val < 0:
            return "background-color: #ff6666; color: #5c0000"
        elif val < 7:
            return "background-color: #ffcccc; color: #8b0000"
        elif val < 14:
            return "background-color: #ffffcc; color: #8b6914"
        elif val >= 365:
            return "background-color: #e8e8ff; color: #333388"
        else:
            return "background-color: #ccffcc; color: #006400"

    styled = df[available].style.map(
        color_coverage, subset=["Days_Coverage"] if "Days_Coverage" in available else []
    )
    st.dataframe(styled, use_container_width=True, height=500,
                 column_config={"ProductoNombre": st.column_config.TextColumn("ProductoNombre", pinned=True)})

    st.markdown("---")
    st.subheader("Ventas Mensuales por Producto")
    product_opts = ["All"]
    if "ProductoNombre" in df.columns:
        product_opts += sorted(df["ProductoNombre"].dropna().unique().tolist())
    sel_prod = st.selectbox("Filtrar por producto", product_opts, key="ra_prod_filter")
    monthly_sales = fetch_product_monthly_sales(sel_prod if sel_prod != "All" else None)
    if monthly_sales:
        df_ms = pd.DataFrame(monthly_sales)
        title = f"Ventas Mensuales — {sel_prod}" if sel_prod != "All" else "Ventas Mensuales — Todos"
        fig = px.bar(df_ms, x="Mes", y="Unidades Vendidas", title=title, text_auto=True)
        fig.update_layout(height=320, margin=dict(t=40), xaxis_title="")
        fig.update_xaxes(type="category")
        st.plotly_chart(fig, use_container_width=True, key="ra_monthly")

    st.markdown("---")
    st.subheader("Listado de Pedido (Purchase Order)")
    order_list = df[df["Unid_a_comprar_custom"] > 0].copy()
    if not order_list.empty:
        order_cols = ["ProductoNombre", "Unid_a_comprar_custom"]
        if "Cajas_custom" in order_list.columns: order_cols.append("Cajas_custom")
        if "Coste" in order_list.columns: order_cols.append("Coste")
        if "Importe_custom" in order_list.columns: order_cols.append("Importe_custom")
        if "UNIDADES POR CAJA" in order_list.columns: order_cols.append("UNIDADES POR CAJA")
        st.dataframe(order_list[order_cols].rename(columns={
            "ProductoNombre": "Producto", "Unid_a_comprar_custom": "Unidades",
            "Cajas_custom": "Cajas", "Importe_custom": "Importe",
        }), use_container_width=True)
        csv = order_list[order_cols].to_csv(index=False).encode("utf-8")
        st.download_button("Download Purchase Order CSV", data=csv,
                           file_name="purchase_order.csv", mime="text/csv", key="ra_download")
    else:
        st.success("No hay productos que reabastecer con esta cobertura.")

    st.markdown("---")
    st.subheader("Combos Vendidos")
    st.caption("Unidades vendidas por combo (nivel Product Name).")
    col_cf1, col_cf2 = st.columns(2)
    with col_cf1:
        combo_start = st.date_input("Desde", value=pd.to_datetime("2026-01-01").date(), key="ra_combo_start")
    with col_cf2:
        combo_end = st.date_input("Hasta", value=pd.Timestamp.today().date(), key="ra_combo_end")
    combo_sales = fetch_combo_sales(str(combo_start), str(combo_end))
    if combo_sales:
        df_cs = pd.DataFrame(combo_sales)
        st.dataframe(df_cs, use_container_width=True, height=400)
        total_units = df_cs["Unidades Vendidas"].sum() if "Unidades Vendidas" in df_cs.columns else 0
        st.caption(f"Total combos distintos: {len(df_cs)} | Total unidades: {total_units:,}")
    else:
        st.info("Sin combos vendidos en el periodo seleccionado.")


# ================================================================== #
#  PAGE 4: AFILIADOS
# ================================================================== #
def page_afiliados():
    st.header("Detalle de Afiliados")

    cr_monthly = fetch_creator_by_month()
    ct_data = fetch_creator_by_type()

    # Filters row 1
    c1, c2, c3 = st.columns(3)
    with c1:
        ct_opts = ["Todos"]
        if ct_data:
            ct_opts += sorted({r.get("Content Type", "") for r in ct_data if r.get("Content Type")})
        ct_filter = st.selectbox("Tipo de Contenido", ct_opts, key="af_ct")
    with c2:
        all_creators = fetch_top_creators(200)
        cr_opts = ["Todos"] + sorted([r["Creator Username"] for r in all_creators if r.get("Creator Username")]) if all_creators else ["Todos"]
        cr_filter = st.selectbox("Nombre Creador", cr_opts, key="af_creator")
    with c3:
        af_date = st.date_input("Período", value=[], key="af_date")

    # Filters row 2
    c4, c5, c6 = st.columns(3)
    with c4:
        prod_search = st.text_input("Producto", "", key="af_prod")
    with c5:
        order_search = st.text_input("Buscar Orden", "", key="af_order")
    with c6:
        status_opts = ["Todos", "COMPLETED", "CANCELED", "PROCESSING"]
        af_status = st.selectbox("Order Status", status_opts, key="af_status")

    af_date_from = str(af_date[0]) if len(af_date) >= 1 else None
    af_date_to = str(af_date[1]) if len(af_date) == 2 else (str(af_date[0]) if len(af_date) == 1 else None)

    params = {
        "limit": 2000,
        "content_type": ct_filter if ct_filter != "Todos" else None,
        "creator": cr_filter if cr_filter != "Todos" else None,
        "product": prod_search or None,
        "order_id": order_search or None,
        "order_status": af_status if af_status != "Todos" else None,
        "date_from": af_date_from,
        "date_to": af_date_to,
    }
    params = {k: v for k, v in params.items() if v is not None}

    result = api_get("/analytics/affiliates/orders", params) or {"total": 0, "orders": []}
    total = result.get("total", 0)
    orders = result.get("orders", [])

    if not orders:
        st.warning("Sin datos de afiliados para los filtros seleccionados.")
        return

    df = pd.DataFrame(orders)
    df_completed = df[df["Order Status"].astype(str).str.upper() == "COMPLETED"] if "Order Status" in df.columns else df
    df_active = df[~df["Order Status"].astype(str).str.upper().str.contains("CANCEL", na=False)] if "Order Status" in df.columns else df

    c1, c2, c3 = st.columns(3)
    gmv_completado = df_completed["Payment Amount"].sum() if "Payment Amount" in df_completed.columns else 0
    comision_pagada = df_completed["Commission"].sum() if "Commission" in df_completed.columns else 0
    c1.metric("GMV Afiliados (completado)", f"${gmv_completado:,.2f}")
    c2.metric("Comisión Pagada", f"${comision_pagada:,.2f}")
    c3.metric("Órdenes Activas", f"{df_active['Order ID'].nunique():,}" if "Order ID" in df_active.columns else "0")

    st.markdown("---")
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Top 15 Combos Vendidos (Afiliados)")
        if "Product Name" in df.columns:
            top_combos = df.groupby("Product Name").agg(
                Units=("Quantity", "sum"), GMV=("Payment Amount", "sum"),
            ).reset_index().sort_values("Units", ascending=False).head(15)
            fig = px.bar(top_combos, x="Units", y="Product Name", orientation="h",
                         color="GMV", color_continuous_scale="Blues")
            fig.update_layout(height=450, yaxis={"categoryorder": "total ascending"}, margin=dict(t=10))
            st.plotly_chart(fig, use_container_width=True, key="af_combos")
    with col2:
        st.subheader("Top 20 Rendimiento por Creador (Funnel)")
        if "Creator Username" in df.columns and "Payment Amount" in df.columns:
            top_perf = df.groupby("Creator Username").agg(GMV=("Payment Amount", "sum")).reset_index()
            top_perf = top_perf.sort_values("GMV", ascending=False).head(20)
            if not top_perf.empty:
                fig = px.funnel(top_perf, x="GMV", y="Creator Username")
                fig.update_layout(height=450, margin=dict(t=10))
                st.plotly_chart(fig, use_container_width=True, key="af_funnel")

    st.subheader("Detalle de Ventas por Creador")
    detail_cols = ["Order ID", "Creator Username", "Product Name", "Quantity",
                   "Payment Amount", "Content Type", "Commission",
                   "Order Status", "Time Created"]
    available = [c for c in detail_cols if c in df.columns]
    st.dataframe(df[available].head(200), use_container_width=True, height=400)

    st.markdown("---")
    col3, col4 = st.columns(2)
    with col3:
        st.subheader("Ventas por Mes (Afiliados)")
        if cr_monthly:
            df_crm = pd.DataFrame(cr_monthly)
            monthly_agg = df_crm.groupby("Month")["GMV"].sum().reset_index()
            fig = px.bar(monthly_agg, x="Month", y="GMV", text_auto="$.2s")
            fig.update_layout(height=350, margin=dict(t=10))
            st.plotly_chart(fig, use_container_width=True, key="af_monthly")
    with col4:
        st.subheader("Creadores por Tipo de Contenido")
        if ct_data:
            df_ct = pd.DataFrame(ct_data)
            fig = px.bar(df_ct, x="Content Type", y="GMV", color="Content Type", text_auto="$.2s")
            fig.update_layout(height=350, margin=dict(t=10))
            st.plotly_chart(fig, use_container_width=True, key="af_by_type")

    if cr_monthly:
        st.subheader("Top 10 Afiliados con más Productos")
        if "Product Name" in df.columns and "Creator Username" in df.columns:
            aff_prods = df.groupby("Creator Username")["Product Name"].nunique().reset_index()
            aff_prods.columns = ["Creator Username", "Unique Products"]
            aff_prods = aff_prods.sort_values("Unique Products", ascending=False).head(10)
            fig = px.bar(aff_prods, x="Unique Products", y="Creator Username", orientation="h")
            fig.update_layout(height=350, yaxis={"categoryorder": "total ascending"}, margin=dict(t=10))
            st.plotly_chart(fig, use_container_width=True, key="af_top_prods")

        st.subheader("Creadores por Mes (Pivot)")
        df_crm = pd.DataFrame(cr_monthly)
        if not df_crm.empty:
            pivot = df_crm.pivot_table(index="Creator Username", columns="Month",
                                       values="GMV", fill_value=0, aggfunc="sum")
            pivot_fmt = pivot.map(lambda x: f"{int(x):,}" if x != 0 else "—")
            st.dataframe(pivot_fmt, use_container_width=True, height=300)

            top5 = df_crm.groupby("Creator Username")["GMV"].sum().nlargest(5).index
            cr_top5 = df_crm[df_crm["Creator Username"].isin(top5)]
            fig = px.line(cr_top5, x="Month", y="GMV", color="Creator Username", markers=True)
            fig.update_layout(height=350, margin=dict(t=10))
            st.plotly_chart(fig, use_container_width=True, key="af_line_monthly")


# ================================================================== #
#  PAGE 5: FINANCES
# ================================================================== #
def page_finances():
    st.header("Finances")
    data = fetch_finances()
    if not data:
        st.warning("Sin datos.")
        return
    df = pd.DataFrame(data)

    c1, c2, c3 = st.columns(3)
    c1.metric("Total Stock Units", f"{df['StockActualizado'].sum():,.0f}")
    c2.metric("Valor (Coste)", f"${df['ValorInventario'].sum():,.2f}")
    if "PRECIO" in df.columns:
        df["ValorVenta"] = df["StockActualizado"] * df["PRECIO"]
        c3.metric("Valor (Retail)", f"${df['ValorVenta'].sum():,.2f}")

    fin_cols = ["ProductoNombre", "StockActualizado", "PRECIO", "Coste", "ValorInventario"]
    available = [c for c in fin_cols if c in df.columns]
    st.dataframe(df[available].sort_values("ValorInventario", ascending=False),
                 use_container_width=True, height=600)
    csv = df[available].to_csv(index=False).encode("utf-8")
    st.download_button("Download Finances CSV", data=csv, file_name="finances.csv", mime="text/csv")


# ================================================================== #
#  PAGE 6: ORDENES CHECK
# ================================================================== #
def page_ordenes_check():
    st.header("Ordenes Check")

    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        oc_date = st.date_input("Created Time", value=[], key="oc_date")
    with c2:
        oc_order = st.text_input("Order ID", "", key="oc_order")
    with c3:
        oc_sku = st.text_input("SKU ID", "", key="oc_sku")
    with c4:
        oc_seller_sku = st.text_input("Seller SKU", "", key="oc_seller_sku")
    with c5:
        oc_product = st.text_input("Product Name", "", key="oc_product")

    params = {"limit": 500}
    if len(oc_date) >= 1: params["date_from"] = str(oc_date[0])
    if len(oc_date) == 2: params["date_to"] = str(oc_date[1])
    if oc_order: params["order_id"] = oc_order
    if oc_sku: params["sku"] = oc_sku
    if oc_seller_sku: params["seller_sku"] = oc_seller_sku
    if oc_product: params["product_name"] = oc_product

    result = api_get("/analytics/orders", params)
    if not result:
        st.warning("Sin órdenes.")
        return

    orders = result.get("orders", [])
    total = result.get("total", 0)
    st.metric("Total Coincidentes", f"{total:,}")

    if orders:
        df = pd.DataFrame(orders)
        st.subheader("Orders by Status")
        if "status" in df.columns:
            summary = df.groupby("status").agg(
                Count=("order_id", "nunique"),
                Quantity=("quantity", "sum"),
                GMV=("sku_subtotal_after_discount", "sum"),
            ).reset_index()
            st.dataframe(summary, use_container_width=True)

        st.subheader("Order Details")
        detail_cols = ["order_id", "status", "substatus", "product_name",
                       "seller_sku", "quantity", "sku_subtotal_after_discount",
                       "order_amount", "order_date", "shipped_time", "fulfillment_type"]
        available = [c for c in detail_cols if c in df.columns]
        st.dataframe(df[available].head(200), use_container_width=True, height=400)

    st.markdown("---")
    st.subheader("Listado Top Combos")
    combos = fetch_top_combos(20)
    if combos:
        st.dataframe(pd.DataFrame(combos), use_container_width=True, height=400)


# ================================================================== #
#  PAGE 7: CUPONES
# ================================================================== #
def page_cupones():
    st.header("Analisis Cupones")

    buyers_data = fetch_frequent_buyers()
    if not buyers_data:
        st.warning("Sin datos.")
        return

    buyers_df = pd.DataFrame(buyers_data)

    c1, c2 = st.columns(2)
    with c1:
        buyer_opts = ["All"] + sorted(buyers_df["Buyer Username"].dropna().unique().tolist())[:100] if "Buyer Username" in buyers_df.columns else ["All"]
        buyer_filter = st.selectbox("Buyer Username", buyer_opts, key="cup_buyer")
    with c2:
        cup_date = st.date_input("Created Time", value=[], key="cup_date")

    st.subheader("Clientes Frecuentes")
    freq = buyers_df[buyers_df["OrderCount"] > 1].copy()
    if buyer_filter != "All" and "Buyer Username" in freq.columns:
        freq = freq[freq["Buyer Username"] == buyer_filter]
    st.metric("Repeat Customers", f"{len(freq):,}")
    st.dataframe(freq.head(100), use_container_width=True, height=400)

    st.markdown("---")
    st.subheader("Detalle por Orden")
    params = {"limit": 200}
    if len(cup_date) >= 1: params["date_from"] = str(cup_date[0])
    if len(cup_date) == 2: params["date_to"] = str(cup_date[1])
    if buyer_filter != "All": params["buyer"] = buyer_filter
    result = api_get("/analytics/orders", params) or {}
    orders = result.get("orders", [])
    if orders:
        df = pd.DataFrame(orders)
        detail_cols = ["order_id", "buyer_username", "product_name", "quantity",
                       "sku_subtotal_after_discount", "order_amount",
                       "order_date", "status"]
        available = [c for c in detail_cols if c in df.columns]
        st.dataframe(df[available], use_container_width=True, height=400)


# ================================================================== #
#  PAGE 8: FULL DETAIL
# ================================================================== #
def page_full_detail():
    st.header("Full Detail")

    c1, c2, c3, c4 = st.columns(4)
    with c1: fd_buyer = st.text_input("Buyer Username", "", key="fd_buyer")
    with c2: fd_order = st.text_input("Order ID", "", key="fd_order")
    with c3: fd_status = st.text_input("Order Status", "", key="fd_status")
    with c4: fd_product = st.text_input("Product Name", "", key="fd_product")

    c5, c6, c7, c8 = st.columns(4)
    with c5: fd_cancel = st.text_input("Cancel/Return Type", "", key="fd_cancel")
    with c6: fd_city = st.text_input("City", "", key="fd_city")
    with c7: fd_fulfill = st.text_input("Fulfillment Type", "", key="fd_fulfill")
    with c8: fd_recipient = st.text_input("Recipient", "", key="fd_recipient")

    params = {"limit": 500}
    if fd_buyer: params["buyer"] = fd_buyer
    if fd_order: params["order_id"] = fd_order
    if fd_status: params["status"] = fd_status
    if fd_product: params["product_name"] = fd_product
    if fd_cancel: params["cancel_type"] = fd_cancel
    if fd_city: params["city"] = fd_city
    if fd_fulfill: params["fulfillment"] = fd_fulfill
    if fd_recipient: params["recipient"] = fd_recipient

    result = api_get("/analytics/orders", params)
    if not result:
        return

    orders = result.get("orders", [])
    total = result.get("total", 0)
    st.metric("Records", f"{total:,}")

    if orders:
        df = pd.DataFrame(orders)
        st.dataframe(df.head(500), use_container_width=True, height=600)
        csv = df.to_csv(index=False).encode("utf-8")
        st.download_button("Download Filtered Data CSV", data=csv,
                           file_name="full_detail.csv", mime="text/csv")


# ================================================================== #
#  PAGE 9: GESTION INVENTARIO PENDIENTE
# ================================================================== #
def page_gestion_inventario():
    st.header("Gestion Inventario Pendiente")
    st.caption("Edita, agrega o elimina pedidos pendientes.")

    data = fetch_incoming_stock()
    if data:
        df = pd.DataFrame(data)
    else:
        df = pd.DataFrame(columns=["id", "product_id", "qty_ordered", "status",
                                    "supplier", "tracking", "cost", "notes", "order_date"])

    status_options = ["Pendiente", "Recibido", "En transito", "Cancelado", "Ajuste"]

    if "status" in df.columns:
        df["status"] = df["status"].astype(str).str.strip()
        df.loc[df["status"].isin(["nan", "", "None", "pending"]), "status"] = "Pendiente"

    if "order_date" in df.columns:
        df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce").dt.date

    # Filters
    fcol1, fcol2 = st.columns(2)
    with fcol1:
        prod_ids = sorted(df["product_id"].dropna().astype(str).unique().tolist()) if "product_id" in df.columns else []
        filtro_prod = st.selectbox("Filtrar por Producto ID", ["Todos"] + prod_ids, key="filter_prod_inv")
    with fcol2:
        filtro_status = st.selectbox("Filtrar por Status", ["Todos"] + status_options, key="filter_status_inv")

    df_view = df.copy()
    if filtro_prod != "Todos" and "product_id" in df_view.columns:
        df_view = df_view[df_view["product_id"].astype(str) == filtro_prod]
    if filtro_status != "Todos" and "status" in df_view.columns:
        df_view = df_view[df_view["status"] == filtro_status]

    st.subheader("Pedidos actuales")
    edited = st.data_editor(
        df_view, num_rows="dynamic",
        column_config={
            "status": st.column_config.SelectboxColumn("Status", options=status_options, default="pending"),
            "qty_ordered": st.column_config.NumberColumn("Qty Ordered", min_value=0),
            "cost": st.column_config.NumberColumn("Cost", min_value=0, format="$%.2f"),
            "order_date": st.column_config.DateColumn("Fecha pedido"),
        },
        use_container_width=True, height=500, key="inv_editor",
        disabled=["id", "store_id", "product_id"],
    )

    col_save, col_info = st.columns([1, 3])
    with col_save:
        if st.button("Guardar cambios", type="primary", key="save_pending"):
            saved, errors = 0, 0
            for _, row in edited.iterrows():
                record_id = row.get("id")
                if record_id and pd.notna(record_id):
                    update_data = {
                        "qty_ordered": int(row.get("qty_ordered", 0) or 0),
                        "status": row.get("status", "pending"),
                        "supplier": row.get("supplier") if pd.notna(row.get("supplier", None)) else None,
                        "tracking": row.get("tracking") if pd.notna(row.get("tracking", None)) else None,
                        "cost": float(row["cost"]) if pd.notna(row.get("cost")) else None,
                        "notes": row.get("notes") if pd.notna(row.get("notes", None)) else None,
                    }
                    result = api_put(f"/inventory/incoming/{record_id}", update_data)
                    if result:
                        saved += 1
                    else:
                        errors += 1
            st.success(f"Guardado: {saved} registros.")
            if errors:
                st.warning(f"{errors} errores al guardar.")
            st.cache_data.clear()
            st.rerun()
    with col_info:
        if not df.empty and "status" in df.columns:
            n_p = len(df[df["status"] == "Pendiente"])
            n_r = len(df[df["status"] == "Recibido"])
            n_t = len(df[df["status"] == "En transito"])
            total_v = len(df_view)
            total_a = len(df)
            label = f"Mostrando {total_v} de {total_a}" if (filtro_prod != "Todos" or filtro_status != "Todos") else f"Total: {total_a}"
            st.info(f"Pending: {n_p} | Recibido: {n_r} | En transito: {n_t} | {label}")


# ================================================================== #
#  PAGE 10: LISTADO PRODUCTOS
# ================================================================== #
def page_listado_productos():
    st.header("Listado de Productos")
    st.caption("Catálogo de productos. Edita o agrega productos.")

    products = fetch_products()
    if products:
        df = pd.DataFrame(products)
    else:
        df = pd.DataFrame(columns=["id", "sku", "name", "category", "price_cost",
                                    "price_sale", "units_per_box", "supplier", "status"])

    if "status" in df.columns:
        df["status"] = df["status"].fillna("active").astype(str).str.strip()
        df.loc[df["status"].isin(["nan", "", "None"]), "status"] = "active"

    for col in ["price_cost", "price_sale"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "units_per_box" in df.columns:
        df["units_per_box"] = pd.to_numeric(df["units_per_box"], errors="coerce").fillna(1).astype(int)

    edited = st.data_editor(
        df, num_rows="dynamic",
        column_config={
            "price_cost": st.column_config.NumberColumn("Coste", min_value=0, format="$%.2f"),
            "price_sale": st.column_config.NumberColumn("Precio", min_value=0, format="$%.2f"),
            "units_per_box": st.column_config.NumberColumn("Unid/Caja", min_value=0),
            "status": st.column_config.SelectboxColumn("Status", options=["active", "inactive"], default="active"),
        },
        disabled=["id", "store_id", "created_at", "updated_at"],
        use_container_width=True,
        height=500,
        key="productos_editor",
    )

    col_save, col_info = st.columns([1, 3])
    with col_save:
        if st.button("Guardar productos", type="primary", key="save_productos"):
            saved, created, errors = 0, 0, 0
            for _, row in edited.iterrows():
                product_id = row.get("id")
                if product_id and pd.notna(product_id) and str(product_id).strip():
                    update_data = {}
                    for field in ["name", "category", "price_cost", "price_sale",
                                  "units_per_box", "supplier", "status"]:
                        val = row.get(field)
                        if val is not None and pd.notna(val):
                            update_data[field] = val
                    if update_data:
                        result = api_put(f"/products/{product_id}", update_data)
                        if result:
                            saved += 1
                        else:
                            errors += 1
                else:
                    sku = row.get("sku")
                    name = row.get("name")
                    if sku and name and pd.notna(sku) and pd.notna(name):
                        new_data = {
                            "sku": str(sku).strip(),
                            "name": str(name).strip(),
                            "category": row.get("category") if pd.notna(row.get("category", None)) else None,
                            "price_cost": float(row["price_cost"]) if pd.notna(row.get("price_cost")) else None,
                            "price_sale": float(row["price_sale"]) if pd.notna(row.get("price_sale")) else None,
                            "units_per_box": int(row["units_per_box"]) if pd.notna(row.get("units_per_box")) else None,
                            "supplier": row.get("supplier") if pd.notna(row.get("supplier", None)) else None,
                        }
                        result = api_post("/products", new_data)
                        if result:
                            created += 1
                        else:
                            errors += 1
            st.success(f"Guardado: {saved} actualizados, {created} creados.")
            if errors:
                st.warning(f"{errors} errores.")
            st.cache_data.clear()
            st.rerun()
    with col_info:
        st.info(f"Total productos: {len(edited)}")


# ================================================================== #
#  PAGE 11: GESTION COMBOS
# ================================================================== #
def page_gestion_combos():
    st.header("Gestion Combos")
    st.caption("SKUs sin combo asignado y editor de combos.")

    unknown = fetch_unknown_combos()
    if unknown:
        n = len(unknown)
        st.warning(f"{n} SKU(s) pendientes de asignar combo")
        st.subheader("SKUs sin asignar")
        st.dataframe(pd.DataFrame(unknown), use_container_width=True, height=min(300, 50 + 35 * n))

        # Quick-add form
        st.subheader("Asignar combo nuevo")
        products_data = fetch_products()
        product_map = {p["name"]: p["id"] for p in products_data} if products_data else {}
        sku_options = [r.get("seller_sku", "") for r in unknown if r.get("seller_sku")]

        selected_sku = st.selectbox("SKU a asignar", [""] + sku_options, key="combo_assign_sku")
        if selected_sku:
            row_data = next((r for r in unknown if r.get("seller_sku") == selected_sku), {})
            st.caption(f"Producto: {row_data.get('product_name', 'N/A')} | Órdenes: {row_data.get('order_count', 0)}")

            n_products = st.number_input("¿Cuántos productos tiene este combo?",
                                          min_value=1, max_value=12, value=1, key="combo_n_prods")
            selected_products = []
            prod_cols = st.columns(min(int(n_products), 4))
            for i in range(int(n_products)):
                with prod_cols[i % len(prod_cols)]:
                    p = st.selectbox(f"Producto {i+1}", [""] + sorted(product_map.keys()),
                                     key=f"combo_prod_{i}")
                    selected_products.append(p)

            if st.button("Agregar combo y guardar", type="primary", key="btn_add_combo"):
                valid = [p for p in selected_products if p]
                if not valid:
                    st.error("Selecciona al menos un producto.")
                else:
                    items = [{"product_id": product_map[p], "quantity": 1} for p in valid if p in product_map]
                    unknown_row = next((r for r in unknown if r.get("seller_sku") == selected_sku), {})
                    result = api_post("/combos", {
                        "combo_sku": selected_sku,
                        "combo_name": unknown_row.get("product_name", selected_sku),
                        "items": items,
                    })
                    if result:
                        st.success(f"Combo '{selected_sku}' creado con {len(items)} productos.")
                        st.cache_data.clear()
                        st.rerun()
                    else:
                        st.error("Error creando combo.")
    else:
        st.success("Todos los SKUs tienen combo asignado o son productos conocidos.")

    st.markdown("---")
    st.subheader("Listado de Combos")
    combos = fetch_combos()
    if combos:
        combo_df = pd.DataFrame(combos)
        st.dataframe(combo_df, use_container_width=True, height=400)
        st.info(f"Total combos definidos: {len(combo_df)}")

    st.markdown("---")
    st.subheader("Agregar Combo Manualmente")
    products_data = fetch_products()
    product_map = {p["name"]: p["id"] for p in products_data} if products_data else {}

    with st.form("add_combo_form"):
        new_sku = st.text_input("Combo SKU (Seller SKU)")
        new_name = st.text_input("Combo Name")
        new_items_text = st.text_area(
            "Productos (uno por linea: nombre_producto,cantidad)",
            placeholder="Producto A,1\nProducto B,2",
        )
        if st.form_submit_button("Agregar Combo"):
            if new_sku and new_name and new_items_text:
                items = []
                errors_list = []
                for line in new_items_text.strip().split("\n"):
                    parts = line.strip().split(",")
                    pname = parts[0].strip()
                    qty = int(parts[1].strip()) if len(parts) >= 2 else 1
                    pid = product_map.get(pname)
                    if pid:
                        items.append({"product_id": pid, "quantity": qty})
                    else:
                        errors_list.append(pname)
                if errors_list:
                    st.error(f"Productos no encontrados: {', '.join(errors_list)}")
                elif items:
                    result = api_post("/combos", {"combo_sku": new_sku, "combo_name": new_name, "items": items})
                    if result:
                        st.success(f"Combo '{new_sku}' creado.")
                        st.cache_data.clear()
                        st.rerun()
                    else:
                        st.error("Error creando combo.")
            else:
                st.warning("Completa todos los campos.")


# ================================================================== #
#  PAGE 12: INVENTARIO FBT
# ================================================================== #
def page_inventario_fbt():
    st.header("Gestión Inventario FBT")
    st.caption("Productos enviados al almacén de TikTok (FBT). Edita, agrega o elimina envíos.")

    data = fetch_fbt_inventory()
    if data:
        df = pd.DataFrame(data)
    else:
        df = pd.DataFrame(columns=["id", "goods_code", "goods_name", "total_units", "fecha_envio"])

    if "fecha_envio" in df.columns:
        df["fecha_envio"] = pd.to_datetime(df["fecha_envio"], errors="coerce").dt.date

    st.subheader("Envíos a FBT")
    edited = st.data_editor(
        df, num_rows="dynamic",
        column_config={
            "goods_code": st.column_config.TextColumn("SKU"),
            "goods_name": st.column_config.TextColumn("Producto"),
            "total_units": st.column_config.NumberColumn("Unidades", min_value=0),
            "fecha_envio": st.column_config.DateColumn("Fecha Envio"),
        },
        disabled=["id", "store_id"],
        use_container_width=True,
        height=400,
        key="fbt_inv_editor",
    )

    col_save, col_info = st.columns([1, 3])
    with col_save:
        if st.button("Guardar cambios", type="primary", key="save_fbt"):
            saved, created, errors = 0, 0, 0
            for _, row in edited.iterrows():
                record_id = row.get("id")
                if record_id and pd.notna(record_id) and str(record_id).strip():
                    update_data = {
                        "goods_code": row.get("goods_code") if pd.notna(row.get("goods_code", None)) else None,
                        "goods_name": row.get("goods_name") if pd.notna(row.get("goods_name", None)) else None,
                        "total_units": int(row.get("total_units", 0) or 0),
                        "fecha_envio": str(row["fecha_envio"]) if pd.notna(row.get("fecha_envio")) else None,
                    }
                    result = api_put(f"/inventory/fbt/{record_id}", update_data)
                    if result:
                        saved += 1
                    else:
                        errors += 1
                else:
                    goods_code = row.get("goods_code")
                    if goods_code and pd.notna(goods_code):
                        new_data = {
                            "goods_code": str(goods_code).strip(),
                            "goods_name": str(row.get("goods_name", "")).strip() or None,
                            "total_units": int(row.get("total_units", 0) or 0),
                            "fecha_envio": str(row["fecha_envio"]) if pd.notna(row.get("fecha_envio")) else None,
                        }
                        result = api_post("/inventory/fbt", new_data)
                        if result:
                            created += 1
                        else:
                            errors += 1
            st.success(f"Guardado: {saved} actualizados, {created} creados.")
            if errors:
                st.warning(f"{errors} errores.")
            st.cache_data.clear()
            st.rerun()
    with col_info:
        if not edited.empty and "total_units" in edited.columns:
            total = edited["total_units"].sum()
            n_skus = len(edited)
            st.info(f"SKUs: {n_skus} | Total unidades enviadas: {total:,.0f}")


# ================================================================== #
#  LOGIN + MAIN
# ================================================================== #
def login_page():
    st.title("Rodmat Dashboard V2")
    tab1, tab2 = st.tabs(["Login", "Register"])

    with tab1:
        with st.form("login_form"):
            email = st.text_input("Email")
            password = st.text_input("Password", type="password")
            if st.form_submit_button("Login"):
                if login(email, password):
                    st.rerun()
                else:
                    st.error("Email o contraseña incorrectos")

    with tab2:
        with st.form("register_form"):
            store_name = st.text_input("Store Name")
            email = st.text_input("Email", key="reg_email")
            password = st.text_input("Password", type="password", key="reg_pass")
            if st.form_submit_button("Register"):
                if store_name and email and password:
                    if register(email, password, store_name):
                        st.rerun()
                else:
                    st.warning("Completa todos los campos")


# ================================================================== #
#  FINANCE SECTION PAGES
# ================================================================== #

def page_finance_dashboard():
    st.header("Finance Dashboard")
    data = api_get("/finance/dashboard")
    if data is None:
        return
    if not data.get("recent_transactions"):
        st.info("No hay transacciones. Ve a Finance → Management → Importar Nuevas para empezar.")
        return

    bal = data.get("balance_actual", 0)
    ing = data.get("ingresos_mes", 0)
    gas = data.get("gastos_mes", 0)
    net = data.get("net_mes", 0)
    ing_prev = data.get("ingresos_prev", 0)
    gas_prev = data.get("gastos_prev", 0)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Balance Actual", f"${bal:,.2f}")
    c2.metric("Ingresos Mes", f"${ing:,.2f}", delta=f"{ing - ing_prev:+,.0f}" if ing_prev else None)
    c3.metric("Gastos Mes", f"${gas:,.2f}", delta=f"{gas - gas_prev:+,.0f}" if gas_prev else None, delta_color="inverse")
    c4.metric("Net Mes", f"${net:,.2f}", delta_color="normal" if net >= 0 else "inverse")

    st.markdown("---")

    monthly_raw = data.get("monthly_cashflow", [])
    top_cat = data.get("top_gastos_cat", [])

    col_left, col_right = st.columns(2)
    with col_left:
        st.subheader("Cash Flow Mensual")
        if monthly_raw:
            df_m = pd.DataFrame(monthly_raw)
            fig = go.Figure()
            bal_rows = df_m[df_m["tipo"] == "Balance"] if "tipo" in df_m.columns else pd.DataFrame()
            gas_rows = df_m[df_m["tipo"] == "Gastos"] if "tipo" in df_m.columns else pd.DataFrame()
            if not bal_rows.empty:
                fig.add_trace(go.Bar(name="Ingresos", x=bal_rows["month"], y=bal_rows["amount"], marker_color="#27ae60"))
            if not gas_rows.empty:
                fig.add_trace(go.Bar(name="Gastos", x=gas_rows["month"], y=gas_rows["amount"].abs(), marker_color="#e74c3c"))
            fig.update_layout(barmode="group", height=350, margin=dict(t=20))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Sin datos de cash flow.")

    with col_right:
        st.subheader("Top Gastos por Categoría (mes actual)")
        if top_cat:
            df_cat = pd.DataFrame(top_cat)
            fig_pie = px.pie(df_cat, values="amount", names="clasificacion", height=350)
            fig_pie.update_layout(margin=dict(t=20))
            st.plotly_chart(fig_pie, use_container_width=True)
        else:
            st.info("Sin gastos clasificados este mes.")

    pivot_raw = data.get("pivot_6m", [])
    if pivot_raw:
        st.subheader("Resumen por Clasificación (últimos 6 meses)")
        st.dataframe(pd.DataFrame(pivot_raw), use_container_width=True, hide_index=True)

    recent = data.get("recent_transactions", [])
    if recent:
        st.subheader("Últimas 50 transacciones")
        st.dataframe(pd.DataFrame(recent), use_container_width=True, hide_index=True)


def page_finance_gestion():
    st.header("Gestión de Transacciones")

    clasif_map = api_get("/finance/classifications") or {}
    all_clasif = sorted(set(c for lst in clasif_map.values() for c in lst))
    tipos_list = ["Balance", "Gastos", "Pendiente"]

    pend_data = api_get("/finance/pending-count") or {}
    n_pending = pend_data.get("count", 0)
    if n_pending > 0:
        st.info(f"{n_pending} transacciones pendientes de clasificar.")

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        fecha_desde = st.date_input("Desde", value=None, key="fg_desde")
    with col2:
        fecha_hasta = st.date_input("Hasta", value=None, key="fg_hasta")
    with col3:
        tipo_filter = st.selectbox("Tipo", ["Todos"] + tipos_list, key="fg_tipo")
    with col4:
        estado_filter = st.selectbox("Estado", ["Todas", "Pendientes", "Auto-clasificadas", "Manuales"], key="fg_estado")

    params: dict = {"tipo": tipo_filter, "estado": estado_filter, "limit": 500}
    if fecha_desde:
        params["date_from"] = str(fecha_desde)
    if fecha_hasta:
        params["date_to"] = str(fecha_hasta)

    txs = api_get("/finance/transactions", params=params) or []

    if not txs:
        st.info("Sin transacciones con esos filtros.")
        return

    st.caption(f"{len(txs)} transacciones")

    df = pd.DataFrame(txs)
    edited = st.data_editor(
        df[["id", "date", "description", "amount", "tipo", "clasificacion",
            "classification_method", "classification_confidence", "is_pending_review"]],
        column_config={
            "id": st.column_config.TextColumn("ID", disabled=True, width="small"),
            "date": st.column_config.TextColumn("Fecha", disabled=True, width="small"),
            "description": st.column_config.TextColumn("Descripción", disabled=True, width="large"),
            "amount": st.column_config.NumberColumn("Monto", format="$%.2f", disabled=True),
            "tipo": st.column_config.SelectboxColumn("Tipo", options=tipos_list),
            "clasificacion": st.column_config.SelectboxColumn("Clasificación", options=all_clasif),
            "classification_method": st.column_config.TextColumn("Método", disabled=True, width="small"),
            "classification_confidence": st.column_config.NumberColumn("Conf.", format="%.0%%", disabled=True, width="small"),
            "is_pending_review": st.column_config.CheckboxColumn("Pendiente", disabled=True, width="small"),
        },
        use_container_width=True,
        hide_index=True,
        num_rows="fixed",
        key="finance_gestion_editor",
    )

    if st.button("Guardar cambios", type="primary"):
        saved = 0
        for _, row in edited.iterrows():
            orig = next((t for t in txs if t["id"] == row["id"]), None)
            if orig and (orig["tipo"] != row["tipo"] or orig["clasificacion"] != row["clasificacion"]):
                api_patch(f"/finance/transactions/{row['id']}", {
                    "tipo": row["tipo"],
                    "clasificacion": row["clasificacion"],
                })
                saved += 1
        if saved:
            st.success(f"{saved} transacciones actualizadas.")
            st.rerun()
        else:
            st.info("Sin cambios para guardar.")


def page_finance_insights():
    st.header("Finance Insights")
    data = api_get("/finance/insights")
    if data is None:
        return
    if data.get("burn_rate", 0) == 0 and data.get("ingresos_tiktok", 0) == 0:
        st.info("Importa transacciones bancarias para ver los insights financieros.")
        return

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Ingresos TikTok Total", f"${data.get('ingresos_tiktok', 0):,.0f}")
    c2.metric("Burn Rate Mensual", f"${data.get('burn_rate', 0):,.0f}")
    c3.metric("Runway estimado", f"{data.get('runway_months', 0):.1f} meses")
    c4.metric("Margen neto banco", f"{data.get('margen_pct', 0):.1f}%")

    st.markdown("---")
    st.subheader("TikTok Facturado vs Cobrado en Banco")
    cxc = data.get("cxc_cobrado", 0)
    tiktok = data.get("ingresos_tiktok", 0)
    gap = data.get("gap_tiktok_banco", 0)
    col1, col2, col3 = st.columns(3)
    col1.metric("Facturado TikTok", f"${tiktok:,.0f}")
    col2.metric("Cobrado en banco (CxC)", f"${cxc:,.0f}")
    col3.metric("Diferencia", f"${gap:,.0f}", delta_color="inverse" if gap > 0 else "normal")

    st.markdown("---")
    alertas = data.get("alertas", [])
    st.subheader("Alertas de Gastos Anormales (vs mes anterior)")
    if alertas:
        st.dataframe(pd.DataFrame(alertas), use_container_width=True, hide_index=True)
    else:
        st.success("Sin variaciones mayores al 50% entre meses.")

    recurrentes = data.get("recurrentes", [])
    if recurrentes:
        st.subheader("Top Gastos Recurrentes")
        st.dataframe(pd.DataFrame(recurrentes), use_container_width=True, hide_index=True)


def page_finance_management():
    st.header("Finance Management")

    clasif_map = api_get("/finance/classifications") or {}
    all_clasif = sorted(set(c for lst in clasif_map.values() for c in lst))
    tipos_list = [t for t in clasif_map.keys() if t != "Pendiente"] + ["Pendiente"]

    tab_import, tab_pending, tab_tools, tab_catalog = st.tabs(
        ["📥 Importar Nuevas", "📋 Revisar Pendientes", "⚙️ Herramientas", "🗂️ Plantilla"]
    )

    # ---- TAB 1: IMPORTAR ----
    with tab_import:
        st.subheader("Importar Nuevas Transacciones")
        st.markdown("**Paso 1 — Selecciona archivo**")
        uploaded = st.file_uploader(
            "Archivo del banco (.xlsx, .xls, .csv)",
            type=["xlsx", "xls", "csv"],
            key="finance_uploader",
        )

        if not uploaded:
            st.info("Sube un archivo Excel o CSV exportado desde el banco.")
        else:
            if st.button("Analizar archivo", type="secondary", key="btn_preview"):
                with st.spinner("Analizando..."):
                    files = {"file": (uploaded.name, uploaded.getvalue(), "application/octet-stream")}
                    preview = api_post("/finance/preview", files=files)
                    if preview:
                        st.session_state["finance_preview"] = preview
                        st.session_state["finance_preview_file"] = uploaded.name

            preview = st.session_state.get("finance_preview")
            if preview:
                detected = preview.get("detected", [])
                if detected:
                    st.markdown("**Paso 2 — Columnas detectadas**")
                    st.dataframe(pd.DataFrame(detected), use_container_width=True, hide_index=True)

                if not preview.get("ok"):
                    missing = preview.get("missing", [])
                    st.error(f"Faltan columnas obligatorias: **{', '.join(missing)}**.")
                else:
                    st.success("Todas las columnas obligatorias detectadas.")
                    rows = preview.get("rows", [])
                    if rows:
                        n_auto = sum(1 for r in rows if r.get("tipo") != "Pendiente")
                        n_pend = len(rows) - n_auto
                        cm1, cm2 = st.columns(2)
                        cm1.metric("Se auto-clasificarán ✅", n_auto)
                        cm2.metric("Quedarán pendientes ⚠️", n_pend)

                        st.markdown("**Paso 3 — Revisa y edita antes de importar**")
                        df_prev = pd.DataFrame(rows)[["fecha", "description", "amount", "tipo", "clasificacion", "confidence"]]
                        edited_prev = st.data_editor(
                            df_prev,
                            column_config={
                                "fecha": st.column_config.TextColumn("Fecha", disabled=True, width="small"),
                                "description": st.column_config.TextColumn("Descripción", disabled=True, width="large"),
                                "amount": st.column_config.NumberColumn("Monto", format="$%.2f", disabled=True, width="small"),
                                "tipo": st.column_config.SelectboxColumn("Tipo", options=tipos_list, width="small"),
                                "clasificacion": st.column_config.SelectboxColumn("Clasificación", options=all_clasif, width="medium"),
                                "confidence": st.column_config.NumberColumn("Conf.", format="%.0%%", disabled=True, width="small"),
                            },
                            use_container_width=True,
                            hide_index=True,
                            num_rows="fixed",
                            key="finance_import_preview",
                        )

                        st.markdown("---")
                        if st.button("Importar y Confirmar", type="primary", key="btn_import"):
                            full_rows = preview.get("rows", [])
                            import_payload = []
                            for i, r in enumerate(full_rows):
                                import_payload.append({
                                    "fecha": r["fecha"],
                                    "description": r["description"],
                                    "amount": r["amount"],
                                    "running_balance": r.get("running_balance"),
                                    "tipo": edited_prev.iloc[i]["tipo"] if i < len(edited_prev) else r["tipo"],
                                    "clasificacion": edited_prev.iloc[i]["clasificacion"] if i < len(edited_prev) else r["clasificacion"],
                                })
                            with st.spinner("Importando..."):
                                result = api_post("/finance/import", json_data=import_payload)
                            if result:
                                st.success(
                                    f"Importación completa: **{result['added']}** nuevas | "
                                    f"**{result['duplicates']}** duplicadas | "
                                    f"**{result['pending']}** pendientes de revisión"
                                )
                                st.session_state.pop("finance_preview", None)
                                if result["pending"] > 0:
                                    st.info("Ve a **Revisar Pendientes** para clasificarlas.")
                                st.rerun()

    # ---- TAB 2: REVISAR PENDIENTES ----
    with tab_pending:
        pend = api_get("/finance/transactions", params={"estado": "Pendientes", "limit": 200}) or []
        if not pend:
            st.success("No hay transacciones pendientes de clasificar.")
        else:
            st.info(f"{len(pend)} transacciones pendientes")
            col_btn, _ = st.columns([1, 4])
            with col_btn:
                if st.button("Auto-clasificar todas", type="secondary", key="btn_auto_all"):
                    with st.spinner("Clasificando..."):
                        result = api_post("/finance/reclassify-pending")
                    if result:
                        st.success(f"{result['classified']} clasificadas. {result['still_pending']} siguen pendientes.")
                        st.rerun()

            st.markdown("---")
            for row in pend[:50]:
                with st.container():
                    cols = st.columns([3, 1, 2, 2, 1])
                    cols[0].markdown(f"**{str(row['description'])[:80]}**")
                    cols[1].markdown(f"${float(row['amount']):,.2f}")
                    tipo_sel = cols[2].selectbox(
                        "Tipo", tipos_list, key=f"tp_{row['id']}",
                        label_visibility="collapsed",
                    )
                    clasif_sel = cols[3].selectbox(
                        "Clasificación", all_clasif, key=f"cl_{row['id']}",
                        label_visibility="collapsed",
                    )
                    if cols[4].button("✓", key=f"ok_{row['id']}"):
                        api_patch(f"/finance/transactions/{row['id']}", {
                            "tipo": tipo_sel,
                            "clasificacion": clasif_sel,
                        })
                        st.rerun()
                    st.divider()

    # ---- TAB 3: HERRAMIENTAS ----
    with tab_tools:
        st.subheader("Herramientas")

        st.markdown("### Re-clasificar pendientes con reglas")
        pend_count = (api_get("/finance/pending-count") or {}).get("count", 0)
        st.write(f"Transacciones pendientes: **{pend_count}**")
        if st.button("Re-clasificar con reglas", type="secondary", key="btn_reclasify"):
            with st.spinner("Clasificando..."):
                result = api_post("/finance/reclassify-pending")
            if result:
                st.success(f"{result['classified']} clasificadas. {result['still_pending']} siguen pendientes.")
                st.rerun()

        st.markdown("---")
        st.markdown("### Corrección de fechas (banco americano)")
        st.info("Si los datos tienen día y mes invertidos (MM/DD/YYYY), corrige los registros con fechas en el futuro.")
        if st.button("Corregir fechas invertidas", key="btn_fix_dates"):
            with st.spinner("Corrigiendo..."):
                result = api_post("/finance/fix-dates")
            if result:
                fixed = result.get("fixed", 0)
                if fixed:
                    st.success(f"{fixed} fechas corregidas.")
                    st.rerun()
                else:
                    st.info("No se encontraron fechas futuras para corregir.")

    # ---- TAB 4: PLANTILLA ----
    with tab_catalog:
        st.subheader("Plantilla: Tipos y Clasificaciones")
        st.info("Define los tipos y clasificaciones disponibles en todo el módulo Finance.")

        for tipo_name, clasif_list in list(clasif_map.items()):
            with st.expander(f"**{tipo_name}** — {len(clasif_list)} clasificaciones", expanded=False):
                for i, cname in enumerate(list(clasif_list)):
                    c1, c2 = st.columns([5, 1])
                    c1.text(cname)
                    if c2.button("🗑", key=f"del_{tipo_name}_{i}"):
                        clasif_map[tipo_name] = [x for x in clasif_map[tipo_name] if x != cname]
                        api_put("/finance/classifications", clasif_map)
                        st.rerun()

                nc1, nc2 = st.columns([4, 1])
                new_c = nc1.text_input("Nueva clasificación", key=f"new_c_{tipo_name}", label_visibility="collapsed", placeholder="Ej: Publicidad digital")
                if nc2.button("Añadir", key=f"add_{tipo_name}"):
                    v = new_c.strip()
                    if v and v not in clasif_map[tipo_name]:
                        clasif_map[tipo_name].append(v)
                        clasif_map[tipo_name].sort()
                        api_put("/finance/classifications", clasif_map)
                        st.rerun()

        st.markdown("---")
        st.markdown("**Crear nuevo tipo:**")
        nt1, nt2 = st.columns([4, 1])
        new_tipo = nt1.text_input("Nombre del tipo", key="new_tipo_input", label_visibility="collapsed", placeholder="Ej: Inversiones")
        if nt2.button("Crear", key="btn_new_tipo"):
            v = new_tipo.strip()
            if v and v not in clasif_map:
                clasif_map[v] = []
                api_put("/finance/classifications", clasif_map)
                st.rerun()


# ================================================================== #
#  MAIN
# ================================================================== #
def main():
    if "jwt_token" not in st.session_state:
        login_page()
        return

    user = api_get("/auth/me")
    if not user:
        st.session_state.pop("jwt_token", None)
        login_page()
        return

    st.title("Rodmat Dashboard V2")

    with st.sidebar:
        st.write(f"**{user.get('email', '')}**")
        store_name = user.get("store_name", user.get("store_id", "")[:8])
        st.caption(f"Tienda: {store_name}")
        if st.button("Actualizar Datos"):
            api_post("/analytics/clear-cache")
            st.cache_data.clear()
            st.rerun()
        if st.button("Cerrar Sesión"):
            st.session_state.pop("jwt_token", None)
            st.cache_data.clear()
            st.rerun()
        st.markdown("---")
        st.caption("v2.2.0 | Rodmat Dashboard")

    _role = user.get("role", "viewer")
    _sections = ["Dashboard", "Gestion", "Finance"] if _role in ("admin", "superadmin") else ["Dashboard", "Gestion"]
    section = st.sidebar.radio("Sección", _sections, index=0)

    if section == "Dashboard":
        tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8 = st.tabs([
            "Resumen General", "Inventario Summary", "Restock Analysis", "Afiliados",
            "Finances", "Ordenes Check", "Cupones", "Full Detail",
        ])
        with tab1: page_overview()
        with tab2: page_inventario_summary()
        with tab3: page_restock_analysis()
        with tab4: page_afiliados()
        with tab5: page_finances()
        with tab6: page_ordenes_check()
        with tab7: page_cupones()
        with tab8: page_full_detail()

    elif section == "Gestion":
        tab_g1, tab_g2, tab_g3, tab_g4 = st.tabs([
            "Inventario Pendiente", "Listado Productos", "Gestion Combos", "Inventario FBT",
        ])
        with tab_g1: page_gestion_inventario()
        with tab_g2: page_listado_productos()
        with tab_g3: page_gestion_combos()
        with tab_g4: page_inventario_fbt()

    elif section == "Finance":
        tab_f1, tab_f2, tab_f3, tab_f4 = st.tabs(
            ["Dashboard", "Gestion", "Insights", "Management"]
        )
        with tab_f1: page_finance_dashboard()
        with tab_f2: page_finance_gestion()
        with tab_f3: page_finance_insights()
        with tab_f4: page_finance_management()


if __name__ == "__main__":
    main()
