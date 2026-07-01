"""
Multi-tenant Streamlit Dashboard — title and modules are rendered
per-tenant based on /api/auth/me (store_name + modules_enabled).
"""
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go

from api_client import api_get, api_post, api_put, api_patch, api_delete, login, register

st.set_page_config(
    page_title="Dashboard",
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
def fetch_overview(date_from=None, date_to=None, platform=None):
    params = {}
    if date_from: params["date_from"] = str(date_from)
    if date_to: params["date_to"] = str(date_to)
    if platform: params["platform"] = platform
    return api_get("/analytics/overview", params) or {}

@st.cache_data(ttl=300)
def fetch_sales_by_month(date_from=None, date_to=None, platform=None):
    params = {}
    if date_from: params["date_from"] = str(date_from)
    if date_to: params["date_to"] = str(date_to)
    if platform: params["platform"] = platform
    return api_get("/analytics/sales-by-month", params) or []

@st.cache_data(ttl=300)
def fetch_sales_by_day(date_from=None, date_to=None, platform=None):
    params = {}
    if date_from: params["date_from"] = str(date_from)
    if date_to: params["date_to"] = str(date_to)
    if platform: params["platform"] = platform
    return api_get("/analytics/sales-by-day", params) or []

@st.cache_data(ttl=300)
def fetch_platform_summary(date_from=None, date_to=None):
    params = {}
    if date_from: params["date_from"] = str(date_from)
    if date_to: params["date_to"] = str(date_to)
    return api_get("/analytics/platform-summary", params) or {}

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


@st.cache_data(ttl=60)
def fetch_sku_maps(platform: str = "all"):
    """Devuelve mapeos walmart_sku_map + amazon_sku_map unificados."""
    return api_get("/sku-maps", {"platform": platform}) or []

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

@st.cache_data(ttl=300)
def fetch_viral_alerts(threshold=20, days=5):
    return api_get("/analytics/viral-alerts", {"threshold": threshold, "days": days}) or []

@st.cache_data(ttl=300)
def fetch_creator_own_orders():
    return api_get("/analytics/creator-own-orders") or []

@st.cache_data(ttl=300)
def fetch_pallet_orders():
    return api_get("/analytics/pallet-orders") or []



# ================================================================== #
#  Per-tenant enabled platforms (defaults to all if not configured)
# ================================================================== #
ALL_PLATFORMS = ["tiktok", "amazon", "walmart"]

def get_enabled_platforms() -> list[str]:
    """Returns list of platform keys enabled for the active tenant.
    Empty config / unauthenticated user => all platforms allowed (legacy default)."""
    u = st.session_state.get("cached_user") or {}
    pe = u.get("platforms_enabled")
    if pe is None:
        return list(ALL_PLATFORMS)
    if not isinstance(pe, list):
        return list(ALL_PLATFORMS)
    # Keep only valid keys, preserve order matching ALL_PLATFORMS
    out = [p for p in ALL_PLATFORMS if p in pe]
    return out or list(ALL_PLATFORMS)

# ================================================================== #
#  PLATFORM SELECTOR — shown at top of every Dashboard page
# ================================================================== #
_PS = {
    None:     {"bg": "#4f46e5", "text": "white",   "emoji": "🌐", "label": "Todas las plataformas"},
    "tiktok": {"bg": "#010101", "text": "white",   "emoji": "🎵", "label": "TikTok Shop"},
    "amazon": {"bg": "#FF9900", "text": "#232F3E", "emoji": "🛒", "label": "Amazon"},
    "walmart": {"bg": "#0071CE", "text": "#FFC220", "emoji": "🏬", "label": "Walmart"},
}

def render_platform_selector(page_key: str) -> str | None:
    enabled = get_enabled_platforms()
    # Single-platform tenant: lock to that platform, render badge only.
    if len(enabled) == 1:
        only = enabled[0]
        if st.session_state.get("platform_filter") != only:
            st.session_state["platform_filter"] = only
        sty = _PS.get(only, _PS[None])
        st.markdown(f"""
        <div style="background:{sty['bg']};color:{sty['text']};padding:8px 16px;border-radius:8px;
             margin-bottom:8px;font-weight:700;font-size:14px;letter-spacing:0.4px;
             box-shadow:0 2px 6px rgba(0,0,0,.25);">
          {sty['emoji']} &nbsp; CANAL: {sty['label']}
        </div>""", unsafe_allow_html=True)
        return only

    # Multi-platform tenant: show "Todos" + one button per enabled platform.
    p = st.session_state.get("platform_filter")
    if p is not None and p not in enabled:
        # User had a filter selected that the tenant no longer has access to.
        st.session_state["platform_filter"] = None
        p = None
    sty = _PS.get(p, _PS[None])
    st.markdown(f"""
    <div style="background:{sty['bg']};color:{sty['text']};padding:8px 16px;border-radius:8px;
         margin-bottom:8px;font-weight:700;font-size:14px;letter-spacing:0.4px;
         box-shadow:0 2px 6px rgba(0,0,0,.25);">
      {sty['emoji']} &nbsp; CANAL ACTIVO: {sty['label']}
    </div>""", unsafe_allow_html=True)
    # 1 "Todos" + 1 button per platform; leave generous left padding
    n_btns = 1 + len(enabled)
    cols = st.columns([3.5] + [1] * n_btns + [0.5])
    with cols[1]:
        if st.button("🌐 Todos", key=f"pf_all_{page_key}",
                     type="primary" if p is None else "secondary",
                     use_container_width=True):
            st.session_state["platform_filter"] = None
            st.rerun()
    for idx, platform_key in enumerate(enabled, start=2):
        plat_sty = _PS[platform_key]
        with cols[idx]:
            if st.button(f"{plat_sty['emoji']} {plat_sty['label'].split()[0]}",
                         key=f"pf_{platform_key}_{page_key}",
                         type="primary" if p == platform_key else "secondary",
                         use_container_width=True):
                st.session_state["platform_filter"] = platform_key
                st.rerun()
    return p


# ================================================================== #
#  PAGE 1: OVERVIEW
# ================================================================== #
def page_overview():
    st.header("Resumen General")
    _platform = render_platform_selector("ov")

    if _platform != "amazon":
        unknown = fetch_unknown_combos()
        if unknown:
            st.warning(f"{len(unknown)} SKU(s) en pedidos sin combo asignado. Ve a Gestion > Gestion Combos para revisarlos.")

    # Platform breakdown only for multi-platform tenants and when no filter set
    if _platform is None and len(get_enabled_platforms()) > 1:
        ps = fetch_platform_summary()
        if ps and (ps.get("amazon", {}).get("orders", 0) > 0):
            with st.expander("Resumen por Plataforma", expanded=True):
                pc1, pc2, pc3 = st.columns(3)
                with pc1:
                    tk = ps.get("tiktok", {})
                    st.metric("TikTok Órdenes", f"{tk.get('orders', 0):,}")
                    st.metric("TikTok GMV", f"${tk.get('gmv', 0):,.2f}")
                with pc2:
                    az = ps.get("amazon", {})
                    st.metric("Amazon Órdenes", f"{az.get('orders', 0):,}")
                    st.metric("Amazon GMV", f"${az.get('gmv', 0):,.2f}")
                with pc3:
                    cb = ps.get("combined", {})
                    st.metric("Total Órdenes", f"{cb.get('orders', 0):,}")
                    st.metric("Total GMV", f"${cb.get('gmv', 0):,.2f}")

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

    m = fetch_overview(date_from, date_to, platform=_platform)
    if not m or m.get("totalOrders", 0) == 0:
        if _platform == "amazon":
            st.info("No hay pedidos Amazon en la base de datos aún. Importa el archivo .txt desde el panel de administración → Data Import → paso 7.")
        else:
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

    c13, c14, c15, c16 = st.columns(4)
    c13.metric("Descuento Envío", f"${m.get('ShippingDiscount', 0):,.2f}")
    c14.metric("Dto. Plataforma", f"${m.get('PlatformDiscount', 0):,.2f}")
    c15.metric("Dto. Vendedor", f"${m.get('SellerDiscount', 0):,.2f}")
    c16.metric("Total Órdenes", f"{m.get('totalOrders', 0):,}")

    st.markdown("---")

    monthly = fetch_sales_by_month(date_from, date_to, platform=_platform)
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

    daily_raw = fetch_sales_by_day(date_from, date_to, platform=_platform)
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
    display_cols = ["ProductoNombre", "Tipo", "Initial_Stock", "FBT_Sent",
                    "Stock_Warehouse", "Stock_FBT", "QtyShipped",
                    "StockActualizado", "PedidosPendiente", "StockConPedidos",
                    "Sales_30d", "Days_Coverage", "Days_Cov_WH", "Days_Cov_FBT", "ValorInventario"]
    available = [c for c in display_cols if c in df.columns]
    display_df = df[available].sort_values("StockActualizado", ascending=True).copy()
    for cov_col in ["Days_Coverage", "Days_Cov_WH", "Days_Cov_FBT"]:
        if cov_col in display_df.columns:
            display_df[cov_col] = display_df[cov_col].apply(
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
    table_cols = ["ProductoNombre", "Tipo", "Stock_Warehouse", "Stock_FBT",
                  "QtyShipped", "StockActualizado", "StockConPedidos",
                  "WeeklyAvg_30d", "WeeklyAvg_60d", "Inv_deseado_custom", "Unid_a_comprar_custom",
                  "Days_Coverage", "Days_Cov_WH", "Days_Cov_FBT", "SellThroughRate"]
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

    cov_cols = [c for c in ["Days_Coverage", "Days_Cov_WH", "Days_Cov_FBT"] if c in available]
    styled = df[available].style.map(color_coverage, subset=cov_cols) if cov_cols else df[available].style
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
    _platform = render_platform_selector("afl")
    if _platform == "amazon":
        st.warning("Los afiliados y creadores son exclusivos de TikTok Shop. No hay datos de creadores en Amazon. Selecciona **Todos** o **TikTok** para ver esta sección.")
        return

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

    st.markdown("---")
    st.subheader("Alertas Virales (últimos 5 días)")
    st.caption("Creadores con ≥20 unidades vendidas en los últimos 5 días — posible video viral.")
    col_va1, col_va2 = st.columns(2)
    with col_va1:
        va_threshold = st.number_input("Umbral mínimo de unidades", min_value=1, value=20, key="af_va_threshold")
    with col_va2:
        va_days = st.number_input("Últimos N días", min_value=1, max_value=30, value=5, key="af_va_days")
    viral = fetch_viral_alerts(va_threshold, va_days)
    if viral:
        st.dataframe(pd.DataFrame(viral), use_container_width=True, height=300)
    else:
        st.info("Sin alertas virales en el período seleccionado.")

    st.markdown("---")
    st.subheader("Órdenes de Creadores (como compradores)")
    st.caption("Órdenes en AllBBDD donde el Buyer Username coincide con un Creator Username del panel de afiliados.")
    own_orders = fetch_creator_own_orders()
    if own_orders:
        st.metric("Total órdenes encontradas", len(own_orders))
        st.dataframe(pd.DataFrame(own_orders).head(200), use_container_width=True, height=350)
    else:
        st.info("No se encontraron órdenes de compradores que coincidan con creadores.")


# ================================================================== #
#  PAGE 5: FINANCES
# ================================================================== #
def page_finances():
    st.header("Finances")
    render_platform_selector("fin")
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
    _platform = render_platform_selector("ord")

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
    if _platform: params["platform"] = _platform

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
    _platform = render_platform_selector("cup")
    if _platform == "amazon":
        st.warning("Los compradores frecuentes y cupones son exclusivos de TikTok Shop. Selecciona **Todos** o **TikTok** para ver esta sección.")
        return

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
    if _platform: params["platform"] = _platform
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
    _platform = render_platform_selector("fdt")

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
    if _platform: params["platform"] = _platform

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
    if "expected_arrival" in df.columns:
        df["expected_arrival"] = pd.to_datetime(df["expected_arrival"], errors="coerce").dt.date

    # Filters — use product_name if available, fallback to product_id
    name_col = "product_name" if "product_name" in df.columns else "product_id"
    fcol1, fcol2 = st.columns(2)
    with fcol1:
        prod_names = sorted(df[name_col].dropna().astype(str).unique().tolist()) if name_col in df.columns else []
        filtro_prod = st.selectbox("Filtrar por Producto", ["Todos"] + prod_names, key="filter_prod_inv")
    with fcol2:
        filtro_status = st.selectbox("Filtrar por Status", ["Todos"] + status_options, key="filter_status_inv")

    df_view = df.copy()
    if filtro_prod != "Todos" and name_col in df_view.columns:
        df_view = df_view[df_view[name_col].astype(str) == filtro_prod]
    if filtro_status != "Todos" and "status" in df_view.columns:
        df_view = df_view[df_view["status"] == filtro_status]

    # Column order: product_name first, hide raw product_id
    col_order = ["product_name", "qty_ordered", "status", "cost", "order_date",
                 "expected_arrival", "supplier", "tracking", "notes", "id", "store_id", "product_id", "actual_arrival"]
    col_order = [c for c in col_order if c in df_view.columns]
    df_view = df_view[col_order]

    # Cargar catálogo de productos para el dropdown del editor (multi-tenant: filtra por store del user)
    _all_products = fetch_products() or []
    _product_names = sorted([p.get("name") for p in _all_products if p.get("name")])
    _name_to_pid = {p["name"]: p["id"] for p in _all_products if p.get("name") and p.get("id")}

    st.subheader("Pedidos actuales")
    st.caption("Para añadir un pedido nuevo, ve a la última fila vacía, elige el producto en el desplegable y rellena unidades + status.")
    edited = st.data_editor(
        df_view, num_rows="dynamic",
        column_config={
            "product_name": st.column_config.SelectboxColumn(
                "Producto", options=_product_names, required=False,
                help="Selecciona el producto del catálogo (se filtra por tu tienda)",
            ),
            "status": st.column_config.SelectboxColumn("Status", options=status_options, default="Pendiente"),
            "qty_ordered": st.column_config.NumberColumn("Unidades", min_value=-99999),
            "cost": st.column_config.NumberColumn("Coste", min_value=0, format="$%.2f"),
            "order_date": st.column_config.DateColumn("Fecha pedido"),
            "expected_arrival": st.column_config.DateColumn("Entrega estimada"),
        },
        use_container_width=True, height=500, key="inv_editor",
        disabled=["id", "store_id", "product_id", "actual_arrival"],  # product_name ya NO está disabled
        column_order=["product_name", "qty_ordered", "status", "cost", "order_date",
                      "expected_arrival", "supplier", "tracking", "notes"],
    )

    col_save, col_info = st.columns([1, 3])
    with col_save:
        if st.button("Guardar cambios", type="primary", key="save_pending"):
            saved, created, errors = 0, 0, 0
            err_msgs = []
            for _, row in edited.iterrows():
                record_id = row.get("id")
                if record_id and pd.notna(record_id):
                    # UPDATE de fila existente
                    update_data = {
                        "qty_ordered": int(row.get("qty_ordered", 0) or 0),
                        "status": row.get("status", "Pendiente"),
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
                else:
                    # INSERT de fila nueva — requiere product_name + qty_ordered
                    pname = row.get("product_name")
                    qty = row.get("qty_ordered")
                    if not pname or pd.isna(pname) or qty is None or pd.isna(qty):
                        continue  # fila vacía, skip silencioso
                    pid = _name_to_pid.get(pname)
                    if not pid:
                        errors += 1
                        err_msgs.append(f"Producto '{pname}' no encontrado en el catálogo")
                        continue
                    create_data = {
                        "product_id": pid,
                        "qty_ordered": int(qty or 0),
                        "status": row.get("status", "Pendiente") or "Pendiente",
                        "supplier": row.get("supplier") if pd.notna(row.get("supplier", None)) else None,
                        "tracking": row.get("tracking") if pd.notna(row.get("tracking", None)) else None,
                        "cost": float(row["cost"]) if pd.notna(row.get("cost")) else None,
                        "notes": row.get("notes") if pd.notna(row.get("notes", None)) else None,
                        "order_date": str(row["order_date"]) if pd.notna(row.get("order_date")) else None,
                        "expected_arrival": str(row["expected_arrival"]) if pd.notna(row.get("expected_arrival")) else None,
                    }
                    # Limpiar None en order_date/expected_arrival si vinieron como NaT
                    create_data = {k: v for k, v in create_data.items() if v is not None}
                    result = api_post("/inventory/incoming", create_data)
                    if result:
                        created += 1
                    else:
                        errors += 1
                        err_msgs.append(f"Error creando línea de '{pname}'")
            msg = f"Guardado: {saved} actualizados"
            if created:
                msg += f", {created} nuevos creados"
            st.success(msg + ".")
            if errors:
                st.warning(f"{errors} errores: " + " | ".join(err_msgs[:5]))
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
            "id": None,
            "store_id": None,
            "created_at": None,
            "updated_at": None,
        },
        disabled=["id", "store_id", "created_at", "updated_at"],
        column_order=["sku", "name", "category", "price_cost", "price_sale",
                      "units_per_box", "supplier", "status"],
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
    st.header("Gestion Combos y Mapeos")
    st.caption("Editor unificado de combos TikTok, mapeos Walmart y Amazon. Todos los SKUs con multiplicador N × producto base.")

    # Banner unknown
    unknown = fetch_unknown_combos()
    if unknown:
        n = len(unknown)
        st.warning(f"⚠️ {n} SKU(s) vendidos SIN mapear — asígnalos abajo o quedarán descontando 1 al azar")
        st.dataframe(pd.DataFrame(unknown), use_container_width=True, height=min(240, 50 + 35 * n))
    else:
        st.success("✅ Todos los SKUs vendidos están mapeados.")

    st.markdown("---")

    # ─── Cargar datos ───
    combos = fetch_combos()
    sku_maps = fetch_sku_maps("all")
    products_data = fetch_products()
    product_names = sorted([p["name"] for p in products_data]) if products_data else []
    product_map = {p["name"]: p["id"] for p in products_data} if products_data else {}

    # ─── Tabla unificada ───
    # Combos: 1 fila por (combo_sku, producto_componente, cantidad)
    # SKU maps: 1 fila por mapeo (walmart/amazon)
    rows = []
    for c in combos:
        for it in c.get("items", []):
            rows.append({
                "id":          c["id"],
                "Plataforma":  "tiktok",
                "SKU":         c["combo_sku"],
                "Producto":    it.get("product_name") or "",
                "Cantidad":    it.get("quantity") or 1,
                "_source":     "combo",
                "_item_id":    it.get("id"),
            })
    for m in sku_maps:
        rows.append({
            "id":          m["id"],
            "Plataforma":  m["platform"],
            "SKU":         m["external_sku"],
            "Producto":    m.get("product_name") or "",
            "Cantidad":    m.get("units_per_sale") or 1,
            "_source":     "sku_map",
            "_item_id":    None,
        })

    st.subheader("Todos los mapeos activos")
    st.caption(f"Total: {len(rows)} mapeos ({len(combos)} combos TikTok, {sum(1 for m in sku_maps if m['platform']=='walmart')} Walmart, {sum(1 for m in sku_maps if m['platform']=='amazon')} Amazon)")

    df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=["id","Plataforma","SKU","Producto","Cantidad","_source","_item_id"])

    # Editor con dropdowns
    edited = st.data_editor(
        df,
        num_rows="dynamic",
        column_config={
            "id":         st.column_config.TextColumn("id", disabled=True, width="small"),
            "Plataforma": st.column_config.SelectboxColumn("Plataforma",
                            options=["tiktok","amazon","walmart"], required=True, width="small"),
            "SKU":        st.column_config.TextColumn("SKU", required=True, width="medium"),
            "Producto":   st.column_config.SelectboxColumn("Producto",
                            options=product_names, required=True, width="large"),
            "Cantidad":   st.column_config.NumberColumn("Cant.", min_value=1, max_value=99,
                            step=1, required=True, width="small"),
            "_source":    None,   # oculta
            "_item_id":   None,
        },
        column_order=["id","Plataforma","SKU","Producto","Cantidad"],
        use_container_width=True,
        height=500,
        key="gestion_mapeos_editor",
    )

    col_save, col_info = st.columns([1, 3])
    with col_save:
        if st.button("💾 Guardar cambios", type="primary", key="save_mapeos"):
            n_created, n_updated, n_deleted, n_errors = 0, 0, 0, 0
            # Diff filas originales vs editadas
            original_ids = {(r["id"], r.get("_item_id")): r for r in rows}
            edited_ids = set()
            for _, r in edited.iterrows():
                rid = r.get("id")
                plat = (r.get("Plataforma") or "").strip()
                sku = (r.get("SKU") or "").strip()
                prod = (r.get("Producto") or "").strip()
                qty = int(r.get("Cantidad") or 1)
                if not (plat and sku and prod):
                    continue

                if rid and pd.notna(rid) and str(rid).strip():
                    # UPDATE existente
                    edited_ids.add(str(rid).strip())
                    orig = next((o for o in rows if o["id"] == rid), None)
                    if orig and (orig["Plataforma"]!=plat or orig["SKU"]!=sku or
                                 orig["Producto"]!=prod or orig["Cantidad"]!=qty):
                        if plat == "tiktok":
                            # combo update — reemplaza items wholesale (mismo prod, misma qty por simplicidad)
                            pid = product_map.get(prod)
                            if not pid: n_errors += 1; continue
                            res = api_put(f"/combos/{rid}", {
                                "combo_sku": sku, "combo_name": sku,
                                "items": [{"product_id": pid, "quantity": qty}]
                            })
                            if res: n_updated += 1
                            else: n_errors += 1
                        else:
                            pid = product_map.get(prod)
                            if not pid: n_errors += 1; continue
                            res = api_put(f"/sku-maps/{rid}?platform={plat}",
                                          {"external_sku": sku, "product_id": pid, "units_per_sale": qty})
                            if res: n_updated += 1
                            else: n_errors += 1
                else:
                    # INSERT nuevo
                    pid = product_map.get(prod)
                    if not pid: n_errors += 1; continue
                    if plat == "tiktok":
                        res = api_post("/combos", {
                            "combo_sku": sku, "combo_name": sku,
                            "items": [{"product_id": pid, "quantity": qty}]
                        })
                    else:
                        res = api_post("/sku-maps", {
                            "platform": plat, "external_sku": sku,
                            "product_id": pid, "units_per_sale": qty
                        })
                    if res: n_created += 1
                    else: n_errors += 1

            # DELETE los que estaban y no están en edited
            for orig in rows:
                if str(orig["id"]) not in edited_ids and orig["id"]:
                    if orig["_source"] == "combo":
                        if api_delete(f"/combos/{orig['id']}"): n_deleted += 1
                        else: n_errors += 1
                    else:
                        if api_delete(f"/sku-maps/{orig['id']}?platform={orig['Plataforma']}"): n_deleted += 1
                        else: n_errors += 1

            if n_errors > 0:
                st.error(f"❌ {n_errors} errores. Revisa que producto sea del dropdown.")
            summary = []
            if n_created: summary.append(f"✅ {n_created} nuevos")
            if n_updated: summary.append(f"✏️ {n_updated} actualizados")
            if n_deleted: summary.append(f"🗑 {n_deleted} borrados")
            if summary:
                st.success(" · ".join(summary))
                st.cache_data.clear()
                st.rerun()
            elif n_errors == 0:
                st.info("Sin cambios que guardar.")

    with col_info:
        st.info("💡 Añade filas nuevas al final. Cambia cualquier valor. Vacía la fila para borrarla. Guarda con el botón.")


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

    st.markdown("---")
    st.subheader("Detalle Órdenes Pallet FBT")
    st.caption("Órdenes TikTok con Fulfillment Type FBT (enviadas desde almacén TikTok).")
    pallet = fetch_pallet_orders()
    if pallet:
        st.dataframe(pd.DataFrame(pallet), use_container_width=True, height=400)
    else:
        st.info("Sin órdenes FBT pallet activas.")


# ================================================================== #
#  LOGIN + MAIN
# ================================================================== #
def login_page():
    st.title("Dashboard")
    with st.form("login_form"):
        email = st.text_input("Email")
        password = st.text_input("Password", type="password")
        if st.form_submit_button("Login"):
            if login(email, password):
                token = st.session_state.get("jwt_token", "")
                if token:
                    st.query_params["t"] = token
                st.rerun()
            else:
                st.error("Email o contraseña incorrectos")
    st.caption("Acceso restringido. Si necesitas una cuenta, contacta al administrador.")


# ================================================================== #
#  FINANCE SECTION — P&L estructurado + calculadora de líneas custom
# ================================================================== #

_MES_NOMBRES = ["", "Enero","Febrero","Marzo","Abril","Mayo","Junio",
                "Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"]
_MES_CORTOS  = ["", "Ene","Feb","Mar","Abr","May","Jun","Jul","Ago","Sep","Oct","Nov","Dic"]


def page_finance_pl():
    """Página única de Finance: P&L + calculadora de líneas custom por mes/YTD."""
    import datetime as _dt
    import pandas as _pd

    st.header("P&L Operacional")

    # --- Selector año + botones mes / YTD ---
    today = _dt.date.today()
    if "fin_year" not in st.session_state:
        st.session_state.fin_year = today.year
    if "fin_period" not in st.session_state:
        st.session_state.fin_period = f"{today.month:02d}"

    col_y, _ = st.columns([1, 6])
    with col_y:
        st.session_state.fin_year = st.selectbox(
            "Año", [today.year, today.year-1, today.year-2],
            index=[today.year, today.year-1, today.year-2].index(st.session_state.fin_year)
            if st.session_state.fin_year in [today.year, today.year-1, today.year-2] else 0,
        )

    # 12 botones mes + 1 botón YTD en una fila
    btn_cols = st.columns(13)
    for i in range(1, 13):
        with btn_cols[i-1]:
            label = _MES_CORTOS[i]
            mm = f"{i:02d}"
            is_selected = (st.session_state.fin_period == mm)
            if st.button(("● " if is_selected else "") + label, key=f"fin_btn_{mm}",
                         use_container_width=True,
                         type="primary" if is_selected else "secondary"):
                st.session_state.fin_period = mm
                st.rerun()
    with btn_cols[12]:
        is_ytd = (st.session_state.fin_period == "YTD")
        if st.button(("● " if is_ytd else "") + "YTD", key="fin_btn_ytd",
                     use_container_width=True,
                     type="primary" if is_ytd else "secondary"):
            st.session_state.fin_period = "YTD"
            st.rerun()

    st.markdown("---")

    # --- Llamar API ---
    year   = st.session_state.fin_year
    period = st.session_state.fin_period
    try:
        pl = api_get(f"/finance/pl?year={year}&period={period}")
    except Exception as exc:
        st.error(f"Error cargando P&L: {exc}")
        return

    st.subheader(f"P&L {pl['period_label']}")

    # --- Tabla P&L estructurada ---
    def _row(label, tt, am, tot, *, bold=False, sign="", color=None):
        cls = "font-weight:bold;" if bold else ""
        if color: cls += f"color:{color};"
        return (f"<tr style='{cls}'>"
                f"<td>{sign}{label}</td>"
                f"<td style='text-align:right'>${tt:,.2f}</td>"
                f"<td style='text-align:right'>${am:,.2f}</td>"
                f"<td style='text-align:right'>${tot:,.2f}</td>"
                f"</tr>")

    t = pl["tiktok"]; a = pl["amazon"]; tot = pl["total"]
    html = "<table style='width:100%;border-collapse:collapse;font-size:14px'>"
    html += "<tr style='border-bottom:2px solid #555;background:#1e2530'>"
    html += "<th style='text-align:left;padding:6px'>Concepto</th>"
    html += "<th style='text-align:right;padding:6px'>TIKTOK</th>"
    html += "<th style='text-align:right;padding:6px'>AMAZON</th>"
    html += "<th style='text-align:right;padding:6px'>TOTAL</th></tr>"

    # INGRESOS
    html += "<tr><td colspan=4 style='padding-top:8px;font-weight:bold;color:#7fc8ff'>INGRESOS</td></tr>"
    html += _row("Subtotal bruto (antes descuento)", t["gross_subtotal"], a["gross_subtotal"], tot["gross_subtotal"])
    html += _row("Seller discount", t["seller_discount"], a["seller_discount"], tot["seller_discount"], sign="− ")
    html += _row("Platform discount", t["platform_discount"], a["platform_discount"], tot["platform_discount"], sign="− ")
    html += _row("GMV (subtotal después de descuento)", t["gmv"], a["gmv"], tot["gmv"], bold=True, sign="= ")
    html += _row("Shipping cobrado al buyer", t["shipping_buyer"], a["shipping_buyer"], tot["shipping_buyer"], sign="+ ")
    html += _row("Ajuste plataforma (tax/etc)", t["platform_adjustment"], a["platform_adjustment"], tot["platform_adjustment"], sign="+ ")
    html += _row("Order amount cobrado al cliente", t["order_amount"], a["order_amount"], tot["order_amount"], bold=True, sign="= ")
    html += _row("Refunds", t["refunds"], a["refunds"], tot["refunds"], sign="− ")
    html += _row("NET ORDER AMOUNT", t["net_order_amount"], a["net_order_amount"], tot["net_order_amount"], bold=True, sign="= ")

    # COSTES
    html += "<tr><td colspan=4 style='padding-top:10px;font-weight:bold;color:#ff9999'>COSTES DIRECTOS</td></tr>"
    html += _row("COGS (coste mercancía vendida)", t["cogs"], a["cogs"], tot["cogs"], sign="− ")
    html += _row("Shipping carrier (Smart Ship)", t["shipping_carrier"], a["shipping_carrier"], tot["shipping_carrier"], sign="− ")
    html += _row(f"Shipping NETO (carrier − cobrado al buyer)", 0, 0, pl["shipping_net"], sign="  → ")

    # FEES
    html += "<tr><td colspan=4 style='padding-top:10px;font-weight:bold;color:#ffcb99'>FEES PLATAFORMA (auto)</td></tr>"
    html += _row("Referral fee", t["referral_fee"], a["referral_fee"], tot["referral_fee"], sign="− ")
    html += _row("Smart Promo fee (3.5%)", t["smart_promo_fee"], a["smart_promo_fee"], tot["smart_promo_fee"], sign="− ")
    html += _row("Smart Promo Campaign (1%)", t["smart_promo_campaign_fee"], a["smart_promo_campaign_fee"], tot["smart_promo_campaign_fee"], sign="− ")
    html += _row("Fees total", t["fees_total"], a["fees_total"], tot["fees_total"], bold=True, sign="= ")

    # CREATORS
    html += "<tr><td colspan=4 style='padding-top:10px;font-weight:bold;color:#cd99ff'>CREATORS</td></tr>"
    html += _row("Comisión creators (affiliate_sales)", 0, 0, tot["creators_commission"], sign="− ")

    # MARGEN BRUTO
    html += "<tr style='border-top:2px solid #555'><td colspan=4 style='padding-top:10px'></td></tr>"
    html += (f"<tr style='background:#243040;font-weight:bold;font-size:16px'>"
             f"<td style='padding:8px'>MARGEN BRUTO OPERACIONAL</td>"
             f"<td colspan=3 style='text-align:right;padding:8px;color:{'#7eff7e' if pl['gross_margin']>=0 else '#ff7e7e'}'>"
             f"${pl['gross_margin']:,.2f}</td></tr>")
    html += "</table>"
    st.markdown(html, unsafe_allow_html=True)

    # --- Calculadora de líneas custom ---
    st.markdown("---")
    st.subheader("Líneas custom (gastos fijos + extras)")

    if period == "YTD":
        st.info(
            "📊 Modo YTD: muestra todas las líneas custom del año "
            f"({year}). Para editar, selecciona un mes concreto."
        )
        if pl["custom_lines"]:
            df_view = _pd.DataFrame([
                {"Mes": l.get("year_month", ""),
                 "Descripción": l["description"], "Importe ($)": l["amount"]}
                for l in pl["custom_lines"]
            ])
            st.dataframe(df_view, hide_index=True, use_container_width=True)
        else:
            st.caption("Sin líneas custom en este año.")
    else:
        st.caption("Positivo = ingreso, negativo = gasto. Guarda con el botón debajo.")

        # Cargar líneas actuales para editar
        try:
            lines = api_get(f"/finance/custom-lines?year={year}&period={period}")
        except Exception as exc:
            st.error(f"Error cargando líneas: {exc}")
            lines = []

        # Construir DataFrame editable; añadir 10 filas vacías al final para que Oralia pueda meter nuevas
        rows = [{"Descripción": l["description"], "Importe ($)": float(l["amount"])} for l in lines]
        for _ in range(10):
            rows.append({"Descripción": "", "Importe ($)": 0.0})

        df_edit = _pd.DataFrame(rows)
        edited = st.data_editor(
            df_edit, num_rows="dynamic", use_container_width=True,
            column_config={
                "Descripción": st.column_config.TextColumn(width="large"),
                "Importe ($)": st.column_config.NumberColumn(format="$ %.2f", width="medium"),
            },
            key=f"fin_editor_{year}_{period}",
        )

        c1, c2, c3 = st.columns([1.4, 1.6, 4])
        with c1:
            if st.button("💾 Guardar líneas", type="primary", use_container_width=True):
                lines_payload = []
                for i, r in edited.iterrows():
                    desc = (r["Descripción"] or "").strip()
                    if not desc: continue
                    lines_payload.append({"description": desc, "amount": float(r["Importe ($)"] or 0), "sort_order": float(i)})
                try:
                    api_put(f"/finance/custom-lines?year={year}&period={period}",
                            {"lines": lines_payload})
                    st.success(f"Guardadas {len(lines_payload)} líneas.")
                    st.cache_data.clear()
                    st.rerun()
                except Exception as exc:
                    st.error(f"Error guardando: {exc}")

        with c2:
            if st.button("📋 Copiar del mes anterior", use_container_width=True):
                try:
                    res = api_post(f"/finance/custom-lines/copy-from-previous?year={year}&period={period}", {})
                    st.success(f"Copiadas {res['copied']} líneas del mes anterior.")
                    st.cache_data.clear()
                    st.rerun()
                except Exception as exc:
                    st.error(f"Error copiando: {exc}")

    # --- Resultado neto final ---
    st.markdown("---")
    summary_html = (
        f"<div style='padding:14px;background:#1e2530;border-radius:6px'>"
        f"<table style='width:100%;font-size:15px'>"
        f"<tr><td>Margen bruto operacional</td>"
        f"<td style='text-align:right'>${pl['gross_margin']:,.2f}</td></tr>"
        f"<tr><td>Ingresos custom</td>"
        f"<td style='text-align:right;color:#7eff7e'>+ ${pl['custom_total_income']:,.2f}</td></tr>"
        f"<tr><td>Gastos custom</td>"
        f"<td style='text-align:right;color:#ff7e7e'>− ${pl['custom_total_expense']:,.2f}</td></tr>"
        f"<tr style='border-top:2px solid #555;font-weight:bold;font-size:18px'>"
        f"<td style='padding-top:8px'>RESULTADO NETO {pl['period_label'].upper()}</td>"
        f"<td style='text-align:right;padding-top:8px;color:{'#7eff7e' if pl['net_result']>=0 else '#ff7e7e'}'>"
        f"${pl['net_result']:,.2f}</td></tr>"
        f"</table></div>"
    )
    st.markdown(summary_html, unsafe_allow_html=True)


# ================================================================== #
#  IMPORTAR — SUBIR FICHEROS Y HISTORIAL
# ================================================================== #
def _refresh_after_import():
    """Equivalent to the user clicking "Actualizar Datos": clears the
    backend analytics cache AND the local Streamlit cache, then reruns the
    page so the freshly-imported data is visible immediately. Called
    automatically after every successful upload — owner does not need to
    walk over to the dashboard and click anything; the next user to open
    a page sees fresh data."""
    try:
        api_post("/analytics/clear-cache")
    except Exception:
        pass
    st.cache_data.clear()
    st.rerun()


def page_import_upload():
    st.subheader("Subir Ficheros")
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### TikTok Shop — Pedidos")
        f_tiktok = st.file_uploader("CSV de pedidos TikTok", type=["csv"], key="up_tiktok")
        if st.button("Importar TikTok", key="btn_tiktok") and f_tiktok:
            with st.spinner("Importando..."):
                result = api_post("/import/orders",
                                  files={"file": (f_tiktok.name, f_tiktok.getvalue(), "text/csv")})
            if result:
                st.success(f"TikTok: {result.get('inserted', 0)} filas importadas, "
                           f"{result.get('errors', 0)} errores. "
                           "El reporte por email se dispara solo en 1-2 min.")
                _refresh_after_import()

        st.markdown("---")
        st.markdown("#### Afiliados / Creadores")
        f_aff = st.file_uploader("CSV de afiliados", type=["csv"], key="up_aff")
        if st.button("Importar Afiliados", key="btn_aff") and f_aff:
            with st.spinner("Importando..."):
                result = api_post("/import/affiliates",
                                  files={"file": (f_aff.name, f_aff.getvalue(), "text/csv")})
            if result:
                st.success(f"Afiliados: {result.get('inserted', 0)} filas importadas")
                _refresh_after_import()

        st.markdown("---")
        st.markdown("#### Productos")
        f_prod = st.file_uploader("Excel de productos", type=["xlsx"], key="up_prod")
        if st.button("Importar Productos", key="btn_prod") and f_prod:
            with st.spinner("Importando..."):
                result = api_post("/import/products",
                                  files={"file": (f_prod.name, f_prod.getvalue(),
                                                  "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")})
            if result:
                st.success(f"Productos: {result.get('inserted', 0)} nuevos, {result.get('updated', 0)} actualizados")
                _refresh_after_import()

    with col2:
        if "amazon" in get_enabled_platforms():
            st.markdown("#### Amazon — Pedidos")
            st.caption("Fichero TXT/TSV de Amazon Seller Central (All Orders report)")
            f_amazon = st.file_uploader("Fichero Amazon (.txt / .tsv)", type=["txt", "tsv", "csv"], key="up_amazon")
            if st.button("Importar Amazon", key="btn_amazon") and f_amazon:
                with st.spinner("Importando..."):
                    result = api_post("/import/amazon",
                                      files={"file": (f_amazon.name, f_amazon.getvalue(), "text/plain")})
                if result:
                    st.success(f"Amazon: {result.get('inserted', 0)} filas importadas, "
                               f"{result.get('errors', 0)} errores. "
                               "El reporte por email se dispara solo en 1-2 min.")
                    _refresh_after_import()

            st.markdown("---")
        if "walmart" in get_enabled_platforms():
            st.markdown("#### Walmart — Pedidos")
            st.caption("Excel PO Data export de Walmart Seller Center. Sube los 2 archivos por separado "
                       "(SellerFulfilled + WFSFulfilled) — cada uno con su batch.")
            f_walmart = st.file_uploader("Fichero Walmart (.xlsx)", type=["xlsx", "xls"], key="up_walmart")
            if st.button("Importar Walmart", key="btn_walmart") and f_walmart:
                with st.spinner("Importando..."):
                    result = api_post("/import/walmart",
                                      files={"file": (f_walmart.name, f_walmart.getvalue(),
                                                      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")})
                if result:
                    st.success(f"Walmart: {result.get('inserted', 0)} filas importadas, "
                               f"{result.get('errors', 0)} errores. "
                               "El reporte por email se dispara solo en 1-2 min.")
                    _refresh_after_import()

            st.markdown("---")
        st.markdown("#### Combos")
        f_combos = st.file_uploader("Excel de combos", type=["xlsx"], key="up_combos")
        if st.button("Importar Combos", key="btn_combos") and f_combos:
            with st.spinner("Importando..."):
                result = api_post("/import/combos",
                                  files={"file": (f_combos.name, f_combos.getvalue(),
                                                  "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")})
            if result:
                st.success(f"Combos: {result.get('inserted', 0)} nuevos, {result.get('updated', 0)} actualizados")
                _refresh_after_import()

        st.markdown("---")
        st.markdown("#### Inventario Pendiente")
        f_inv = st.file_uploader("Excel inventario pendiente", type=["xlsx"], key="up_inv")
        if st.button("Importar Inventario Pendiente", key="btn_inv") and f_inv:
            with st.spinner("Importando..."):
                result = api_post("/import/incoming-stock",
                                  files={"file": (f_inv.name, f_inv.getvalue(),
                                                  "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")})
            if result:
                st.success(f"Inventario: {result.get('inserted', 0)} filas importadas")


def page_import_history():
    st.subheader("Historial de Cargas")
    st.caption("Solo cargas de tipo TikTok y Amazon pueden borrarse (ventas). "
               "Productos, combos e inventario son upserts y no admiten rollback.")

    if st.button("Actualizar historial", key="btn_hist_refresh"):
        st.cache_data.clear()

    history = api_get("/import/history", {"limit": 100}) or []

    if not history:
        st.info("No hay cargas registradas todavía.")
        return

    _type_labels = {"tiktok": "TikTok", "amazon": "Amazon", "affiliates": "Afiliados",
                    "products": "Productos", "combos": "Combos",
                    "inventory": "Inventario", "incoming_stock": "Stock Pendiente"}
    _type_colors = {"tiktok": "#FF0050", "amazon": "#FF9900"}

    for row in history:
        itype = row.get("import_type", "")
        fname = row.get("filename") or "—"
        imported_at = (row.get("imported_at") or "")[:16].replace("T", " ")
        imported_by = row.get("imported_by") or "—"
        rows_in = row.get("rows_imported", 0)
        rows_del = row.get("rows_deleted", 0)
        label = _type_labels.get(itype, itype.upper())
        can_delete = itype in ("tiktok", "amazon")

        with st.container(border=True):
            c1, c2 = st.columns([5, 1])
            with c1:
                color = _type_colors.get(itype, "#888")
                st.markdown(
                    f"<span style='background:{color};color:white;padding:2px 8px;"
                    f"border-radius:4px;font-size:12px;font-weight:600'>{label}</span> "
                    f"&nbsp; **{fname}**",
                    unsafe_allow_html=True,
                )
                st.caption(
                    f"{imported_at}  ·  {imported_by}  ·  "
                    f"{rows_in} filas importadas  ·  {rows_del} anteriores borradas"
                )
            with c2:
                if can_delete:
                    batch_id = row.get("id", "")
                    if st.button("Borrar", key=f"del_{batch_id}",
                                 help="Elimina todas las órdenes de esta carga de la base de datos"):
                        result = api_delete(f"/import/history/{batch_id}")
                        if result:
                            st.success("Carga eliminada correctamente")
                            st.cache_data.clear()
                            st.rerun()
                else:
                    st.caption("No reversible")


# ================================================================== #
#  MAIN
# ================================================================== #
def main():
    # Restore session from URL query param (survives page refresh)
    if "jwt_token" not in st.session_state:
        t = st.query_params.get("t")
        if t:
            st.session_state["jwt_token"] = t

    if "jwt_token" not in st.session_state:
        login_page()
        return

    # Cache user info in session_state so /auth/me isn't called on every widget interaction
    if "cached_user" not in st.session_state:
        user = api_get("/auth/me")
        if not user:
            st.session_state.pop("jwt_token", None)
            st.query_params.clear()
            login_page()
            return
        st.session_state["cached_user"] = user
    else:
        user = st.session_state["cached_user"]

    _store_label = user.get("store_name") or "Dashboard"
    st.title(f"{_store_label} Dashboard")

    with st.sidebar:
        st.write(f"**{user.get('email', '')}**")
        store_name = user.get("store_name", user.get("store_id", "")[:8])
        st.caption(f"Tienda: {store_name}")
        if st.button("Actualizar Datos"):
            api_post("/analytics/clear-cache")
            st.cache_data.clear()
            st.rerun()
        _user_role = user.get("role", "viewer")
        if _user_role in ("admin", "superadmin"):
            if st.button("Enviar Reporte Diario"):
                result = api_post("/reports/send-now")
                if result:
                    st.success("Reporte en cola — llega en 1-2 min")
            with st.expander("Agentes IA"):
                from datetime import date as _date
                _today_wd = _date.today().weekday()  # 0=Mon,1=Tue,2=Wed,3=Thu,4=Fri
                _agents = [
                    ("PRISM", "/agents/prism", "Lunes", 0),
                    ("HAIKU", "/agents/haiku", "Miercoles", 2),
                    ("FARAWAY", "/agents/faraway", "Viernes", 4),
                    ("MESMERIZE", "/agents/mesmerize", "1er Lunes", 0),
                    ("TIMELESS", "/agents/timeless", "Día 1 mes", None),
                ]
                from datetime import date as _date_today
                _today_dom = _date_today.today().day
                for _aname, _apath, _aday, _awd in _agents:
                    if _aname == "TIMELESS":
                        _is_day = _today_dom == 1
                    elif _aname == "MESMERIZE":
                        _is_day = _today_wd == _awd and _today_dom <= 7
                    else:
                        _is_day = _today_wd == _awd
                    _label = f"{_aname} ({_aday})" + (" [HOY]" if _is_day else "")
                    if st.button(_label, key=f"ag_{_aname}"):
                        _r = api_post(f"{_apath}?force=true")
                        if _r:
                            st.success(f"{_aname} en cola — email en ~2 min")
        if st.button("Cerrar Sesion"):
            st.session_state.pop("jwt_token", None)
            st.session_state.pop("cached_user", None)
            st.query_params.clear()
            st.cache_data.clear()
            st.rerun()
        st.markdown("---")
        _sb_p = st.session_state.get("platform_filter")
        _sb_s = _PS.get(_sb_p, _PS[None])
        st.markdown(f"""<div style="background:{_sb_s['bg']};color:{_sb_s['text']};
            padding:4px 10px;border-radius:6px;font-size:12px;font-weight:600;text-align:center;">
            {_sb_s['emoji']} {_sb_s['label']}</div>""", unsafe_allow_html=True)
        st.caption("Cambia el canal en cada página")
        st.markdown("---")
        st.caption(f"v2.2.0 | {_store_label}")

    _role = user.get("role", "viewer")
    _modules = user.get("modules_enabled") or {}
    _finance_enabled = bool(_modules.get("finance", False))
    if _role in ("admin", "superadmin"):
        _sections = ["Dashboard", "Gestion"]
        if _finance_enabled:
            _sections.append("Finance")
    elif _role == "warehouse":
        _sections = ["Gestion"]
    else:
        _sections = ["Dashboard", "Gestion"]
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
        if not _finance_enabled:
            st.warning("El modulo Finance no esta habilitado para esta tienda.")
        else:
            page_finance_pl()



if __name__ == "__main__":
    main()
