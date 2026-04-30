"""
Rodmat Dashboard V2 - Multi-tenant Streamlit Dashboard
Consumes the V2 FastAPI backend via JWT-authenticated REST API.
9 tabs ported from V1.
"""
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go

from api_client import api_get, api_post, api_put, api_delete, login, register

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
    @media (prefers-color-scheme: dark) {
        [data-testid="stMetric"] { background-color: #262730; border-left: 4px solid #4da6ff; }
        [data-testid="stMetricLabel"] p { color: #fafafa !important; }
        [data-testid="stMetricValue"] { color: #ffffff !important; }
    }
</style>
""", unsafe_allow_html=True)


# ================================================================== #
#  CACHED API CALLS
# ================================================================== #
@st.cache_data(ttl=300)
def fetch_overview():
    return api_get("/analytics/overview") or {}

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
def fetch_reorder(coverage_days=30):
    return api_get("/analytics/reorder-list", {"coverage_days": coverage_days}) or []

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
def fetch_unknown_combos():
    return api_get("/analytics/unknown-combos") or []

@st.cache_data(ttl=300)
def fetch_combos():
    return api_get("/combos") or []


# ================================================================== #
#  PAGE 1: OVERVIEW
# ================================================================== #
def page_overview():
    st.header("Overview")

    unknown = fetch_unknown_combos()
    if unknown:
        st.warning(f"{len(unknown)} SKU(s) en pedidos sin combo asignado. Ve a Gestion > Gestion Combos para revisarlos.")

    m = fetch_overview()
    if not m:
        st.warning("No data available.")
        return

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Net Order Amount", f"${m.get('NetOrderAmount', 0):,.2f}")
    c2.metric("GMV (SKU Subtotal)", f"${m.get('TITKOKGMVOrderAmount', 0):,.2f}")
    c3.metric("Creator Commission", f"${m.get('CreatorCommission', 0):,.2f}")
    c4.metric("Creator Payment", f"${m.get('CreatorPayment', 0):,.2f}")

    c5, c6, c7, c8 = st.columns(4)
    c5.metric("Net Orders", f"{m.get('netOrder', 0):,}")
    c6.metric("% vs Prev Month", f"{m.get('PctVsPrevMonth', 0):+.1f}%")
    c7.metric("Net w/o Shipping", f"${m.get('netOrderWOUshipping', 0):,.2f}")
    c8.metric("Shipping Fees", f"${m.get('ShippingFees', 0):,.2f}")

    c9, c10, c11, c12 = st.columns(4)
    c9.metric("Seller+Platform Disc.", f"${m.get('SellerDiscount',0) + m.get('PlatformDiscount',0):,.2f}")
    c10.metric("Comm+Ref+PlatDisc", f"${m.get('CreatorCommission',0) + m.get('RefferarFees',0) + m.get('PlatformDiscount',0):,.2f}")
    c11.metric("Creator Orders", f"{m.get('CreatorOrderCount', 0):,}")
    c12.metric("Referrar Fees (est.)", f"${m.get('RefferarFees', 0):,.2f}")

    st.markdown("---")
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("GMV Sales by Month")
        monthly = fetch_sales_by_month()
        if monthly:
            df = pd.DataFrame(monthly)
            fig = px.bar(df, x="Month", y="GMV", text_auto="$.2s")
            fig.update_layout(height=350, margin=dict(t=10, b=10))
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("GMV Sales by Day")
        daily = fetch_sales_by_day()
        if daily:
            df = pd.DataFrame(daily)
            fig = px.line(df, x="Day", y="GMV", markers=True)
            fig.update_layout(height=350, margin=dict(t=10, b=10))
            st.plotly_chart(fig, use_container_width=True)

    col3, col4 = st.columns(2)
    with col3:
        st.subheader("Top 10 Creators")
        creators = fetch_top_creators(10)
        if creators:
            df = pd.DataFrame(creators)
            fig = px.bar(df, x="GMV", y="Creator Username", orientation="h",
                         color="GMV", color_continuous_scale="Blues")
            fig.update_layout(height=400, yaxis={"categoryorder": "total ascending"}, margin=dict(t=10))
            st.plotly_chart(fig, use_container_width=True)

    with col4:
        st.subheader("Content Type Distribution")
        ct = fetch_creator_by_type()
        if ct:
            df = pd.DataFrame(ct)
            fig = px.pie(df, values="GMV", names="Content Type")
            fig.update_layout(height=400, margin=dict(t=10))
            st.plotly_chart(fig, use_container_width=True)


# ================================================================== #
#  PAGE 2: INVENTARIO SUMMARY
# ================================================================== #
def page_inventario_summary():
    st.header("Inventario Summary")
    data = fetch_stock_summary()
    if not data:
        st.warning("No stock data.")
        return
    df = pd.DataFrame(data)

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Stock Actual", f"{df['StockActualizado'].sum():,.0f}")
    c2.metric("Valor Inventario", f"${df['ValorInventario'].sum():,.2f}")
    low_count = len(df[(df.get("Days_Coverage", pd.Series(dtype=float)) < 7) & (df["Initial_Stock"] > 0)]) if "Days_Coverage" in df.columns else 0
    total_with_stock = len(df[df["Initial_Stock"] > 0])
    ruptura = (low_count / total_with_stock * 100) if total_with_stock > 0 else 0
    c3.metric("Ruptura 7d %", f"{ruptura:.1f}%")
    total_sold = df["Sales_30d"].sum() if "Sales_30d" in df.columns else 0
    total_stock = df["StockActualizado"].sum()
    str_rate = (total_sold / (total_sold + total_stock) * 100) if (total_sold + total_stock) > 0 else 0
    c4.metric("Sell Through Rate", f"{str_rate:.1f}%")
    c5.metric("Products Low Stock", f"{low_count}")

    st.markdown("---")
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Stock vs Ventas 30d")
        chart = df[["ProductoNombre", "StockActualizado", "Sales_30d"]].sort_values("StockActualizado", ascending=False).head(20)
        fig = go.Figure()
        fig.add_trace(go.Bar(name="Stock", x=chart["ProductoNombre"], y=chart["StockActualizado"]))
        fig.add_trace(go.Bar(name="Ventas 30d", x=chart["ProductoNombre"], y=chart["Sales_30d"]))
        fig.update_layout(barmode="group", height=400, margin=dict(t=10), xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Valor Inventario por Tipo")
        if "Tipo" in df.columns:
            val = df.groupby("Tipo")["ValorInventario"].sum().reset_index()
            fig = px.bar(val, x="Tipo", y="ValorInventario", color="Tipo", text_auto="$.2s")
            fig.update_layout(height=400, margin=dict(t=10))
            st.plotly_chart(fig, use_container_width=True)

    st.subheader("Stock Table")
    display_cols = ["ProductoNombre", "Tipo", "Initial_Stock", "QtyShipped", "StockActualizado",
                    "PedidosPendiente", "StockConPedidos", "Sales_30d", "Days_Coverage", "ValorInventario"]
    available = [c for c in display_cols if c in df.columns]
    st.dataframe(df[available].sort_values("StockActualizado", ascending=True), use_container_width=True, height=400)


# ================================================================== #
#  PAGE 3: STOCK DETAIL
# ================================================================== #
def page_stock_detail():
    st.header("Stock Detail")
    coverage = st.number_input("Coverage days", min_value=7, max_value=180, value=30, key="sd_cov")
    data = fetch_stock_detail(coverage)
    if not data:
        st.warning("No stock data.")
        return
    df = pd.DataFrame(data)

    df["Inv_deseado_custom"] = (df["AvgVentas30d"] * coverage).round(0)
    df["Unid_a_comprar_custom"] = np.maximum(0, df["Inv_deseado_custom"] - df["StockActualizado"]).round(0)
    if "UNIDADES POR CAJA" in df.columns:
        df["Cajas_custom"] = np.where(df["UNIDADES POR CAJA"] > 0,
                                       np.ceil(df["Unid_a_comprar_custom"] / df["UNIDADES POR CAJA"]),
                                       df["Unid_a_comprar_custom"])
    if "Coste" in df.columns:
        df["Importe_custom"] = df["Unid_a_comprar_custom"] * df["Coste"]
        st.metric("Total Importe a Comprar", f"${df['Importe_custom'].sum():,.2f}")

    st.subheader("Stock Analysis")
    table_cols = ["ProductoNombre", "Tipo", "QtyShipped", "StockActualizado", "StockConPedidos",
                  "AvgVentas30d", "AvgVentas60d", "Inv_deseado_custom", "Unid_a_comprar_custom",
                  "Days_Coverage", "SellThroughRate"]
    available = [c for c in table_cols if c in df.columns]

    def color_coverage(val):
        if isinstance(val, (int, float)):
            if val < 7: return "background-color: #ffcccc"
            elif val < 14: return "background-color: #ffffcc"
            else: return "background-color: #ccffcc"
        return ""

    styled = df[available].style.map(color_coverage, subset=["Days_Coverage"] if "Days_Coverage" in available else [])
    st.dataframe(styled, use_container_width=True, height=500)

    st.markdown("---")
    st.subheader("Purchase Order")
    reorder = df[df["Unid_a_comprar_custom"] > 0].copy()
    if not reorder.empty:
        order_cols = ["ProductoNombre", "Unid_a_comprar_custom"]
        if "Cajas_custom" in reorder.columns: order_cols.append("Cajas_custom")
        if "Coste" in reorder.columns: order_cols.append("Coste")
        if "Importe_custom" in reorder.columns: order_cols.append("Importe_custom")
        st.dataframe(reorder[order_cols], use_container_width=True)
        csv = reorder[order_cols].to_csv(index=False).encode("utf-8")
        st.download_button("Download Purchase Order CSV", data=csv,
                           file_name="purchase_order.csv", mime="text/csv")
    else:
        st.success("No products need reordering.")


# ================================================================== #
#  PAGE 4: AFILIADOS
# ================================================================== #
def page_afiliados():
    st.header("Afiliados Detail")

    c1, c2, c3 = st.columns(3)
    creators = fetch_top_creators(50)
    ct_data = fetch_creator_by_type()
    cr_monthly = fetch_creator_by_month()

    if not creators:
        st.warning("No affiliate data.")
        return

    cr_df = pd.DataFrame(creators)
    c1.metric("Total Creators", f"{len(cr_df):,}")
    c2.metric("Total GMV", f"${cr_df['GMV'].sum():,.2f}")
    c3.metric("Total Commission", f"${cr_df['Commission'].sum():,.2f}")

    st.markdown("---")
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Top 20 Creators by GMV")
        fig = px.bar(cr_df.head(20), x="GMV", y="Creator Username", orientation="h",
                     color="GMV", color_continuous_scale="Blues")
        fig.update_layout(height=450, yaxis={"categoryorder": "total ascending"}, margin=dict(t=10))
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Content Type Distribution")
        if ct_data:
            df = pd.DataFrame(ct_data)
            fig = px.pie(df, values="GMV", names="Content Type")
            fig.update_layout(height=450, margin=dict(t=10))
            st.plotly_chart(fig, use_container_width=True)

    col3, col4 = st.columns(2)
    with col3:
        st.subheader("Sales by Month (Affiliates)")
        if cr_monthly:
            df = pd.DataFrame(cr_monthly)
            monthly_agg = df.groupby("Month")["GMV"].sum().reset_index()
            fig = px.bar(monthly_agg, x="Month", y="GMV", text_auto="$.2s")
            fig.update_layout(height=350, margin=dict(t=10))
            st.plotly_chart(fig, use_container_width=True)

    with col4:
        st.subheader("GMV by Content Type")
        if ct_data:
            df = pd.DataFrame(ct_data)
            fig = px.bar(df, x="Content Type", y="GMV", color="Content Type", text_auto="$.2s")
            fig.update_layout(height=350, margin=dict(t=10))
            st.plotly_chart(fig, use_container_width=True)

    if cr_monthly:
        st.subheader("Creator by Month (Pivot)")
        df = pd.DataFrame(cr_monthly)
        if not df.empty:
            pivot = df.pivot_table(index="Creator Username", columns="Month", values="GMV", fill_value=0, aggfunc="sum")
            st.dataframe(pivot, use_container_width=True, height=300)


# ================================================================== #
#  PAGE 5: FINANCES
# ================================================================== #
def page_finances():
    st.header("Finances")
    data = fetch_finances()
    if not data:
        st.warning("No data.")
        return
    df = pd.DataFrame(data)

    c1, c2, c3 = st.columns(3)
    c1.metric("Total Stock Units", f"{df['StockActualizado'].sum():,.0f}")
    c2.metric("Value (Cost)", f"${df['ValorInventario'].sum():,.2f}")
    if "ValorRetail" in df.columns:
        c3.metric("Value (Retail)", f"${df['ValorRetail'].sum():,.2f}")

    st.dataframe(df.sort_values("ValorInventario", ascending=False), use_container_width=True, height=600)
    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button("Download Finances CSV", data=csv, file_name="finances.csv", mime="text/csv")


# ================================================================== #
#  PAGE 6: ORDENES CHECK
# ================================================================== #
def page_ordenes_check():
    st.header("Ordenes Check")

    c1, c2 = st.columns(2)
    with c1:
        oc_order = st.text_input("Order ID", "", key="oc_order")
    with c2:
        oc_sku = st.text_input("SKU", "", key="oc_sku")

    params = {"limit": 500}
    if oc_order: params["order_id"] = oc_order
    if oc_sku: params["sku"] = oc_sku

    result = api_get("/analytics/orders", params)
    if not result:
        st.warning("No orders.")
        return

    orders = result.get("orders", [])
    total = result.get("total", 0)
    st.metric("Total Matching", f"{total:,}")

    if orders:
        df = pd.DataFrame(orders)
        # Status summary
        if "status" in df.columns:
            st.subheader("Orders by Status")
            summary = df.groupby("status").agg(Count=("order_id", "nunique")).reset_index()
            st.dataframe(summary, use_container_width=True)

        st.subheader("Order Details")
        st.dataframe(df.head(200), use_container_width=True, height=400)

    st.markdown("---")
    st.subheader("Top Combos")
    combos = fetch_top_combos(20)
    if combos:
        st.dataframe(pd.DataFrame(combos), use_container_width=True)


# ================================================================== #
#  PAGE 7: CUPONES
# ================================================================== #
def page_cupones():
    st.header("Analisis Cupones")

    buyers = fetch_frequent_buyers()
    if not buyers:
        st.warning("No data.")
        return

    df = pd.DataFrame(buyers)
    freq = df[df["OrderCount"] > 1].copy()

    st.subheader("Clientes Frecuentes")
    st.metric("Repeat Customers", f"{len(freq):,}")
    st.dataframe(freq.head(100), use_container_width=True, height=400)


# ================================================================== #
#  PAGE 8: FULL DETAIL
# ================================================================== #
def page_full_detail():
    st.header("Full Detail")

    c1, c2, c3, c4 = st.columns(4)
    with c1: fd_buyer = st.text_input("Buyer Username", "", key="fd_buyer")
    with c2: fd_order = st.text_input("Order ID", "", key="fd_order")
    with c3: fd_status = st.text_input("Status", "", key="fd_status")
    with c4: fd_product = st.text_input("Product Name", "", key="fd_product")

    params = {"limit": 500}
    if fd_buyer: params["buyer"] = fd_buyer
    if fd_order: params["order_id"] = fd_order
    if fd_status: params["status"] = fd_status
    if fd_product: params["product_name"] = fd_product

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
        st.download_button("Download CSV", data=csv, file_name="full_detail.csv", mime="text/csv")


# ================================================================== #
#  PAGE 9: GESTION INVENTARIO
# ================================================================== #
def page_gestion_inventario():
    st.header("Gestion Inventario Pendiente")

    data = fetch_incoming_stock()
    if data:
        df = pd.DataFrame(data)
    else:
        df = pd.DataFrame(columns=["id", "product_id", "qty_ordered", "status", "supplier",
                                    "tracking", "cost", "notes"])

    status_options = ["pending", "Recibido", "En transito", "Cancelado"]

    edited = st.data_editor(
        df, num_rows="dynamic",
        column_config={
            "status": st.column_config.SelectboxColumn("Status", options=status_options, default="pending"),
            "qty_ordered": st.column_config.NumberColumn("Qty Ordered", min_value=0),
            "cost": st.column_config.NumberColumn("Cost", min_value=0, format="$%.2f"),
        },
        use_container_width=True, height=500, key="inv_editor",
        disabled=["id", "store_id", "product_id"],
    )

    if st.button("Save Changes", type="primary"):
        for _, row in edited.iterrows():
            record_id = row.get("id")
            if record_id and pd.notna(record_id):
                update_data = {
                    "qty_ordered": int(row.get("qty_ordered", 0)),
                    "status": row.get("status", "pending"),
                    "supplier": row.get("supplier"),
                    "tracking": row.get("tracking"),
                    "cost": float(row["cost"]) if pd.notna(row.get("cost")) else None,
                    "notes": row.get("notes"),
                }
                api_put(f"/inventory/incoming/{record_id}", update_data)
        st.success("Changes saved!")
        st.cache_data.clear()


# ================================================================== #
#  PAGE 10: GESTION COMBOS
# ================================================================== #
def page_gestion_combos():
    st.header("Gestion Combos")
    st.caption("SKUs sin combo asignado y editor de combos.")

    # Alert: unknown combos
    unknown = fetch_unknown_combos()
    if unknown:
        n = len(unknown)
        st.warning(f"{n} SKU(s) pendientes de asignar combo")
        st.subheader("SKUs sin asignar")
        st.dataframe(pd.DataFrame(unknown), use_container_width=True, height=min(300, 50 + 35 * n))
    else:
        st.success("Todos los SKUs tienen combo asignado o son productos conocidos.")

    st.markdown("---")

    # Editable combo table
    st.subheader("Listado de Combos")
    combos = fetch_combos()
    if combos:
        combo_df = pd.DataFrame(combos)
    else:
        combo_df = pd.DataFrame(columns=["id", "combo_sku", "items"])

    if not combo_df.empty:
        st.dataframe(combo_df, use_container_width=True, height=400)

    st.markdown("---")
    st.subheader("Agregar Combo")

    # Fetch products for selection
    products_data = api_get("/products") or []
    product_map = {p["name"]: p["id"] for p in products_data} if products_data else {}

    with st.form("add_combo_form"):
        new_sku = st.text_input("Combo SKU (Seller SKU)")
        new_name = st.text_input("Combo Name")
        new_items_text = st.text_area(
            "Productos (uno por linea, formato: nombre_producto,cantidad)",
            placeholder="Producto A,1\nProducto B,2",
            help="Usa los nombres exactos de productos registrados."
        )
        if st.form_submit_button("Agregar Combo"):
            if new_sku and new_name and new_items_text:
                items = []
                errors = []
                for line in new_items_text.strip().split("\n"):
                    parts = line.strip().split(",")
                    pname = parts[0].strip()
                    qty = int(parts[1].strip()) if len(parts) >= 2 else 1
                    pid = product_map.get(pname)
                    if pid:
                        items.append({"product_id": pid, "quantity": qty})
                    else:
                        errors.append(pname)
                if errors:
                    st.error(f"Productos no encontrados: {', '.join(errors)}")
                elif items:
                    result = api_post("/combos", {"combo_sku": new_sku, "combo_name": new_name, "items": items})
                    if result:
                        st.success(f"Combo '{new_sku}' creado con {len(items)} productos.")
                        st.cache_data.clear()
                        st.rerun()
                    else:
                        st.error("Error creando combo.")
                else:
                    st.warning("Agrega al menos un producto.")
            else:
                st.warning("Completa todos los campos.")


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
                    st.error("Invalid email or password")

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
                    st.warning("Fill all fields")


def main():
    if "jwt_token" not in st.session_state:
        login_page()
        return

    # Verify token
    user = api_get("/auth/me")
    if not user:
        login_page()
        return

    st.title("Rodmat Dashboard V2")

    with st.sidebar:
        st.write(f"**{user.get('email', '')}**")
        st.caption(f"Store: {user.get('store_id', '')[:8]}...")
        if st.button("Refresh Data"):
            st.cache_data.clear()
            st.rerun()
        if st.button("Logout"):
            st.session_state.pop("jwt_token", None)
            st.cache_data.clear()
            st.rerun()
        st.markdown("---")
        st.caption("v2.0.0 | Rodmat Dashboard")

    section = st.sidebar.radio("Seccion", ["Dashboard", "Gestion"], index=0)

    if section == "Dashboard":
        tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8 = st.tabs([
            "Overview", "Inventario Summary", "Stock Detail", "Afiliados",
            "Finances", "Ordenes Check", "Cupones", "Full Detail",
        ])

        with tab1: page_overview()
        with tab2: page_inventario_summary()
        with tab3: page_stock_detail()
        with tab4: page_afiliados()
        with tab5: page_finances()
        with tab6: page_ordenes_check()
        with tab7: page_cupones()
        with tab8: page_full_detail()

    elif section == "Gestion":
        tab_g1, tab_g2 = st.tabs(["Inventario Pendiente", "Gestion Combos"])
        with tab_g1: page_gestion_inventario()
        with tab_g2: page_gestion_combos()


if __name__ == "__main__":
    main()
