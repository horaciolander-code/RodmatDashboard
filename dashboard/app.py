"""
Multi-tenant Streamlit Dashboard — title and modules are rendered
per-tenant based on /api/auth/me (store_name + modules_enabled).
"""
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go


# ══════════════════════════════════════════════════════════════════════════
# 🎨 Plotly Template Global — paleta futurista aplicada a TODAS las gráficas
# ══════════════════════════════════════════════════════════════════════════
import plotly.io as _pio
_pio.templates["rodmat_neon"] = go.layout.Template(
    layout=go.Layout(
        colorway=["#00D4FF", "#00FF88", "#FF9F45", "#7B61FF", "#FF3D6B", "#FF6B35",
                  "#4A9EFF", "#B47CFF", "#00CC66", "#FFB84D"],
        paper_bgcolor="rgba(15,20,47,0)",
        plot_bgcolor="rgba(15,20,47,0.4)",
        font=dict(family="-apple-system, 'Segoe UI', system-ui, sans-serif",
                  color="#e4e9ff", size=12),
        title=dict(font=dict(color="#e4e9ff", size=15, family="-apple-system, sans-serif"), x=0.02),
        xaxis=dict(gridcolor="rgba(123,97,255,0.08)", zerolinecolor="rgba(123,97,255,0.2)",
                   linecolor="rgba(123,97,255,0.2)", tickcolor="rgba(123,97,255,0.2)",
                   tickfont=dict(color="#8892b0", size=11),
                   title=dict(font=dict(color="#8892b0", size=12))),
        yaxis=dict(gridcolor="rgba(123,97,255,0.08)", zerolinecolor="rgba(123,97,255,0.2)",
                   linecolor="rgba(123,97,255,0.2)", tickcolor="rgba(123,97,255,0.2)",
                   tickfont=dict(color="#8892b0", size=11),
                   title=dict(font=dict(color="#8892b0", size=12))),
        legend=dict(bgcolor="rgba(15,20,47,0.5)", bordercolor="rgba(123,97,255,0.2)",
                    borderwidth=1, font=dict(color="#e4e9ff", size=11)),
        hoverlabel=dict(bgcolor="#0f142f", bordercolor="#00D4FF",
                        font=dict(color="#e4e9ff", size=12, family="Menlo, monospace")),
        margin=dict(l=40, r=20, t=50, b=40),
    )
)
_pio.templates.default = "rodmat_neon"
# Colores default para plotly.express (matplotlib fallback)
try:
    import plotly.express as _px_theme
    _px_theme.defaults.color_discrete_sequence = ["#00D4FF", "#00FF88", "#FF9F45", "#7B61FF", "#FF3D6B", "#FF6B35"]
    _px_theme.defaults.template = "rodmat_neon"
except Exception:
    pass


from api_client import api_get, api_post, api_put, api_patch, api_delete, login, register

st.set_page_config(
    page_title="Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    /* ══════════════════════════════════════════════════════════════════
       🎨 PALETA FUTURISTA GLOBAL — Rodmat Dashboard
       Base #0a0e27 · Cyan #00D4FF · Green #00FF88 · Orange #FF9F45
       Red #FF6B35 · Crimson #FF3D6B · Purple #7B61FF
       ══════════════════════════════════════════════════════════════════ */

    /* Fondo global oscuro */
    .stApp { background: #0a0e27 !important; color: #e4e9ff; }
    [data-testid="stAppViewContainer"] { background: #0a0e27 !important; }
    [data-testid="stHeader"] { background: transparent !important; }
    .block-container { padding-top: 1rem; padding-bottom: 1rem; max-width: 100% !important; }

    /* Sidebar */
    [data-testid="stSidebar"] { background: #0f142f !important; border-right: 1px solid rgba(123,97,255,0.15); }
    [data-testid="stSidebar"] * { color: #e4e9ff !important; }
    [data-testid="stSidebar"] .stButton button {
        background: linear-gradient(135deg, #0f142f, #141a3d) !important;
        border: 1px solid rgba(0,212,255,0.3) !important; color: #00D4FF !important;
    }
    [data-testid="stSidebar"] .stButton button:hover {
        border-color: #00D4FF !important; box-shadow: 0 0 12px rgba(0,212,255,0.4);
    }

    /* Títulos gradientes */
    h1, h2 {
        background: linear-gradient(90deg, #00D4FF, #7B61FF);
        -webkit-background-clip: text !important;
        -webkit-text-fill-color: transparent !important;
        background-clip: text !important;
    }
    h3, h4, h5, h6 { color: #e4e9ff !important; }
    p, div, span, label, li { color: #e4e9ff; }

    /* Métricas Streamlit (st.metric) → cards con glow */
    [data-testid="stMetric"] {
        background: linear-gradient(135deg, rgba(15,20,47,0.9), rgba(20,26,61,0.9)) !important;
        padding: 14px !important;
        border-radius: 12px !important;
        border: 1px solid rgba(123,97,255,0.15) !important;
        border-left: 3px solid #00D4FF !important;
        box-shadow: 0 2px 8px rgba(0,0,0,0.3);
    }
    [data-testid="stMetricLabel"] p {
        color: #8892b0 !important; font-weight: 600 !important;
        font-size: 0.75rem !important; text-transform: uppercase; letter-spacing: 0.6px;
    }
    [data-testid="stMetricValue"] { color: #ffffff !important; font-size: 1.5rem !important; font-weight: 700 !important; }
    [data-testid="stMetricDelta"] { color: #00FF88 !important; font-size: 0.75rem !important; }

    /* Botones */
    .stButton > button {
        background: linear-gradient(135deg, #0f142f, #141a3d);
        border: 1px solid rgba(0,212,255,0.3); color: #00D4FF;
        font-weight: 600; border-radius: 8px; transition: all 0.2s;
    }
    .stButton > button:hover {
        border-color: #00D4FF; color: #ffffff;
        box-shadow: 0 0 12px rgba(0,212,255,0.4); transform: translateY(-1px);
    }
    .stButton > button[kind="primary"] {
        background: linear-gradient(90deg, #00D4FF, #7B61FF); color: #0a0e27;
        border: none; box-shadow: 0 0 14px rgba(0,212,255,0.35);
    }

    /* Tabs */
    button[data-baseweb="tab"] {
        color: #8892b0 !important; background: transparent !important;
        border-bottom: 2px solid transparent !important; font-weight: 600;
    }
    button[data-baseweb="tab"][aria-selected="true"] {
        color: #00D4FF !important; border-bottom-color: #00D4FF !important;
    }

    /* Radio (usado como tabs también) */
    [data-testid="stRadio"] label { color: #e4e9ff !important; }
    [data-testid="stRadio"] > div > label { color: #8892b0 !important; }

    /* Inputs / Selectbox */
    .stTextInput input, .stNumberInput input, .stDateInput input, .stSelectbox > div {
        background: rgba(15,20,47,0.6) !important; color: #e4e9ff !important;
        border: 1px solid rgba(123,97,255,0.2) !important;
    }
    .stTextInput label, .stNumberInput label, .stDateInput label, .stSelectbox label,
    .stMultiSelect label, .stTextArea label { color: #8892b0 !important; font-weight: 600 !important; }

    /* DataFrame / data_editor */
    [data-testid="stDataFrame"], [data-testid="stDataFrameResizable"] { background: rgba(15,20,47,0.5) !important; border-radius: 8px; }
    .stDataFrame table { color: #e4e9ff !important; }
    .stDataFrame thead th { background: rgba(0,212,255,0.1) !important; color: #00D4FF !important; }

    /* Alerts / info / warning / success */
    .stAlert { background: rgba(15,20,47,0.7) !important; border-radius: 8px; }
    [data-testid="stAlertContainer"] { background: rgba(15,20,47,0.7) !important; }
    div[data-baseweb="notification"][kind="info"] { border-left: 3px solid #00D4FF; }
    div[data-baseweb="notification"][kind="success"] { border-left: 3px solid #00FF88; }
    div[data-baseweb="notification"][kind="warning"] { border-left: 3px solid #FF9F45; }
    div[data-baseweb="notification"][kind="error"] { border-left: 3px solid #FF6B35; }

    /* Expander */
    [data-testid="stExpander"] {
        background: linear-gradient(135deg, rgba(15,20,47,0.9), rgba(20,26,61,0.9)) !important;
        border: 1px solid rgba(123,97,255,0.15) !important; border-radius: 10px !important;
    }

    /* Dividers */
    hr { border-color: rgba(123,97,255,0.15) !important; }

    /* Plotly charts — fondo transparente para que se integre */
    [data-testid="stPlotlyChart"] { background: rgba(15,20,47,0.4); border-radius: 12px; padding: 8px; }

    /* Plotly wrapper con glow y padding premium */
    [data-testid="stPlotlyChart"] {
        background: linear-gradient(135deg, rgba(15,20,47,0.9), rgba(20,26,61,0.9)) !important;
        border: 1px solid rgba(0,212,255,0.15) !important;
        border-radius: 14px !important;
        padding: 14px !important;
        box-shadow: 0 4px 20px rgba(0,212,255,0.08), inset 0 0 20px rgba(123,97,255,0.03) !important;
        margin-bottom: 12px !important;
        transition: box-shadow 0.3s ease;
    }
    [data-testid="stPlotlyChart"]:hover {
        box-shadow: 0 4px 24px rgba(0,212,255,0.18), inset 0 0 24px rgba(123,97,255,0.05) !important;
    }

    /* DataFrame — look premium con glow suave */
    [data-testid="stDataFrame"], [data-testid="stDataFrameResizable"], .stDataFrame {
        background: linear-gradient(135deg, rgba(15,20,47,0.9), rgba(20,26,61,0.9)) !important;
        border: 1px solid rgba(0,212,255,0.15) !important;
        border-radius: 12px !important;
        padding: 8px !important;
        box-shadow: 0 4px 16px rgba(0,212,255,0.06) !important;
    }
    .stDataFrame [data-testid="stTable"], .stDataFrame table {
        background: transparent !important;
        color: #e4e9ff !important;
        border-radius: 8px;
    }
    .stDataFrame thead tr th, .stDataFrame [role="columnheader"] {
        background: linear-gradient(180deg, rgba(0,212,255,0.12), rgba(0,212,255,0.04)) !important;
        color: #00D4FF !important;
        font-weight: 700 !important;
        text-transform: uppercase !important;
        letter-spacing: 0.5px !important;
        font-size: 11px !important;
        border-bottom: 1px solid rgba(0,212,255,0.2) !important;
    }
    .stDataFrame tbody tr td, .stDataFrame [role="cell"] {
        color: #e4e9ff !important;
        border-bottom: 1px solid rgba(123,97,255,0.06) !important;
        background: rgba(15,20,47,0.3) !important;
    }
    .stDataFrame tbody tr:nth-child(even) td { background: rgba(15,20,47,0.5) !important; }
    .stDataFrame tbody tr:hover td { background: rgba(0,212,255,0.06) !important; }

    /* data_editor mismo look */
    [data-testid="stDataEditor"] {
        background: linear-gradient(135deg, rgba(15,20,47,0.9), rgba(20,26,61,0.9)) !important;
        border: 1px solid rgba(0,212,255,0.15) !important;
        border-radius: 12px !important;
        padding: 8px !important;
        box-shadow: 0 4px 16px rgba(0,212,255,0.06) !important;
    }
    [data-testid="stDataEditor"] .glide-cell-header {
        background: rgba(0,212,255,0.1) !important;
        color: #00D4FF !important;
    }

    /* Text captions y small */
    [data-testid="stCaptionContainer"] { color: #8892b0 !important; }
    small { color: #8892b0 !important; }

    /* Section dividers */
    [data-testid="stMarkdownContainer"] h2, [data-testid="stMarkdownContainer"] h3 {
        border-bottom: 1px solid rgba(0,212,255,0.15);
        padding-bottom: 6px;
        margin-top: 12px;
    }

    /* Responsive mobile */
    @media (max-width: 768px) {
        [data-testid="stHorizontalBlock"] { flex-wrap: wrap !important; }
        [data-testid="stHorizontalBlock"] > div { flex: 1 1 45% !important; min-width: 45% !important; }
        [data-testid="stMetric"] { padding: 10px !important; margin-bottom: 0.25rem; }
        [data-testid="stMetricValue"] { font-size: 1.1rem !important; }
        .block-container { padding-left: 0.5rem; padding-right: 0.5rem; }
        button[data-baseweb="tab"] { font-size: 0.75rem !important; padding: 0.4rem 0.5rem !important; }
    }
    @media (max-width: 480px) {
        [data-testid="stHorizontalBlock"] > div { flex: 1 1 100% !important; min-width: 100% !important; }
        [data-testid="stMetricValue"] { font-size: 1rem !important; }
    }
</style>
""", unsafe_allow_html=True)


# ================================================================== #
#  CACHED API CALLS
# ================================================================== #
@st.cache_data(ttl=300)
def fetch_overview(date_from=None, date_to=None, platform=None, brand_slug=None):
    params = {}
    if date_from: params["date_from"] = str(date_from)
    if date_to: params["date_to"] = str(date_to)
    if platform: params["platform"] = platform
    if brand_slug: params["brand_slug"] = brand_slug
    return api_get("/analytics/overview", params) or {}

@st.cache_data(ttl=300)
def fetch_sales_by_month(date_from=None, date_to=None, platform=None, brand_slug=None):
    params = {}
    if date_from: params["date_from"] = str(date_from)
    if date_to: params["date_to"] = str(date_to)
    if platform: params["platform"] = platform
    if brand_slug: params["brand_slug"] = brand_slug
    return api_get("/analytics/sales-by-month", params) or []

@st.cache_data(ttl=300)
def fetch_sales_by_day(date_from=None, date_to=None, platform=None, brand_slug=None):
    params = {}
    if date_from: params["date_from"] = str(date_from)
    if date_to: params["date_to"] = str(date_to)
    if platform: params["platform"] = platform
    if brand_slug: params["brand_slug"] = brand_slug
    return api_get("/analytics/sales-by-day", params) or []

@st.cache_data(ttl=300)
def fetch_platform_summary(date_from=None, date_to=None, brand_slug=None):
    params = {}
    if date_from: params["date_from"] = str(date_from)
    if date_to: params["date_to"] = str(date_to)
    if brand_slug: params["brand_slug"] = brand_slug
    return api_get("/analytics/platform-summary", params) or {}

@st.cache_data(ttl=300)
def fetch_stock_summary(coverage_days=30, brand_slug=None):
    params = {"coverage_days": coverage_days}
    if brand_slug: params["brand_slug"] = brand_slug
    return api_get("/analytics/stock-summary", params) or []

@st.cache_data(ttl=300)
def fetch_stock_detail(coverage_days=30, brand_slug=None):
    params = {"coverage_days": coverage_days}
    if brand_slug: params["brand_slug"] = brand_slug
    return api_get("/analytics/stock-detail", params) or []

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
def fetch_top_combos(n=15, brand_slug=None):
    params = {"n": n}
    if brand_slug: params["brand_slug"] = brand_slug
    return api_get("/analytics/top-combos", params) or []

@st.cache_data(ttl=300)
def fetch_finances(brand_slug=None):
    params = {}
    if brand_slug: params["brand_slug"] = brand_slug
    return api_get("/analytics/finances", params) or []

@st.cache_data(ttl=300)
def fetch_incoming_stock(brand_slug=None):
    params = {}
    if brand_slug: params["brand_slug"] = brand_slug
    return api_get("/inventory/incoming", params) or []

@st.cache_data(ttl=300)
def fetch_fbt_inventory():
    return api_get("/inventory/fbt") or []

@st.cache_data(ttl=300)
def fetch_unknown_combos():
    return api_get("/analytics/unknown-combos") or []

@st.cache_data(ttl=300)
def fetch_combos(brand_slug=None):
    params = {}
    if brand_slug: params["brand_slug"] = brand_slug
    return api_get("/combos", params) or []


@st.cache_data(ttl=60)
def fetch_sku_maps(platform: str = "all"):
    """Devuelve mapeos walmart_sku_map + amazon_sku_map unificados."""
    return api_get("/sku-maps", {"platform": platform}) or []

@st.cache_data(ttl=300)
def fetch_products(brand_slug=None):
    params = {}
    if brand_slug: params["brand_slug"] = brand_slug
    return api_get("/products", params) or []
@st.cache_data(ttl=300)
def fetch_brands():
    """Lista brands activas del store del user."""
    return api_get("/brands") or []

@st.cache_data(ttl=300)
def fetch_product_brand_map():
    """SKU -> brand_slug map (para filtrado client-side)."""
    return api_get("/brands/product-map") or {}


@st.cache_data(ttl=300)
def fetch_combo_sales(date_from=None, date_to=None, brand_slug=None):
    params = {}
    if date_from: params["date_from"] = str(date_from)
    if date_to: params["date_to"] = str(date_to)
    if brand_slug: params["brand_slug"] = brand_slug
    return api_get("/analytics/combo-sales", params) or []

@st.cache_data(ttl=300)
def fetch_product_monthly_sales_pivot(year=None, brand_slug=None):
    params = {}
    if year: params["year"] = year
    if brand_slug: params["brand_slug"] = brand_slug
    return api_get("/analytics/product-monthly-sales-pivot", params) or {"years": [], "rows": []}


def fetch_combo_monthly_sales_pivot(year=None, brand_slug=None):
    params = {}
    if year: params["year"] = year
    if brand_slug: params["brand_slug"] = brand_slug
    return api_get("/analytics/combo-monthly-sales-pivot", params) or {"years": [], "rows": []}


def fetch_creator_monthly_pivot(year=None):
    params = {}
    if year: params["year"] = year
    return api_get("/analytics/creator-monthly-pivot", params) or {"years": [], "rows": []}


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
#  BRAND helpers (multi-brand support LuxPerfumes vs Avon)
# ================================================================== #
def get_current_brand_slug() -> str | None:
    """Return brand slug active for the current session.
    - If user has forced brand_id: return that slug (locked).
    - Else return session_state selection (or None = 'All').
    """
    user = st.session_state.get("cached_user") or {}
    if user.get("brand_slug"):
        return user["brand_slug"]  # locked by user record
    sel = st.session_state.get("selected_brand_slug")
    return sel if sel and sel != "__ALL__" else None

def filter_df_by_brand(df, sku_col: str = "sku"):
    """Filter a DataFrame client-side by the active brand.
    - No brand active OR no user OR brands_enabled=False -> pass-through.
    - Uses fetch_product_brand_map() for SKU -> brand_slug lookup.
    - Rows with SKU not in the map are DROPPED when brand filter active (safe default: never leak unknown SKUs).
    """
    import pandas as _pd
    if df is None or not hasattr(df, "empty") or df.empty:
        return df
    user = st.session_state.get("cached_user") or {}
    if not user.get("brands_enabled"):
        return df
    brand = get_current_brand_slug()
    if not brand:
        return df  # 'All' — no filter
    if sku_col not in df.columns:
        return df
    try:
        bmap = fetch_product_brand_map() or {}
    except Exception:
        return df
    mask = df[sku_col].astype(str).map(lambda s: bmap.get(s) == brand)
    return df[mask].reset_index(drop=True)


def apply_brand_filter_records(records, sku_key: str = "sku"):
    """Igual que filter_df_by_brand pero para lista de dicts (records) del API.
    - No filtra si brands_enabled=False o brand activa = 'Todas'.
    - Si brand activa: keep solo los records cuyo SKU mapee a esa brand.
    - Records con SKU no en product-map son DROPPED (safe default).
    """
    if not records:
        return records
    user = st.session_state.get("cached_user") or {}
    if not user.get("brands_enabled"):
        return records
    brand = get_current_brand_slug()
    if not brand:
        return records
    try:
        bmap = fetch_product_brand_map() or {}
    except Exception:
        return records
    return [r for r in records if bmap.get(str(r.get(sku_key, ""))) == brand]



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
    _u = st.session_state.get("cached_user") or {}
    _bs = get_current_brand_slug() if _u.get("brands_enabled") else None
    if _bs:
        _bn = next((b["display_name"] for b in (_u.get("available_brands") or []) if b["slug"] == _bs), _bs)
        _bc = next((b.get("brand_color") for b in (_u.get("available_brands") or []) if b["slug"] == _bs), "#8B4A9C")
        st.markdown(f"<div style='background:{_bc};color:#fff;padding:6px 12px;border-radius:6px;display:inline-block;font-weight:600;'>🏷 Filtrado: {_bn}</div>", unsafe_allow_html=True)
    _platform = render_platform_selector("ov")

    if _platform != "amazon":
        unknown = fetch_unknown_combos()
        if unknown:
            st.warning(f"{len(unknown)} SKU(s) en pedidos sin combo asignado. Ve a Gestion > Gestion Combos para revisarlos.")

    # Platform breakdown only for multi-platform tenants and when no filter set
    if _platform is None and len(get_enabled_platforms()) > 1:
        ps = fetch_platform_summary(brand_slug=_bs)
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

    m = fetch_overview(date_from, date_to, platform=_platform, brand_slug=_bs)
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

    monthly = fetch_sales_by_month(date_from, date_to, platform=_platform, brand_slug=_bs)
    st.subheader("GMV por Mes")
    if monthly:
        df_m = pd.DataFrame(monthly)
        # Filtro año — botones arriba, default: último año (mayor)
        df_m["_Year"] = df_m["Month"].astype(str).str[:4]
        years_avail = sorted(df_m["_Year"].dropna().unique().tolist(), reverse=True)
        year_opts = ["Todos"] + years_avail
        default_year = years_avail[0] if years_avail else "Todos"
        if "ov_gmv_year" not in st.session_state:
            st.session_state["ov_gmv_year"] = default_year
        yr_cols = st.columns(len(year_opts))
        for i, yl in enumerate(year_opts):
            with yr_cols[i]:
                btype = "primary" if st.session_state["ov_gmv_year"] == yl else "secondary"
                if st.button(yl, key=f"ov_gmv_y_{yl}", use_container_width=True, type=btype):
                    st.session_state["ov_gmv_year"] = yl
                    st.rerun()
        sel_year = st.session_state["ov_gmv_year"]
        if sel_year != "Todos":
            df_m = df_m[df_m["_Year"] == sel_year].reset_index(drop=True)
        # Stacked bar: Tienda (base) + Afiliados (encima)
        fig = go.Figure()
        if "GMV_Tienda" in df_m.columns:
            fig.add_trace(go.Bar(
                name="Ventas Tienda",
                x=df_m["Month"], y=df_m["GMV_Tienda"],
                marker_color="#00D4FF",
                text=df_m["GMV_Tienda"].apply(lambda v: f"${v/1000:.0f}k" if v > 0 else ""),
                textposition="inside",
                hovertemplate="<b>%{x}</b><br>Tienda: $%{y:,.0f}<extra></extra>",
            ))
        if "GMV_Afiliados" in df_m.columns:
            fig.add_trace(go.Bar(
                name="Ventas Afiliados",
                x=df_m["Month"], y=df_m["GMV_Afiliados"],
                marker_color="#7B61FF",
                text=df_m["GMV_Afiliados"].apply(lambda v: f"${v/1000:.0f}k" if v > 0 else ""),
                textposition="inside",
                hovertemplate="<b>%{x}</b><br>Afiliados: $%{y:,.0f}<extra></extra>",
            ))
        # Totales encima de cada barra
        if "GMV" in df_m.columns:
            fig.add_trace(go.Scatter(
                x=df_m["Month"], y=df_m["GMV"],
                mode="text",
                text=df_m["GMV"].apply(lambda v: f"${v/1000:.0f}k"),
                textposition="top center",
                textfont=dict(color="#e4e9ff", size=11, family="SF Mono, monospace"),
                showlegend=False, hoverinfo="skip",
            ))
        fig.update_layout(
            barmode="stack", height=380, margin=dict(t=10, b=10),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            xaxis_title="", yaxis_title="GMV ($)",
        )
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

    daily_raw = fetch_sales_by_day(date_from, date_to, platform=_platform, brand_slug=_bs)
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

    _u = st.session_state.get("cached_user") or {}
    _bs = get_current_brand_slug() if _u.get("brands_enabled") else None
    data = fetch_stock_summary(brand_slug=_bs)
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
    low_count = len(df[(df["Days_Coverage"] > 0) & (df["Days_Coverage"] < 7)]) if "Days_Coverage" in df.columns else 0
    c3.metric("Productos Stock Bajo", f"{low_count}")
    c4.metric("Stock Almacén", f"{int(df['Stock_Warehouse'].sum()):,}" if "Stock_Warehouse" in df.columns else "N/A")
    c5.metric("Stock FBT", f"{int(df['Stock_FBT'].sum()):,}" if "Stock_FBT" in df.columns else "N/A")

    if "Days_Coverage" in df.columns:
        low_3 = df[df["Days_Coverage"] > 0].nsmallest(3, "Days_Coverage")
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
    display_cols = ["ProductoNombre", "Tipo",
                    "Stock_Warehouse", "Stock_FBT", "QtyShipped",
                    "StockActualizado", "PedidosPendiente", "StockConPedidos",
                    "Sales_30d", "Days_Coverage", "Days_Cov_WH", "Days_Cov_FBT", "ValorInventario"]
    available = [c for c in display_cols if c in df.columns]
    display_df = df[available].sort_values("StockActualizado", ascending=True).copy()
    # Mostrar 0 como "—" en cobertura (visualmente más limpio, indica "sin ventas")
    for cov_col in ["Days_Coverage", "Days_Cov_WH", "Days_Cov_FBT"]:
        if cov_col in display_df.columns:
            display_df[cov_col] = display_df[cov_col].apply(
                lambda v: "—" if isinstance(v, (int, float)) and v == 0 else v
            )
    st.dataframe(display_df, use_container_width=True, height=400)

    # ─── Ventas Mensuales por Producto (pivot) ───
    st.markdown("---")
    st.subheader("📅 Ventas Mensuales por Producto")
    st.caption("Unidades vendidas por mes tras descomponer combos. Filtro por producto/categoría y año.")

    # Cargar datos (año actual por defecto)
    _pmsp_first = fetch_product_monthly_sales_pivot(brand_slug=_bs)
    years_pmsp = _pmsp_first.get("years", [])
    if years_pmsp:
        col_y1, col_y2, col_y3 = st.columns([1, 2, 2])
        with col_y1:
            year_sel = st.selectbox("Año", years_pmsp, index=0, key="pmsp_year")
        pmsp_data = fetch_product_monthly_sales_pivot(year=year_sel, brand_slug=_bs)
        rows_p = pmsp_data.get("rows", [])
        if rows_p:
            df_p = pd.DataFrame(rows_p)
            with col_y2:
                cat_opts = ["Todas"] + sorted(df_p["categoria"].dropna().unique().tolist())
                cat_f = st.selectbox("Categoría", cat_opts, key="pmsp_cat")
            with col_y3:
                prod_opts = ["Todos"] + sorted(df_p["producto"].dropna().unique().tolist())
                prod_f = st.selectbox("Producto", prod_opts, key="pmsp_prod")
            if cat_f != "Todas": df_p = df_p[df_p["categoria"] == cat_f]
            if prod_f != "Todos": df_p = df_p[df_p["producto"] == prod_f]

            # Reordenar columnas + rename para display
            month_cols = [f"m{m:02d}" for m in range(1, 13)]
            display_p_cols = ["producto", "categoria"] + month_cols + ["total"]
            display_p_cols = [c for c in display_p_cols if c in df_p.columns]
            month_rename = {f"m{m:02d}": f"{m:02d}" for m in range(1, 13)}
            rename_p = {"producto": "Producto", "categoria": "Categoría", "total": "TOTAL", **month_rename}
            st.dataframe(df_p[display_p_cols].rename(columns=rename_p),
                         use_container_width=True, height=400,
                         column_config={"Producto": st.column_config.TextColumn(pinned=True, width="medium")})
            st.caption(f"📊 {len(df_p)} productos · Total unidades: {int(df_p['total'].sum()):,}")
        else:
            st.info(f"Sin ventas de productos en {year_sel}.")
    else:
        st.info("No hay ventas de productos registradas todavía.")

    # ─── Ventas Mensuales por Combo (pivot) ───
    st.markdown("---")
    st.subheader("📅 Ventas Mensuales por Combo")
    st.caption("Unidades vendidas por combo (Seller SKU + descripción, nivel de orden sin descomponer). Filtro por SKU/descripción y año.")

    _cmsp_first = fetch_combo_monthly_sales_pivot(brand_slug=_bs)
    years_cmsp = _cmsp_first.get("years", [])
    if years_cmsp:
        col_c1, col_c2 = st.columns([1, 3])
        with col_c1:
            year_c = st.selectbox("Año", years_cmsp, index=0, key="cmsp_year")
        cmsp_data = fetch_combo_monthly_sales_pivot(year=year_c, brand_slug=_bs)
        rows_c = cmsp_data.get("rows", [])
        if rows_c:
            df_c = pd.DataFrame(rows_c)
            with col_c2:
                search = st.text_input("Buscar por SKU o descripción", "", key="cmsp_search")
            if search:
                mask = (df_c["sku"].str.contains(search, case=False, na=False) |
                        df_c["descripcion"].str.contains(search, case=False, na=False))
                df_c = df_c[mask]

            month_cols = [f"m{m:02d}" for m in range(1, 13)]
            display_c_cols = ["sku", "descripcion"] + month_cols + ["total"]
            display_c_cols = [c for c in display_c_cols if c in df_c.columns]
            month_rename = {f"m{m:02d}": f"{m:02d}" for m in range(1, 13)}
            rename_c = {"sku": "SKU", "descripcion": "Descripción", "total": "TOTAL", **month_rename}
            st.dataframe(df_c[display_c_cols].rename(columns=rename_c),
                         use_container_width=True, height=400,
                         column_config={"SKU": st.column_config.TextColumn(pinned=True, width="small"),
                                        "Descripción": st.column_config.TextColumn(width="large")})
            st.caption(f"📊 {len(df_c)} combos · Total unidades: {int(df_c['total'].sum()):,}")
        else:
            st.info(f"Sin ventas de combos en {year_c}.")
    else:
        st.info("No hay ventas de combos registradas todavía.")


# ================================================================== #
#  PAGE 3: RESTOCK ANALYSIS
# ================================================================== #
def page_restock_analysis():
    st.header("Análisis de Reabastecimiento")

    col_s1, col_s2 = st.columns(2)
    with col_s1:
        coverage = st.number_input("Días de cobertura objetivo", min_value=7, max_value=180, value=30, key="ra_cov")
    with col_s2:
        _u = st.session_state.get("cached_user") or {}
        _bs = get_current_brand_slug() if _u.get("brands_enabled") else None
        data_all = fetch_stock_detail(30, brand_slug=_bs)
        tipo_opts = ["Todos"]
        if data_all:
            df_all = pd.DataFrame(data_all)
            if "Tipo" in df_all.columns:
                tipo_opts += sorted(df_all["Tipo"].dropna().unique().tolist())
        tipo_filter = st.selectbox("Tipo de Producto", tipo_opts, key="ra_tipo")

    data = fetch_stock_detail(coverage, brand_slug=_bs)
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
        # Fórmula NUEVA (spec finanzas): Importe = cajas × (coste_unitario × unidades_por_caja)
        # = cajas × coste_por_caja. Compras cajas enteras, no unidades exactas.
        if "Cajas_custom" in df.columns and "UNIDADES POR CAJA" in df.columns:
            df["CostePorCaja"] = (df["Coste"] * df["UNIDADES POR CAJA"]).round(2)
            df["Importe_custom"] = (df["Cajas_custom"] * df["CostePorCaja"]).round(2)
        else:
            # Fallback si no hay UNIDADES POR CAJA
            df["Importe_custom"] = (df["Unid_a_comprar_custom"] * df["Coste"]).round(2)
        # Filtrar KPI solo a productos a comprar (evita ruido de productos con 0 cajas)
        _to_buy = df[df["Unid_a_comprar_custom"] > 0]
        _total = _to_buy["Importe_custom"].sum() if not _to_buy.empty else 0
        st.metric("Total Importe a Comprar", f"${_total:,.2f}")

    st.markdown("---")
    st.subheader("Análisis Total Stock")
    table_cols = ["ProductoNombre", "Tipo", "Stock_Warehouse", "Stock_FBT",
                  "SalesInPeriod", "StockActualizado", "StockConPedidos",
                  "WeeklyAvg_30d", "WeeklyAvg_60d", "Inv_deseado_custom", "Unid_a_comprar_custom",
                  "Days_Coverage", "Days_Cov_WH", "Days_Cov_FBT", "SellThroughRate"]
    available = [c for c in table_cols if c in df.columns]

    # Cast enteros donde aplica (evita .000000)
    _int_cols = ["Stock_Warehouse", "Stock_FBT", "SalesInPeriod", "StockActualizado",
                 "StockConPedidos", "Inv_deseado_custom", "Unid_a_comprar_custom",
                 "Days_Coverage", "Days_Cov_WH", "Days_Cov_FBT"]
    for _c in _int_cols:
        if _c in df.columns:
            df[_c] = pd.to_numeric(df[_c], errors="coerce").fillna(0).astype(int)

    def color_coverage(val):
        if not isinstance(val, (int, float)):
            return ""
        if val <= -999:
            return "background-color: rgba(120,130,150,0.15); color: #8892b0"
        elif val < 0:
            return "background-color: rgba(255,107,53,0.25); color: #FF6B35"
        elif val < 7:
            return "background-color: rgba(255,61,107,0.2); color: #FF3D6B"
        elif val < 14:
            return "background-color: rgba(255,159,69,0.2); color: #FF9F45"
        elif val >= 365:
            return "background-color: rgba(123,97,255,0.15); color: #B47CFF"
        else:
            return "background-color: rgba(0,255,136,0.15); color: #00FF88"

    cov_cols = [c for c in ["Days_Coverage", "Days_Cov_WH", "Days_Cov_FBT"] if c in available]
    styled = df[available].style.map(color_coverage, subset=cov_cols) if cov_cols else df[available].style
    # Format sin decimales para columnas numéricas + 2 decimales para ratios
    _fmt = {c: "{:,.0f}" for c in _int_cols if c in available}
    if "WeeklyAvg_30d" in available: _fmt["WeeklyAvg_30d"] = "{:,.1f}"
    if "WeeklyAvg_60d" in available: _fmt["WeeklyAvg_60d"] = "{:,.1f}"
    if "SellThroughRate" in available: _fmt["SellThroughRate"] = "{:,.1f}%"
    styled = styled.format(_fmt)

    st.dataframe(styled, use_container_width=True, height=380,
                 column_config={
                     "ProductoNombre": st.column_config.TextColumn("Producto", pinned=True, width="medium"),
                     "Tipo": st.column_config.TextColumn("Tipo", width="small"),
                     "Stock_Warehouse": st.column_config.NumberColumn("Stock WH", width="small"),
                     "Stock_FBT": st.column_config.NumberColumn("FBT", width="small"),
                     "SalesInPeriod": st.column_config.NumberColumn(f"Vendido {coverage}d", width="small"),
                     "StockActualizado": st.column_config.NumberColumn("Stock Act.", width="small"),
                     "StockConPedidos": st.column_config.NumberColumn("+Pedidos", width="small"),
                     "WeeklyAvg_30d": st.column_config.NumberColumn("Sem 30d", width="small"),
                     "WeeklyAvg_60d": st.column_config.NumberColumn("Sem 60d", width="small"),
                     "Inv_deseado_custom": st.column_config.NumberColumn("Deseado", width="small"),
                     "Unid_a_comprar_custom": st.column_config.NumberColumn("A comprar", width="small"),
                     "Days_Coverage": st.column_config.NumberColumn("Días cob.", width="small"),
                     "Days_Cov_WH": st.column_config.NumberColumn("Cob. WH", width="small"),
                     "Days_Cov_FBT": st.column_config.NumberColumn("Cob. FBT", width="small"),
                     "SellThroughRate": st.column_config.NumberColumn("STR", width="small"),
                 })

    st.markdown("---")
    st.subheader("Listado de Pedido (Purchase Order)")
    order_list = df[df["Unid_a_comprar_custom"] > 0].copy()
    if not order_list.empty:
        # Fórmulas nuevas según spec finanzas:
        # Coste por caja = coste_unitario × unidades_caja
        # Importe = cajas × coste_por_caja
        if "Coste" in order_list.columns and "UNIDADES POR CAJA" in order_list.columns:
            order_list["CostePorCaja"] = (order_list["Coste"] * order_list["UNIDADES POR CAJA"]).round(2)
            if "Cajas_custom" in order_list.columns:
                order_list["ImporteTotal"] = (order_list["Cajas_custom"] * order_list["CostePorCaja"]).round(2)
        # Orden nuevo: Unidades a comprar → Unidades por caja → Cajas a comprar → Coste por caja → Importe
        order_cols = ["ProductoNombre", "Unid_a_comprar_custom"]
        if "UNIDADES POR CAJA" in order_list.columns: order_cols.append("UNIDADES POR CAJA")
        if "Cajas_custom" in order_list.columns: order_cols.append("Cajas_custom")
        if "CostePorCaja" in order_list.columns: order_cols.append("CostePorCaja")
        if "ImporteTotal" in order_list.columns:
            order_cols.append("ImporteTotal")
        elif "Importe_custom" in order_list.columns:
            order_cols.append("Importe_custom")
        rename_map = {
            "ProductoNombre": "Producto",
            "Unid_a_comprar_custom": "Unidades a comprar",
            "UNIDADES POR CAJA": "Unidades por caja",
            "Cajas_custom": "Cajas a comprar",
            "CostePorCaja": "Coste por caja",
            "ImporteTotal": "Importe",
            "Importe_custom": "Importe",
        }
        st.dataframe(order_list[order_cols].rename(columns=rename_map),
                     use_container_width=True,
                     column_config={
                         "Coste por caja": st.column_config.NumberColumn(format="$%.2f"),
                         "Importe": st.column_config.NumberColumn(format="$%.2f"),
                     })
        # Total importe abajo
        if "ImporteTotal" in order_list.columns:
            total = order_list["ImporteTotal"].sum()
            st.caption(f"💰 **Importe total pedido: ${total:,.2f}**")
        csv = order_list[order_cols].rename(columns=rename_map).to_csv(index=False).encode("utf-8")
        st.download_button("Descargar Purchase Order CSV", data=csv,
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
    _u = st.session_state.get("cached_user") or {}
    _bs = get_current_brand_slug() if _u.get("brands_enabled") else None
    if _bs:
        _bn = next((b["display_name"] for b in (_u.get("available_brands") or []) if b["slug"] == _bs), _bs)
        st.info(f"ℹ️ Filtro marca activo: **{_bn}**. Las métricas de creators/afiliados son agregadas del store (los creators pueden generar ventas cross-brand).")
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

        st.subheader("Creadores por Mes (Pivot) — con muestras y ventas anuales")
        # NEW: pivot con muestras_gratis + muestras_compradas + ventas mes 01-12 + total
        _cmp_first = fetch_creator_monthly_pivot()
        years_cmp = _cmp_first.get("years", [])
        if years_cmp:
            col_y_af1, col_y_af2 = st.columns([1, 3])
            with col_y_af1:
                year_af = st.selectbox("Año", years_cmp, index=0, key="cmp_year")
            cmp_data = fetch_creator_monthly_pivot(year=year_af)
            rows_cmp = cmp_data.get("rows", [])
            if rows_cmp:
                df_cmp = pd.DataFrame(rows_cmp)
                with col_y_af2:
                    creator_search = st.text_input("Buscar creator", "", key="cmp_search")
                if creator_search:
                    df_cmp = df_cmp[df_cmp["creator"].str.contains(creator_search, case=False, na=False)]

                month_cols = [f"m{m:02d}" for m in range(1, 13)]
                display_cmp_cols = ["creator", "muestras_gratis", "muestras_compradas"] + month_cols + ["total"]
                display_cmp_cols = [c for c in display_cmp_cols if c in df_cmp.columns]
                month_rename = {f"m{m:02d}": f"{m:02d}" for m in range(1, 13)}
                rename_cmp = {
                    "creator": "Creator Username",
                    "muestras_gratis": "Muestras gratis",
                    "muestras_compradas": "Muestras compradas",
                    "total": "TOTAL Ventas Año",
                    **month_rename,
                }
                # Column config para formato moneda en columnas mes + total
                cc = {"Creator Username": st.column_config.TextColumn(pinned=True, width="medium"),
                      "Muestras gratis": st.column_config.NumberColumn(width="small"),
                      "Muestras compradas": st.column_config.NumberColumn(width="small"),
                      "TOTAL Ventas Año": st.column_config.NumberColumn(format="$%.0f", width="small")}
                for m in range(1, 13):
                    cc[f"{m:02d}"] = st.column_config.NumberColumn(format="$%.0f", width="small")
                st.dataframe(df_cmp[display_cmp_cols].rename(columns=rename_cmp),
                             use_container_width=True, height=400,
                             column_config=cc)
                st.caption(f"📊 {len(df_cmp)} creators · Total GMV año: ${df_cmp['total'].sum():,.0f} · Muestras gratis: {int(df_cmp['muestras_gratis'].sum())} · Muestras compradas: {int(df_cmp['muestras_compradas'].sum())}")

                # Line chart top 5 (mantener)
                top5_names = df_cmp.nlargest(5, "total")["creator"].tolist()
                if top5_names:
                    top5_data = []
                    for _, r in df_cmp[df_cmp["creator"].isin(top5_names)].iterrows():
                        for m in range(1, 13):
                            top5_data.append({"Creator": r["creator"], "Mes": f"{m:02d}",
                                              "GMV": r[f"m{m:02d}"]})
                    df_top5 = pd.DataFrame(top5_data)
                    fig = px.line(df_top5, x="Mes", y="GMV", color="Creator", markers=True)
                    fig.update_layout(height=350, margin=dict(t=10))
                    st.plotly_chart(fig, use_container_width=True, key="af_line_monthly_v2")
            else:
                st.info(f"Sin datos de creadores en {year_af}.")

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
    _u = st.session_state.get("cached_user") or {}
    _bs = get_current_brand_slug() if _u.get("brands_enabled") else None
    _raw = fetch_finances(brand_slug=_bs)
    # Brand filter: si el dict tiene sku, filtrar; sino passthrough
    data = apply_brand_filter_records(_raw, sku_key="sku") if _raw and _raw[0].get("sku") else _raw
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
    _u2 = st.session_state.get("cached_user") or {}
    _bs2 = get_current_brand_slug() if _u2.get("brands_enabled") else None
    combos = fetch_top_combos(20, brand_slug=_bs2)
    if combos:
        st.dataframe(pd.DataFrame(combos), use_container_width=True, height=400)


# ================================================================== #
#  PAGE 7: CUPONES
# ================================================================== #
def page_cupones():
    st.header("Analisis Cupones")
    _u = st.session_state.get("cached_user") or {}
    _bs = get_current_brand_slug() if _u.get("brands_enabled") else None
    if _bs:
        _bn = next((b["display_name"] for b in (_u.get("available_brands") or []) if b["slug"] == _bs), _bs)
        st.info(f"ℹ️ Filtro marca activo: **{_bn}**. Los frequent buyers son agregados del store.")
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
    orders = apply_brand_filter_records(orders, sku_key="sku") if orders else orders
    total = len(orders)
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

    _u = st.session_state.get("cached_user") or {}
    _bs = get_current_brand_slug() if _u.get("brands_enabled") else None
    data = fetch_incoming_stock(brand_slug=_bs)
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

    _u = st.session_state.get("cached_user") or {}
    _bs = get_current_brand_slug() if _u.get("brands_enabled") else None
    products = fetch_products(brand_slug=_bs)
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

    # column_config con "id": None a veces PIERDE los IDs en edited.iterrows.
    # Ahora mantenemos id oculto pero conservado, y sacamos brand_id/created_at/updated_at con column_order.
    edited = st.data_editor(
        df, num_rows="dynamic",
        column_config={
            "price_cost": st.column_config.NumberColumn("Coste", min_value=0, format="$%.2f"),
            "price_sale": st.column_config.NumberColumn("Precio", min_value=0, format="$%.2f"),
            "units_per_box": st.column_config.NumberColumn("Unid/Caja", min_value=0),
            "status": st.column_config.SelectboxColumn("Status", options=["active", "inactive"], default="active"),
        },
        disabled=["id", "store_id", "created_at", "updated_at", "brand_id"],
        column_order=["sku", "name", "category", "price_cost", "price_sale",
                      "units_per_box", "supplier", "status"],
        use_container_width=True,
        height=500,
        key="productos_editor",
    )

    col_save, col_info = st.columns([1, 3])
    with col_save:
        if st.button("Guardar productos", type="primary", key="save_productos"):
            saved, created, errors, skipped_no_id = 0, 0, 0, 0
            error_msgs = []
            # Detectar solo filas modificadas comparando contra original
            orig_by_id = {p["id"]: p for p in (products or []) if p.get("id")}
            for _, row in edited.iterrows():
                product_id = row.get("id")
                if product_id and pd.notna(product_id) and str(product_id).strip():
                    # UPDATE — solo enviar campos que cambiaron respecto al original
                    orig = orig_by_id.get(str(product_id), {})
                    update_data = {}
                    for field in ["name", "category", "price_cost", "price_sale",
                                  "units_per_box", "supplier", "status"]:
                        val = row.get(field)
                        if val is None or (isinstance(val, float) and pd.isna(val)):
                            continue
                        # Cast a tipos JSON-safe (numpy → python)
                        if field in ("price_cost", "price_sale"):
                            val = float(val)
                        elif field == "units_per_box":
                            val = int(val)
                        else:
                            val = str(val).strip() if not isinstance(val, str) else val.strip()
                        if orig.get(field) != val:  # solo campos cambiados
                            update_data[field] = val
                    if update_data:
                        result = api_put(f"/products/{product_id}", update_data)
                        if result:
                            saved += 1
                        else:
                            errors += 1
                            error_msgs.append(f"❌ PUT falló: sku={row.get('sku')} data={update_data}")
                else:
                    sku = row.get("sku")
                    name = row.get("name")
                    if sku and name and pd.notna(sku) and pd.notna(name):
                        new_data = {
                            "sku": str(sku).strip(),
                            "name": str(name).strip(),
                            "category": (str(row.get("category")).strip() if pd.notna(row.get("category", None)) else None),
                            "price_cost": float(row["price_cost"]) if pd.notna(row.get("price_cost")) else None,
                            "price_sale": float(row["price_sale"]) if pd.notna(row.get("price_sale")) else None,
                            "units_per_box": int(row["units_per_box"]) if pd.notna(row.get("units_per_box")) else None,
                            "supplier": (str(row.get("supplier")).strip() if pd.notna(row.get("supplier", None)) else None),
                        }
                        result = api_post("/products", new_data)
                        if result:
                            created += 1
                        else:
                            errors += 1
                            error_msgs.append(f"❌ POST falló: sku={sku}")
                    else:
                        skipped_no_id += 1
            # Mostrar resumen detallado
            if saved or created:
                st.success(f"✅ {saved} actualizados · {created} creados")
            else:
                st.info("ℹ️ Sin cambios detectados (nada que guardar). Modifica alguna celda y vuelve a pulsar.")
            if errors:
                st.error(f"⚠️ {errors} errores:")
                for m in error_msgs[:5]:
                    st.error(m)
            if skipped_no_id:
                st.warning(f"{skipped_no_id} filas sin SKU/nombre — no guardadas.")
            st.cache_data.clear()
            if saved or created:
                st.rerun()
    with col_info:
        st.info(f"Total productos: {len(edited)}")


# ================================================================== #
#  PAGE 11: GESTION COMBOS
# ================================================================== #
def page_gestion_combos():
    st.header("Gestion Combos y Mapeos")
    st.caption("Editor unificado de combos TikTok, mapeos Walmart y Amazon. Todos los SKUs con multiplicador N × producto base.")

    # ─── Cargar datos comunes (brand-aware) ───
    _u_gc = st.session_state.get('cached_user') or {}
    _bs_gc = get_current_brand_slug() if _u_gc.get('brands_enabled') else None
    combos = fetch_combos(brand_slug=_bs_gc)
    sku_maps = fetch_sku_maps("all")
    products_data = fetch_products(brand_slug=_bs_gc)
    product_names = sorted([p["name"] for p in products_data]) if products_data else []
    product_map = {p["name"]: p["id"] for p in products_data} if products_data else {}

    # ═══════════════════════════════════════════════════════════════
    # BLOQUE 1 — SKUs sin asignar + asistente para asignarlos
    # ═══════════════════════════════════════════════════════════════
    unknown = fetch_unknown_combos()
    if unknown:
        n = len(unknown)
        st.warning(f"⚠️ {n} SKU(s) vendidos SIN mapear — asígnalos abajo o quedarán descontando 1 al azar")
        st.dataframe(pd.DataFrame(unknown), use_container_width=True, height=min(240, 50 + 35 * n))

        # ─── Quick-add: Asignar combo nuevo (restaurado del pre-FASE B) ───
        st.subheader("➕ Asignar combo nuevo")
        st.caption("Para SKUs huérfanos vendidos. Elige el SKU, cuántos productos incluye y el desglose.")
        sku_options = [r.get("seller_sku", "") for r in unknown if r.get("seller_sku")]

        selected_sku = st.selectbox("SKU a asignar", [""] + sku_options, key="combo_assign_sku")
        if selected_sku:
            row_data = next((r for r in unknown if r.get("seller_sku") == selected_sku), {})
            st.caption(f"Producto tentativo (del CSV): {row_data.get('product_name', 'N/A')}  ·  Órdenes vistas: {row_data.get('order_count', 0)}")

            plataforma = st.selectbox(
                "Plataforma de este SKU",
                options=["tiktok (combo multi-producto)", "amazon (SKU con units_per_sale)", "walmart (SKU con units_per_sale)"],
                key="combo_assign_platform",
            )

            if plataforma.startswith("tiktok"):
                n_products = st.number_input(
                    "¿Cuántos productos DISTINTOS contiene este combo?",
                    min_value=1, max_value=12, value=1, step=1, key="combo_n_prods",
                )
                selected_products = []
                selected_qtys = []
                prod_cols = st.columns(min(int(n_products), 4))
                for i in range(int(n_products)):
                    with prod_cols[i % len(prod_cols)]:
                        p = st.selectbox(f"Producto {i+1}", [""] + product_names,
                                         key=f"combo_prod_{i}")
                        q = st.number_input(f"Cantidad {i+1}", min_value=1, max_value=99, value=1, step=1,
                                            key=f"combo_qty_{i}")
                        selected_products.append(p)
                        selected_qtys.append(q)

                if st.button("💾 Agregar combo y guardar", type="primary", key="btn_add_combo"):
                    valid = [(p, q) for p, q in zip(selected_products, selected_qtys) if p]
                    if not valid:
                        st.error("Selecciona al menos un producto.")
                    else:
                        items = [{"product_id": product_map[p], "quantity": int(q)} for p, q in valid if p in product_map]
                        result = api_post("/combos", {
                            "combo_sku":  selected_sku,
                            "combo_name": row_data.get("product_name", selected_sku),
                            "items":      items,
                        })
                        if result:
                            st.success(f"✅ Combo '{selected_sku}' creado con {len(items)} producto(s).")
                            st.cache_data.clear()
                            st.rerun()
                        else:
                            st.error("Error creando combo.")
            else:
                # amazon/walmart → mapeo directo con units_per_sale
                plat_key = "amazon" if plataforma.startswith("amazon") else "walmart"
                col_p, col_u = st.columns([3, 1])
                with col_p:
                    prod_sel = st.selectbox("Producto base", [""] + product_names, key="mapeo_prod")
                with col_u:
                    ups = st.number_input("Units per sale", min_value=1, max_value=99, value=1, step=1, key="mapeo_ups")

                if st.button(f"💾 Crear mapeo {plat_key} y guardar", type="primary", key="btn_add_mapeo"):
                    if not prod_sel:
                        st.error("Selecciona un producto.")
                    else:
                        pid = product_map.get(prod_sel)
                        result = api_post("/sku-maps", {
                            "platform":       plat_key,
                            "external_sku":   selected_sku,
                            "product_id":     pid,
                            "units_per_sale": int(ups),
                        })
                        if result:
                            st.success(f"✅ Mapeo {plat_key} '{selected_sku}' → {prod_sel} (×{ups}) creado.")
                            st.cache_data.clear()
                            st.rerun()
                        else:
                            st.error("Error creando mapeo.")
    else:
        st.success("✅ Todos los SKUs vendidos están mapeados.")

    st.markdown("---")

    # ═══════════════════════════════════════════════════════════════
    # BLOQUE 2 — Editor unificado (mapeos existentes)
    # ═══════════════════════════════════════════════════════════════
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

    st.subheader("📋 Todos los mapeos activos")
    st.caption(f"Total: {len(rows)} mapeos ({len(combos)} combos TikTok, {sum(1 for m in sku_maps if m['platform']=='walmart')} Walmart, {sum(1 for m in sku_maps if m['platform']=='amazon')} Amazon). Edita/borra aquí; para asignar rápido combos nuevos usa el asistente de arriba.")

    df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=["id","Plataforma","SKU","Producto","Cantidad","_source","_item_id"])

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
            "_source":    None,
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
                    edited_ids.add(str(rid).strip())
                    orig = next((o for o in rows if o["id"] == rid), None)
                    if orig and (orig["Plataforma"]!=plat or orig["SKU"]!=sku or
                                 orig["Producto"]!=prod or orig["Cantidad"]!=qty):
                        if plat == "tiktok":
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
    _u = st.session_state.get("cached_user") or {}
    _bs = get_current_brand_slug() if _u.get("brands_enabled") else None
    if _bs:
        _bn = next((b["display_name"] for b in (_u.get("available_brands") or []) if b["slug"] == _bs), _bs)
        st.warning(f"⚠️ Filtro marca **{_bn}** activo pero el P&L de esta página es global del store todavía. F6 (P&L per-brand) queda pendiente para semana próxima.")

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

    # ─── Cache tenant safety: si cambia el user o store, limpiar cache global ───
    # st.cache_data es un cache global compartido entre sesiones/usuarios de la
    # misma instancia Streamlit. Sin este check, un user de Nokal vería datos
    # cacheados de Rodmat cuando otro dev había entrado antes con Rodmat.
    # Brand marker included (multi-brand: cache MUST reset when brand changes)
    _brand_marker = st.session_state.get("selected_brand_slug", "__ALL__") if user.get("brands_enabled") else "-"
    _uid_marker = f"{user.get('email','')}:{user.get('store_id','')}:{user.get('brand_slug') or _brand_marker}"
    if st.session_state.get("_last_uid_marker") != _uid_marker:
        st.cache_data.clear()
        st.session_state["_last_uid_marker"] = _uid_marker

    _store_label = user.get("store_name") or "Dashboard"
    st.title(f"{_store_label} Dashboard")

    with st.sidebar:
        st.write(f"**{user.get('email', '')}**")
        store_name = user.get("store_name", user.get("store_id", "")[:8])
        st.caption(f"Tienda: {store_name}")

        # ─── Selector de MARCA (solo si multi-brand activo para el store) ───
        if user.get("brands_enabled"):
            _av_brands = user.get("available_brands") or fetch_brands() or []
            _user_brand_slug = user.get("brand_slug")
            if _user_brand_slug:
                # Locked to a specific brand (usuario asociada a una marca)
                _b_disp = next((b["display_name"] for b in _av_brands if b["slug"] == _user_brand_slug), _user_brand_slug)
                _b_col = next((b.get("brand_color") for b in _av_brands if b["slug"] == _user_brand_slug), "#8B4A9C")
                st.markdown(
                    f"<div style='background:{_b_col};color:#fff;padding:6px 10px;border-radius:6px;"
                    f"font-weight:600;text-align:center;margin:8px 0;'>🏷 {_b_disp}</div>",
                    unsafe_allow_html=True,
                )
                st.session_state["selected_brand_slug"] = _user_brand_slug
            else:
                # Admin can pick any brand
                _labels = ["Todas"] + [b["display_name"] for b in _av_brands]
                _slugs  = ["__ALL__"] + [b["slug"] for b in _av_brands]
                _current = st.session_state.get("selected_brand_slug") or "__ALL__"
                _idx = _slugs.index(_current) if _current in _slugs else 0
                _sel = st.selectbox("🏷 Marca", _labels, index=_idx, key="_brand_select_widget")
                _new_slug = _slugs[_labels.index(_sel)]
                if _new_slug != st.session_state.get("selected_brand_slug"):
                    st.session_state["selected_brand_slug"] = _new_slug
                    st.cache_data.clear()
                    st.rerun()

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
    _chat_ai_enabled = (user.get("email") or "").lower() in CHAT_AI_ALLOWED
    if _role in ("admin", "superadmin"):
        _sections = ["Dashboard", "Gestion"]
        if _finance_enabled:
            _sections.append("Finance")
        if _chat_ai_enabled:
            _sections.append("Chat IA")
    elif _role == "warehouse":
        _sections = ["Gestion"]
    else:
        _sections = ["Dashboard", "Gestion"]
        if _chat_ai_enabled:
            _sections.append("Chat IA")
    # Preservar section elegido a través de reruns (ej: cambio de brand)
    _prev_section = st.session_state.get("_last_section")
    _default_idx = _sections.index(_prev_section) if _prev_section in _sections else 0
    section = st.sidebar.radio("Sección", _sections, index=_default_idx, key="_section_radio")
    # FIX: force rerun cuando section cambia — evita DOM stale de hoja anterior
    if _prev_section and _prev_section != section:
        st.session_state["_last_section"] = section
        st.rerun()
    st.session_state["_last_section"] = section

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
            _tiktok_stmt_enabled = bool(_modules.get("tiktok_statements", False))
            if _tiktok_stmt_enabled:
                _fin_view = st.radio("Vista Finance", ["P&L Operacional", "⚡ TikTok Statements"],
                                     horizontal=True, key="_fin_view_radio")
                st.markdown("---")
                if _fin_view == "P&L Operacional":
                    page_finance_pl()
                else:
                    page_finance_tiktok_statements()
            else:
                page_finance_pl()

    elif section == "Chat IA":
        page_chat_ai()



# ============================================================================
#  FINANCE → TikTok Statements (nueva hoja, paleta futurista)
#  Gate: user.modules_enabled.tiktok_statements=true
# ============================================================================
def page_finance_tiktok_statements():
    """Página Finance TikTok Statements — dark futurista con datos reales de BD."""
    import pandas as _pd

    # ------- CSS custom paleta futurista -------
    st.markdown("""
    <style>
    .stt-h1{font-size:22px;font-weight:700;background:linear-gradient(90deg,#00D4FF,#7B61FF);-webkit-background-clip:text;-webkit-text-fill-color:transparent;margin:8px 0 4px;}
    .stt-sub{color:#8892b0;font-size:13px;margin-bottom:16px;}
    .stt-card{background:linear-gradient(135deg,rgba(15,20,47,.9),rgba(20,26,61,.9));border:1px solid rgba(123,97,255,.15);border-radius:12px;padding:16px;margin-bottom:12px;}
    .stt-card h3{color:#8892b0;font-size:12px;text-transform:uppercase;letter-spacing:1px;margin:0 0 10px;font-weight:700;}
    .stt-kpi{background:linear-gradient(135deg,#0f142f,#141a3d);border:1px solid rgba(123,97,255,.15);border-radius:12px;padding:14px;position:relative;overflow:hidden;text-align:left;}
    .stt-kpi::before{content:"";position:absolute;top:0;left:0;right:0;height:2px;background:linear-gradient(90deg,transparent,var(--accent,#00D4FF),transparent);opacity:.7;}
    .stt-kpi-l{color:#8892b0;font-size:11px;text-transform:uppercase;letter-spacing:.5px;}
    .stt-kpi-v{color:#fff;font-size:22px;font-weight:700;margin-top:4px;line-height:1.1;}
    .stt-kpi-s{color:#8892b0;font-size:11px;margin-top:4px;}
    .stt-cyan{--accent:#00D4FF}.stt-green{--accent:#00FF88}.stt-orange{--accent:#FF9F45}.stt-purple{--accent:#7B61FF}
    .stt-c-cyan{color:#00D4FF!important}.stt-c-green{color:#00FF88!important}.stt-c-orange{color:#FF9F45!important}.stt-c-purple{color:#7B61FF!important}
    </style>
    """, unsafe_allow_html=True)

    st.markdown('<div class="stt-h1">⚡ Finance → TikTok Statements</div>', unsafe_allow_html=True)
    st.markdown('<div class="stt-sub">Ventas facturadas vs cobradas al banco · Waterfall con COGS real · Data del Merchant Statement TikTok</div>', unsafe_allow_html=True)

    _u = st.session_state.get("cached_user") or {}
    _bs = get_current_brand_slug() if _u.get("brands_enabled") else None
    _q = f"?brand_slug={_bs}" if _bs else ""

    # KPIs
    kpis = api_get(f"/tiktok-statements/kpis{_q}") or {}
    if not kpis or kpis.get("orders", 0) == 0:
        st.warning("Sin datos aún. Sube el TikTok Merchant Statement desde panel.rodmatcenter.com → Importación → Step 9.")
        return

    # 4 KPI cards
    fmt = lambda v: f"${v:,.0f}" if v else "$0"
    c1, c2, c3, c4 = st.columns(4)
    for col, cls, ck, ct in [
        (c1, "stt-cyan", "stt-c-cyan", "Total Facturado"),
        (c2, "stt-green", "stt-c-green", "Cobrado (settled)"),
        (c3, "stt-orange", "stt-c-orange", "Pending Payment"),
        (c4, "stt-purple", "stt-c-purple", "Margen NETO REAL"),
    ]:
        pass  # se rellenan abajo

    _revenue = kpis.get("revenue", 0)
    _settled = kpis.get("settled", 0)
    _pending = kpis.get("pending", 0)
    _net_real = kpis.get("net_margin_real", 0)
    _net_pct = kpis.get("net_margin_pct", 0)
    _sett_pct = kpis.get("settlement_pct", 0)
    _cogs = kpis.get("cogs_real", 0)
    _fees = kpis.get("fees_total", 0)
    _affiliate = kpis.get("affiliate", 0)
    _orders = kpis.get("orders", 0)
    _stmts = kpis.get("statements", 0)

    c1.markdown(f'<div class="stt-kpi stt-cyan"><div class="stt-kpi-l">Total Facturado</div><div class="stt-kpi-v">{fmt(_revenue)}</div><div class="stt-kpi-s">{_orders} órdenes · {_stmts} statements</div></div>', unsafe_allow_html=True)
    c2.markdown(f'<div class="stt-kpi stt-green"><div class="stt-kpi-l">Cobrado (banco)</div><div class="stt-kpi-v" style="color:#00FF88">{fmt(_settled)}</div><div class="stt-kpi-s">{_sett_pct:.1f}% del facturado</div></div>', unsafe_allow_html=True)
    c3.markdown(f'<div class="stt-kpi stt-orange"><div class="stt-kpi-l">Pending Payment</div><div class="stt-kpi-v" style="color:#FF9F45">{fmt(_pending)}</div><div class="stt-kpi-s">pendiente banco</div></div>', unsafe_allow_html=True)
    c4.markdown(f'<div class="stt-kpi stt-purple"><div class="stt-kpi-l">Margen NETO REAL</div><div class="stt-kpi-v" style="color:#00FF88">{fmt(_net_real)}</div><div class="stt-kpi-s">{_net_pct:.1f}% sobre revenue</div></div>', unsafe_allow_html=True)

    st.markdown("<br/>", unsafe_allow_html=True)

    # WATERFALL con Plotly — cuadra matemáticamente (Income - fees_totales - COGS = Net)
    import plotly.graph_objects as go
    fees_full = api_get(f"/tiktok-statements/fees-breakdown{_q}") or {}
    _referral = fees_full.get("referral", 0)
    _smart_promo_full = fees_full.get("smart_promo", 0)
    _managed = fees_full.get("managed", 0)
    _shipping_full = fees_full.get("shipping", 0)
    _affiliate_full = fees_full.get("affiliate", 0)

    # Suma de desglose que SÍ conocemos
    _known_fees = _referral + _smart_promo_full + _managed + _shipping_full + _affiliate_full
    # tt_cost (Order Cost total del statement) incluye TODOS los fees + shipping + affiliate
    _tt_cost_abs = abs(kpis.get("tt_cost", 0))
    _otros = max(0, _tt_cost_abs - _known_fees)  # taxes + refund fees + otros pequeños

    # Solo mostrar barras con valor > 0
    _bars = [("Income (neto)", _revenue, "#00FF88", "up")]
    if _referral > 0:         _bars.append(("Referral fee", _referral, "#FF6B35", "down"))
    if _smart_promo_full > 0: _bars.append(("Smart Promo", _smart_promo_full, "#FF6B35", "down"))
    if _managed > 0:          _bars.append(("Managed Service", _managed, "#FF6B35", "down"))
    if _shipping_full > 0:    _bars.append(("Shipping (FBT+TT)", _shipping_full, "#FF6B35", "down"))
    if _affiliate_full > 0:   _bars.append(("Affiliate", _affiliate_full, "#FF6B35", "down"))
    if _otros > 0.5:          _bars.append(("Otros fees TT", _otros, "#FF6B35", "down"))
    _bars.append(("COGS mercancía", _cogs, "#FF3D6B", "down"))
    _bars.append(("Margen NETO", _net_real, "#00FF88", "up"))

    fig = go.Figure(data=[go.Bar(
        x=[b[0] for b in _bars],
        y=[b[1] for b in _bars],
        marker=dict(color=[b[2] for b in _bars]),
        text=[f"${b[1]:,.0f}" if b[3]=="up" else f"-${b[1]:,.0f}" for b in _bars],
        textposition="outside",
        textfont=dict(color=[b[2] for b in _bars], size=13, family="Menlo"),
        hovertemplate="<b>%{x}</b><br>$%{y:,.2f}<extra></extra>",
    )])
    fig.update_layout(
        title=dict(text=f"🌊 Cascada Income → Margen NETO REAL   ·   {_net_pct:.1f}% margen s/revenue   ·   ✓ cuadra",
                   font=dict(color="#8892b0", size=14)),
        plot_bgcolor="rgba(15,20,47,0.5)", paper_bgcolor="rgba(15,20,47,0)",
        font=dict(color="#e4e9ff"), showlegend=False,
        xaxis=dict(showgrid=False, color="#8892b0", tickangle=-15),
        yaxis=dict(showgrid=True, gridcolor="rgba(123,97,255,0.1)", color="#8892b0", tickformat="$,.0f"),
        margin=dict(l=20, r=20, t=60, b=80), height=440,
    )
    st.plotly_chart(fig, use_container_width=True)

    # Weekly bars + Top productos
    col_w, col_top = st.columns([2, 1])

    weekly = api_get(f"/tiktok-statements/weekly{_q}") or []
    if weekly:
        df_w = _pd.DataFrame(weekly)
        fig_w = go.Figure()
        # Cobrado (dentro de la barra si hay espacio, arriba si no)
        fig_w.add_trace(go.Bar(name="Cobrado", x=df_w["week"], y=df_w["settled"],
            marker=dict(color="#00D4FF"),
            text=[f"${v:,.0f}" for v in df_w["settled"]],
            textposition="inside", insidetextanchor="middle",
            textfont=dict(color="#0a0e27", size=12, family="Menlo")))
        fig_w.add_trace(go.Bar(name="Pending", x=df_w["week"], y=df_w["pending"],
            marker=dict(color="#FF9F45"),
            text=[f"${v:,.0f}" if v > 100 else "" for v in df_w["pending"]],
            textposition="inside", insidetextanchor="middle",
            textfont=dict(color="#0a0e27", size=11, family="Menlo")))
        # Total encima de cada stack
        totals = [s + p for s, p in zip(df_w["settled"], df_w["pending"])]
        fig_w.add_trace(go.Scatter(x=df_w["week"], y=totals, mode="text",
            text=[f"${t:,.0f}" for t in totals],
            textposition="top center", textfont=dict(color="#e4e9ff", size=11, family="Menlo"),
            showlegend=False, hoverinfo="skip"))

        fig_w.update_layout(
            title=dict(text="📊 Semana × semana — Cobrado vs Pending", font=dict(color="#8892b0", size=14)),
            plot_bgcolor="rgba(15,20,47,0.5)", paper_bgcolor="rgba(15,20,47,0)",
            font=dict(color="#e4e9ff"), barmode="stack", height=360,
            xaxis=dict(showgrid=False, color="#8892b0"),
            yaxis=dict(showgrid=True, gridcolor="rgba(123,97,255,0.1)", color="#8892b0", tickformat="$,.0f"),
            legend=dict(orientation="h", yanchor="top", y=-0.1, x=0.35, bgcolor="rgba(0,0,0,0)"),
            margin=dict(l=20, r=20, t=50, b=20),
        )
        col_w.plotly_chart(fig_w, use_container_width=True)

    top = api_get(f"/tiktok-statements/top-products{_q}&n=5" if _q else "/tiktok-statements/top-products?n=5") or []
    if top:
        top_html = '<div class="stt-card"><h3>🏆 Top 5 productos por margen</h3>'
        for i, p in enumerate(top[:5], 1):
            nm = (p.get("product_name") or "")[:45]
            top_html += f'<div style="display:flex;align-items:center;gap:10px;padding:8px 0;border-bottom:1px solid rgba(123,97,255,.08)"><div style="font-size:12px;font-weight:700;color:#7B61FF;background:rgba(123,97,255,.1);width:22px;height:22px;border-radius:6px;display:flex;align-items:center;justify-content:center">{i}</div><div style="flex:1;font-size:12px;color:#e4e9ff">{nm}</div><div style="color:#00FF88;font-family:Menlo,monospace;font-weight:600;font-size:11px">+${p.get("margin",0):,.0f}</div></div>'
        top_html += '</div>'
        col_top.markdown(top_html, unsafe_allow_html=True)

    # Fees breakdown donut
    fees = api_get(f"/tiktok-statements/fees-breakdown{_q}") or {}
    if fees:
        fees_labels = ["Shipping (FBT+TT)", "Referral fee", "Affiliate", "Smart Promo", "Managed service"]
        fees_values = [fees.get("shipping", 0), fees.get("referral", 0), fees.get("affiliate", 0),
                       fees.get("smart_promo", 0), fees.get("managed", 0)]
        fees_colors = ["#00D4FF", "#7B61FF", "#FF9F45", "#00FF88", "#FF3D6B"]
        fig_d = go.Figure(data=[go.Pie(
            labels=fees_labels, values=fees_values, hole=0.55,
            marker=dict(colors=fees_colors, line=dict(color="#0a0e27", width=2)),
            text=[f"${v:,.0f}" for v in fees_values],
            texttemplate="%{text}<br>%{percent}",
            textinfo="text+percent",
            textposition="outside",
            textfont=dict(color="#e4e9ff", size=11),
            insidetextorientation="radial",
            hovertemplate="<b>%{label}</b><br>$%{value:,.2f} (%{percent})<extra></extra>",
            pull=[0.02] * len(fees_labels),
        )])
        fig_d.update_layout(
            title=dict(text=f"💸 Desglose fees TikTok — Total: ${sum(fees_values):,.0f}", font=dict(color="#8892b0", size=14)),
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(15,20,47,0)",
            font=dict(color="#e4e9ff"), showlegend=True,
            legend=dict(orientation="v", x=1.02, y=0.5, font=dict(color="#e4e9ff", size=11), bgcolor="rgba(0,0,0,0)"),
            margin=dict(l=20, r=20, t=50, b=20), height=340,
        )
        st.plotly_chart(fig_d, use_container_width=True)

    # Statements list
    stmts = api_get(f"/tiktok-statements/statements{_q}") or []
    if stmts:
        st.markdown(f'<div class="stt-card"><h3>💰 {len(stmts)} statements / payouts al banco</h3></div>', unsafe_allow_html=True)
        df_s = _pd.DataFrame(stmts)
        df_s = df_s[["settled_date", "period_start", "period_end", "total_orders", "total_income", "total_margin", "total_fees", "statement_id"]]
        df_s.columns = ["Settled Date", "Desde", "Hasta", "Órdenes", "Income", "Margin TT", "Fees", "Statement ID"]
        st.dataframe(df_s, use_container_width=True, height=380, hide_index=True)


# ============================================================================
#  CHAT IA — Demo gated to rodmatwh@gmail.com only
# ============================================================================
CHAT_AI_ALLOWED = {"rodmatwh@gmail.com"}

def page_chat_ai():
    """Chat IA hibrido: 10 tools + SQL fallback + guardrail off-topic."""
    st.markdown("""
    <div style="background:linear-gradient(135deg,#0a0e27 0%,#1a1f4a 100%);
                padding:20px;border-radius:12px;border:1px solid #00d4ff33;margin-bottom:16px;">
      <div style="color:#00d4ff;font-size:24px;font-weight:700;">🤖 Chat IA · Datos en tiempo real</div>
      <div style="color:#8899aa;font-size:13px;margin-top:6px;">
        Pregunta lo que quieras sobre ventas, inventario, creators, órdenes o P&L.<br>
        Ejemplos: "cuánto vendimos esta semana", "top 5 productos del mes", "stock de Far Away",
        "creators top de agosto", "P&L de julio".
      </div>
    </div>
    """, unsafe_allow_html=True)

    # Init history
    if "_chat_ai_msgs" not in st.session_state:
        st.session_state["_chat_ai_msgs"] = []

    # Render history
    for msg in st.session_state["_chat_ai_msgs"]:
        with st.chat_message(msg["role"], avatar="🤖" if msg["role"]=="assistant" else "👤"):
            st.markdown(msg["content"])
            if msg.get("meta"):
                with st.expander("🔍 Debug info", expanded=False):
                    m = msg["meta"]
                    st.caption(f"**Path:** {m.get('path')} · **Tool:** {m.get('tool_used') or '-'}")
                    if m.get("router_reason"): st.caption(f"**Router:** {m['router_reason']}")
                    if m.get("sql_executed"):
                        st.code(m["sql_executed"], language="sql")
                    if m.get("raw_data"):
                        st.json(m["raw_data"])

    # Input
    q = st.chat_input("¿Qué quieres saber?")
    if q:
        st.session_state["_chat_ai_msgs"].append({"role":"user","content":q})
        with st.chat_message("user", avatar="👤"):
            st.markdown(q)
        with st.chat_message("assistant", avatar="🤖"):
            with st.spinner("Consultando datos..."):
                resp = api_post("/api/chat/query", json_data={"question": q})
            if not resp:
                st.error("Error de red. Reintenta.")
                return
            ans = resp.get("answer", "(sin respuesta)")
            st.markdown(ans)
            with st.expander("🔍 Debug info", expanded=False):
                st.caption(f"**Path:** {resp.get('path')} · **Tool:** {resp.get('tool_used') or '-'}")
                if resp.get("router_reason"): st.caption(f"**Router:** {resp['router_reason']}")
                if resp.get("sql_executed"):
                    st.code(resp["sql_executed"], language="sql")
                if resp.get("raw_data"):
                    st.json(resp["raw_data"])
            st.session_state["_chat_ai_msgs"].append({
                "role":"assistant","content":ans,
                "meta":{"path":resp.get("path"),"tool_used":resp.get("tool_used"),
                        "router_reason":resp.get("router_reason"),
                        "sql_executed":resp.get("sql_executed"),
                        "raw_data":resp.get("raw_data")}
            })

    # Clear button
    if st.session_state["_chat_ai_msgs"]:
        if st.button("🧹 Limpiar conversación", key="_chat_clear"):
            st.session_state["_chat_ai_msgs"] = []
            st.rerun()




if __name__ == "__main__":
    main()
