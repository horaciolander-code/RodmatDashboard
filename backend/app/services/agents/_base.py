"""
Shared utilities for all V2 agents: Groq API call, Resend email, config, data loaders.
"""
from __future__ import annotations
import json
import os

from sqlalchemy.orm import Session

# Groq model — Llama 3.3 70B Versatile deprecated 2026-06-30, decommission 2026-08-16.
# Migration: default to openai/gpt-oss-20b (similar size/speed, ~good quality on structured JSON).
# Env var GROQ_MODEL allows swapping without redeploy if a model regresses.
# Groq model catalog: https://console.groq.com/docs/models
GROQ_MODEL    = os.getenv("GROQ_MODEL", "openai/gpt-oss-20b")
GROQ_API_KEY  = os.getenv("GROQ_API_KEY", "")
RESEND_API_KEY = os.getenv("RESEND_API_KEY", "")
SMTP_USER     = os.getenv("SMTP_USER", "")  # kept for reply-to / fallback recipient


def get_recipients(store) -> list[str]:
    """Return email recipients for a store. Falls back to SMTP_USER."""
    if store and store.settings:
        recs = store.settings.get("report_recipients", [])
        if recs:
            return recs
    return [SMTP_USER] if SMTP_USER else []


def get_business_context(store) -> str:
    """Return the tenant-scoped business context line injected into agent
    prompts. Pulled from store.settings['business_context']. Empty string
    when the tenant has not configured a vertical/brand description yet —
    agents must build a coherent prompt with no vertical assumptions in
    that case."""
    if store and store.settings:
        return store.settings.get("business_context") or ""
    return ""


def resolve_brand_context(db, store_id: str, brand_slug: str | None) -> tuple[dict | None, str]:
    """Devuelve (brand_info, brand_context_line) para agentes brand-scoped.

    Cuando brand_slug es None → (None, "") → agente ve todo el store.
    Cuando brand_slug es X → busca la brand en BD, devuelve info + línea inyectable
    en el prompt LLM del tipo: "ESTÁS ANALIZANDO SOLO la marca X (Lattafa+Atralia)".

    brand_info dict:
      { id, slug, display_name, sku_prefixes_note, brand_color, email_sender }
    """
    if not brand_slug:
        return None, ""

    from sqlalchemy import text as _text
    row = db.execute(_text(
        "SELECT id, slug, display_name, sku_prefixes_note, brand_color, email_sender "
        "FROM brands WHERE store_id=:sid AND slug=:slug LIMIT 1"),
        {"sid": store_id, "slug": brand_slug}
    ).fetchone()
    if not row:
        return None, ""

    binfo = {
        "id": row[0], "slug": row[1], "display_name": row[2],
        "sku_prefixes_note": row[3] or "", "brand_color": row[4] or "",
        "email_sender": row[5] or "",
    }
    prefix_hint = f" (SKU prefixes: {binfo['sku_prefixes_note']})" if binfo['sku_prefixes_note'] else ""
    ctx_line = (
        f"⚠️ ANÁLISIS RESTRINGIDO A UNA MARCA: estás analizando SOLO la marca "
        f"'{binfo['display_name']}'{prefix_hint} dentro del catálogo del store. "
        f"NO menciones datos de otras marcas — el snapshot ya viene filtrado. "
        f"Contextualiza tus recomendaciones específicamente para esta marca.\n"
    )
    return binfo, ctx_line


def get_brand_recipients(db, store_id: str, brand_slug: str | None, fallback_recipients: list[str]) -> list[str]:
    """Devuelve destinatarios específicos por brand.

    Prioridad:
      1. brands.email_sender si está definido para la brand
      2. Fallback: recipients del store (los ya-configurados)
    """
    if not brand_slug:
        return fallback_recipients
    from sqlalchemy import text as _text
    row = db.execute(_text(
        "SELECT email_sender FROM brands WHERE store_id=:sid AND slug=:slug"),
        {"sid": store_id, "slug": brand_slug}
    ).fetchone()
    if row and row[0]:
        return [r.strip() for r in row[0].split(",") if r.strip()]
    return fallback_recipients


def is_agent_enabled(store, agent_name: str) -> bool:
    """Return True if the agent is enabled for this tenant.

    Defaults to True for backwards-compat: tenants without
    settings['agents_enabled'] configured keep their current behaviour.
    Tenants can disable specific agents per-vertical — e.g. MESMERIZE
    is fragrance-only and stays False for Nokal."""
    if not (store and store.settings):
        return True
    flags = store.settings.get("agents_enabled")
    if not isinstance(flags, dict):
        return True
    return bool(flags.get(agent_name, True))


def call_groq(prompt: str, max_tokens: int = 2048) -> str:
    if not GROQ_API_KEY:
        raise RuntimeError("GROQ_API_KEY not set")
    from groq import Groq
    client = Groq(api_key=GROQ_API_KEY)
    completion = client.chat.completions.create(
        model=GROQ_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2,
        max_tokens=max_tokens,
    )
    return completion.choices[0].message.content


def send_email(html: str, subject: str, recipients: list[str]) -> bool:
    if not RESEND_API_KEY or not recipients:
        print("[email] RESEND_API_KEY not set or no recipients")
        return False
    to = [r.lower() for r in recipients]
    try:
        import httpx
        r = httpx.post(
            "https://api.resend.com/emails",
            json={
                "from":     "reportes@rodmatcenter.com",
                "reply_to": SMTP_USER or to[0],
                "to":       to,
                "subject":  subject,
                "html":     html,
            },
            headers={"Authorization": f"Bearer {RESEND_API_KEY}"},
            timeout=30,
        )
        if r.status_code == 200:
            return True
        print(f"[email] Resend error {r.status_code}: {r.text}")
        return False
    except Exception as e:
        print(f"[email] send failed: {e}")
        return False


def load_orders_df(db: Session, store_id: str) -> pd.DataFrame:
    from app.services.stock_calculator import _load_orders_df
    return _load_orders_df(db, store_id)


def load_kpis(db: Session, store_id: str) -> pd.DataFrame:
    from app.services.analytics_service import _get_stock_df
    return _get_stock_df(db, store_id)


def load_creator_df(db: Session, store_id: str):
    import pandas as pd
    from app.models.sales import AffiliateSale
    rows = db.query(AffiliateSale).filter(AffiliateSale.store_id == store_id).all()
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame([{
        "Order ID":         a.order_id,
        "Creator Username": a.creator_username,
        "Payment Amount":   a.payment_amount or 0,
        "Product Name":     a.product_name,
        "Order Status":     a.order_status or "COMPLETED",
        "Time Created":     pd.to_datetime(a.time_created) if a.time_created else pd.NaT,
        "Content Type":     a.content_type,
        "Commission":       a.commission or 0,
    } for a in rows])


def load_pending_df(db: Session, store_id: str):
    import pandas as pd
    from app.models.inventory import IncomingStock
    from app.models.product import Product
    rows = (db.query(IncomingStock, Product)
            .join(Product, IncomingStock.product_id == Product.id)
            .filter(IncomingStock.store_id == store_id,
                    IncomingStock.status == "pending")
            .all())
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame([{
        "Producto":               prod.name,
        "Unidades pedidas":       inc.qty_ordered,
        "Importe total":          (inc.cost or 0) * inc.qty_ordered,
        "Fecha estimada entrega": inc.expected_arrival,
        "Fecha pedido":           inc.order_date,
        "Status":                 inc.status,
    } for inc, prod in rows])


# ═════════════════════════════════════════════════════════════════════════════
#  BRAND-aware helpers (multi-brand support LuxPerfumes/Avon)
# ═════════════════════════════════════════════════════════════════════════════

def get_brand(db: Session, store_id: str, brand_slug: str | None):
    """Return the Brand row for a store+slug, or None."""
    if not brand_slug:
        return None
    from app.models import Brand
    return db.query(Brand).filter(
        Brand.store_id == store_id,
        Brand.slug == brand_slug,
        Brand.is_active == True,
    ).first()


def get_brand_context(store, brand=None) -> str:
    """Return the business context — brand-specific if brand provided,
    else store-level. Falls back gracefully to store business_context if
    brand has no specific setting."""
    if brand is not None:
        # brand context stored in store.settings['brands_context'][slug]
        if store and store.settings:
            bctx = store.settings.get("brands_context") or {}
            if isinstance(bctx, dict) and brand.slug in bctx:
                return bctx[brand.slug]
        # fallback: just the display_name for the LLM to know the brand
        return f"Brand: {brand.display_name}."
    return get_business_context(store)


def get_brand_recipients(store, brand=None) -> list[str]:
    """Return recipients — brand-scoped list if brand provided and
    settings['brands_recipients'][slug] set, else store-level.

    Regla operativa: los emails Atralia/LuxPerfumes NO se mezclan con Rodmat.
    Cuando brand se pasa, si no hay lista brand-específica, devuelve lista vacía
    (mejor no enviar que enviar a la lista wrong)."""
    if brand is not None:
        if store and store.settings:
            br = (store.settings.get("brands_recipients") or {})
            if isinstance(br, dict) and brand.slug in br:
                return br[brand.slug]
        return []  # safety: no leak
    return get_recipients(store)


def get_brand_sender(brand=None) -> str:
    """Return the from-address for emails — brand-specific if brand has
    email_sender set, else default reportes@rodmatcenter.com."""
    if brand and getattr(brand, "email_sender", None):
        return brand.email_sender
    return "reportes@rodmatcenter.com"


def load_orders_df_branded(db: Session, store_id: str, brand_slug: str | None = None):
    """Wrapper that filters orders by brand when brand_slug provided.
    Uses the SKU→brand map from products.brand_id (client-side filter after load
    to avoid touching stock_calculator._load_orders_df signature).
    When brand_slug is None → passthrough (current behavior)."""
    df = load_orders_df(db, store_id)
    if not brand_slug or df is None or df.empty:
        return df
    # Build sku→slug map for this store
    from app.models import Product, Brand
    rows = db.query(Product.sku, Brand.slug).select_from(Product).outerjoin(
        Brand, Brand.id == Product.brand_id
    ).filter(Product.store_id == store_id).all()
    bmap = {r[0]: r[1] for r in rows}
    # Filter df by SKU column (case: 'Seller SKU' o 'SKU ID' o 'sku')
    sku_col = None
    for candidate in ("Seller SKU", "SKU ID", "sku", "SKU"):
        if candidate in df.columns:
            sku_col = candidate
            break
    if not sku_col:
        return df  # no way to filter, passthrough
    mask = df[sku_col].astype(str).map(lambda s: bmap.get(s) == brand_slug)
    return df[mask].reset_index(drop=True)


def send_email_branded(html: str, subject: str, recipients: list[str], brand=None) -> bool:
    """Send email using brand-specific sender when brand provided."""
    if not RESEND_API_KEY or not recipients:
        print("[email] RESEND_API_KEY not set or no recipients")
        return False
    from_addr = get_brand_sender(brand)
    to = [r.lower() for r in recipients]
    try:
        import httpx
        r = httpx.post(
            "https://api.resend.com/emails",
            json={
                "from":     from_addr,
                "reply_to": SMTP_USER or to[0],
                "to":       to,
                "subject":  subject,
                "html":     html,
            },
            headers={"Authorization": f"Bearer {RESEND_API_KEY}"},
            timeout=30,
        )
        if r.status_code == 200:
            return True
        print(f"[email] Resend error {r.status_code}: {r.text}")
        return False
    except Exception as e:
        print(f"[email] send failed: {e}")
        return False
