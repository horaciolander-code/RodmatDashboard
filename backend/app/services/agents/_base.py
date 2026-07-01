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
