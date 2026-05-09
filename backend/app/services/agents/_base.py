"""
Shared utilities for all V2 agents: Groq API call, SMTP email, config, data loaders.
"""
from __future__ import annotations
import json
import os
import smtplib
import urllib.request
import urllib.error
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from sqlalchemy.orm import Session

GROQ_MODEL = "llama-3.3-70b-versatile"
GROQ_URL   = "https://api.groq.com/openai/v1/chat/completions"

SMTP_HOST     = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT     = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER     = os.getenv("SMTP_USER", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
GROQ_API_KEY  = os.getenv("GROQ_API_KEY", "")


def get_recipients(store) -> list[str]:
    """Return email recipients for a store. Falls back to SMTP_USER."""
    if store and store.settings:
        recs = store.settings.get("report_recipients", [])
        if recs:
            return recs
    return [SMTP_USER] if SMTP_USER else []


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
    if not SMTP_USER or not SMTP_PASSWORD or not recipients:
        return False
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = SMTP_USER
    msg["To"]      = ", ".join(recipients)
    msg.attach(MIMEText(html, "html", "utf-8"))
    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as s:
            s.ehlo(); s.starttls(); s.ehlo()
            s.login(SMTP_USER, SMTP_PASSWORD)
            s.sendmail(SMTP_USER, recipients, msg.as_string())
        return True
    except Exception as e:
        print(f"[email] send failed: {e}")
        return False


def load_orders_df(db: Session, store_id: str) -> pd.DataFrame:
    from app.services.stock_calculator import _load_orders_df
    return _load_orders_df(db, store_id)


def load_kpis(db: Session, store_id: str) -> pd.DataFrame:
    from app.services.stock_calculator import calculate_stock
    return calculate_stock(db, store_id)


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
