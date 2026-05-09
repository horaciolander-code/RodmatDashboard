"""
Daily Report Service - Ported from V1 daily_report.py
Builds HTML email report per store and sends via Resend API.
"""
import os
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from sqlalchemy.orm import Session

from app.models.store import Store
from app.models.sales import SalesOrder
from app.services.stock_calculator import calculate_stock, _load_orders_df


RESEND_API_KEY = os.getenv("RESEND_API_KEY", "")
SMTP_USER      = os.getenv("SMTP_USER", "")


def build_report(db: Session, store_id: str) -> str:
    """Build full HTML report for a store. Returns HTML string."""
    today = pd.Timestamp.now().normalize()
    yesterday = today - timedelta(days=1)
    day_before = today - timedelta(days=2)

    df = _load_orders_df(db, store_id)
    if df.empty:
        return "<p>No order data available.</p>"

    store = db.query(Store).filter(Store.id == store_id).first()
    store_name = store.name if store else "Store"
    low_threshold = 30
    stale_days = 3
    if store and store.settings:
        low_threshold = store.settings.get("low_stock_threshold", 30)
        stale_days = store.settings.get("stale_order_days", 3)

    sections = []

    # -- Section 1: Revenue overview --
    yest_orders = df[df["Order_Date"].dt.date == yesterday.date()]
    db_orders = df[df["Order_Date"].dt.date == day_before.date()]

    rev_y = yest_orders["SKU Subtotal After Discount"].sum()
    rev_db = db_orders["SKU Subtotal After Discount"].sum()
    orders_y = yest_orders["Order ID"].nunique()
    units_y = yest_orders["Quantity"].sum()
    pct = ((rev_y - rev_db) / rev_db * 100) if rev_db > 0 else 0
    arrow = "&#9650;" if pct >= 0 else "&#9660;"
    color = "#27ae60" if pct >= 0 else "#e74c3c"

    mtd = df[df["Order_Date"] >= today.replace(day=1)]
    rev_mtd = mtd["SKU Subtotal After Discount"].sum()
    orders_mtd = mtd["Order ID"].nunique()

    sections.append(f"""
    <div style="background:#f8f9fa;border-radius:8px;padding:20px;margin-bottom:20px;">
      <h2 style="color:#2c3e50;margin-top:0;">Resumen - {yesterday.strftime('%d/%m/%Y')}</h2>
      <table style="width:100%;border-collapse:collapse;">
        <tr>
          <td style="padding:12px;text-align:center;background:#fff;border:1px solid #eee;border-radius:8px;">
            <div style="font-size:13px;color:#7f8c8d;">Facturado Ayer</div>
            <div style="font-size:28px;font-weight:bold;color:#2c3e50;">${rev_y:,.2f}</div>
          </td>
          <td style="padding:12px;text-align:center;background:#fff;border:1px solid #eee;border-radius:8px;">
            <div style="font-size:13px;color:#7f8c8d;">Ordenes</div>
            <div style="font-size:28px;font-weight:bold;">{orders_y}</div>
          </td>
          <td style="padding:12px;text-align:center;background:#fff;border:1px solid #eee;border-radius:8px;">
            <div style="font-size:13px;color:#7f8c8d;">Unidades</div>
            <div style="font-size:28px;font-weight:bold;">{units_y:.0f}</div>
          </td>
          <td style="padding:12px;text-align:center;background:#fff;border:1px solid #eee;border-radius:8px;">
            <div style="font-size:13px;color:#7f8c8d;">vs Dia Anterior</div>
            <div style="font-size:28px;font-weight:bold;color:{color};">{arrow} {pct:+.1f}%</div>
          </td>
        </tr>
      </table>
      <div style="margin-top:12px;padding:10px;background:#eaf2f8;border-radius:6px;font-size:13px;">
        <strong>Mes en curso:</strong> ${rev_mtd:,.2f} | {orders_mtd} ordenes
      </div>
    </div>
    """)

    # -- Section 2: Stock alerts --
    from app.services.analytics_service import _get_stock_df
    stock = _get_stock_df(db, store_id)
    if not stock.empty:
        low = stock[
            (stock["Initial_Stock"] > 0) &
            ((stock["StockActualizado"] < 0) |
             ((stock["StockActualizado"] > 0) & (stock["StockActualizado"] < low_threshold)))
        ].sort_values("StockActualizado")

        if not low.empty:
            rows = ""
            for _, r in low.iterrows():
                sv = r["StockActualizado"]
                bg = "#f5b7b1" if sv < 0 else ("#fdedec" if sv <= 5 else ("#fef9e7" if sv <= 15 else "#fff"))
                cov = r.get("Days_Coverage", 999)
                cov_str = f"{cov:.0f}d" if cov < 999 else "N/A"
                rows += f'<tr style="background:{bg};"><td style="padding:8px;border:1px solid #eee;">{r["ProductoNombre"]}</td><td style="padding:8px;border:1px solid #eee;text-align:center;">{sv:.0f}</td><td style="padding:8px;border:1px solid #eee;text-align:center;">{r.get("AvgVentas30d", 0):.1f}</td><td style="padding:8px;border:1px solid #eee;text-align:center;">{cov_str}</td></tr>'

            sections.append(f"""
            <div style="background:#fff;border:2px solid #e74c3c;border-radius:8px;padding:20px;margin-bottom:20px;">
              <h2 style="color:#e74c3c;margin-top:0;">Alerta de Stock - {len(low)} productos</h2>
              <table style="width:100%;border-collapse:collapse;font-size:13px;">
                <thead><tr style="background:#e74c3c;color:#fff;">
                  <th style="padding:10px;text-align:left;">Producto</th>
                  <th style="padding:10px;text-align:center;">Stock</th>
                  <th style="padding:10px;text-align:center;">Vta/Dia</th>
                  <th style="padding:10px;text-align:center;">Cobertura</th>
                </tr></thead>
                <tbody>{rows}</tbody>
              </table>
            </div>
            """)
        else:
            sections.append(f'<div style="background:#eafaf1;border-radius:8px;padding:15px;margin-bottom:20px;"><h2 style="color:#27ae60;margin-top:0;">Stock OK</h2></div>')

    # -- Section 3: Awaiting shipment --
    awaiting_mask = pd.Series(False, index=df.index)
    if "Order Status" in df.columns:
        awaiting_mask |= df["Order Status"].astype(str).str.contains("Awaiting|To ship", case=False, na=False)
    if "Order Substatus" in df.columns:
        awaiting_mask |= df["Order Substatus" if "Order Substatus" in df.columns else "Order Status"].astype(str).str.contains("Awaiting shipment|Awaiting collection", case=False, na=False)

    awaiting = df[awaiting_mask]
    total_awaiting = awaiting["Order ID"].nunique() if not awaiting.empty else 0

    if total_awaiting > 0:
        sections.append(f"""
        <div style="background:#fff3cd;border-radius:8px;padding:15px;margin-bottom:20px;">
          <h2 style="color:#856404;margin-top:0;">Pendiente de Envio: {total_awaiting} ordenes</h2>
        </div>
        """)

    # -- Wrap in full HTML --
    body = "\n".join(sections)
    html = f"""
    <html><body style="font-family:Arial,sans-serif;max-width:800px;margin:0 auto;padding:20px;background:#f5f5f5;">
      <div style="background:#fff;border-radius:12px;padding:30px;box-shadow:0 2px 10px rgba(0,0,0,0.08);">
        <h1 style="color:#2c3e50;border-bottom:3px solid #3498db;padding-bottom:10px;">
          {store_name} - Reporte Diario
        </h1>
        {body}
        <div style="margin-top:20px;padding:15px;background:#f8f9fa;border-radius:6px;font-size:11px;color:#999;">
          Generado automaticamente por Rodmat Dashboard V2 el {datetime.now().strftime('%Y-%m-%d %H:%M')}
        </div>
      </div>
    </body></html>
    """
    return html


def send_report(html: str, recipients: list[str], store_name: str) -> bool:
    """Send HTML report via Resend API."""
    if not RESEND_API_KEY or not recipients:
        print("RESEND_API_KEY not set or no recipients, skipping send")
        return False
    try:
        import httpx
        r = httpx.post(
            "https://api.resend.com/emails",
            json={
                "from":     "Rodmat Dashboard <onboarding@resend.dev>",
                "reply_to": SMTP_USER or recipients[0],
                "to":       recipients,
                "subject":  f"{store_name} - Reporte Diario {datetime.now().strftime('%d/%m/%Y')}",
                "html":     html,
            },
            headers={"Authorization": f"Bearer {RESEND_API_KEY}"},
            timeout=30,
        )
        if r.status_code == 200:
            print(f"Report sent to {recipients}")
            return True
        print(f"Resend error {r.status_code}: {r.text}")
        return False
    except Exception as e:
        print(f"Failed to send report: {e}")
        return False


def run_store_report(db: Session, store_id: str) -> bool:
    """Build and send report for a single store."""
    store = db.query(Store).filter(Store.id == store_id).first()
    if not store:
        return False

    settings = store.settings or {}
    if not settings.get("report_enabled", False):
        return False

    recipients = settings.get("report_recipients", [])
    if not recipients:
        return False

    html = build_report(db, store_id)
    return send_report(html, recipients, store.name)


def run_all_reports(db: Session) -> dict:
    """Run reports for all stores that have reporting enabled."""
    stores = db.query(Store).all()
    results = {}
    for store in stores:
        settings = store.settings or {}
        if settings.get("report_enabled", False) and settings.get("report_recipients"):
            ok = run_store_report(db, store.id)
            results[store.name] = "sent" if ok else "failed"
        else:
            results[store.name] = "skipped (reporting not configured)"
    return results
