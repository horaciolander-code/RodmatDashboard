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
from app.services.stock_calculator import _load_orders_df

RESEND_API_KEY = os.getenv("RESEND_API_KEY", "")
SMTP_USER      = os.getenv("SMTP_USER", "")

LOW_STOCK_THRESHOLD = 30
STALE_ORDER_DAYS    = 3


def _viral_alerts(db: Session, store_id: str, threshold: int = 20, days: int = 5) -> pd.DataFrame:
    """Creators with threshold+ units sold in last N days (affiliate sales)."""
    from app.models.sales import AffiliateSale
    cutoff = pd.Timestamp.now() - pd.Timedelta(days=days)
    rows = db.query(AffiliateSale).filter(
        AffiliateSale.store_id == store_id,
        AffiliateSale.time_created >= cutoff.to_pydatetime(),
        AffiliateSale.order_status.in_(["Completed", "Delivered", "Shipped"]),
    ).all()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame([{
        "Creator Username": r.creator_username,
        "Product Name":     r.product_name,
        "Unidades":         r.quantity or 0,
        "GMV":              r.payment_amount or 0,
    } for r in rows])
    agg = df.groupby(["Creator Username", "Product Name"]).agg(
        Unidades=("Unidades", "sum"),
        Ordenes=("Unidades", "count"),
        GMV=("GMV", "sum"),
    ).reset_index()
    return agg[agg["Unidades"] >= threshold].sort_values("Unidades", ascending=False)


def build_report(db: Session, store_id: str) -> tuple[str, str]:
    """Build full HTML report. Returns (html, subject)."""
    today     = pd.Timestamp.now().normalize()
    yesterday = today - timedelta(days=1)
    day_before = today - timedelta(days=2)

    df = _load_orders_df(db, store_id)
    if df.empty:
        return "<p>No order data available.</p>", "Reporte Diario - sin datos"

    store = db.query(Store).filter(Store.id == store_id).first()
    store_name   = store.name if store else "Store"
    low_threshold = store.settings.get("low_stock_threshold", LOW_STOCK_THRESHOLD) if store and store.settings else LOW_STOCK_THRESHOLD
    stale_days    = store.settings.get("stale_order_days",    STALE_ORDER_DAYS)    if store and store.settings else STALE_ORDER_DAYS

    sections = []

    # ── 1. OVERVIEW ──────────────────────────────────────────────────────────
    yest_df  = df[df["Order_Date"].dt.date == yesterday.date()]
    prev_df  = df[df["Order_Date"].dt.date == day_before.date()]
    rev_y    = yest_df["SKU Subtotal After Discount"].sum()
    rev_prev = prev_df["SKU Subtotal After Discount"].sum()
    orders_y = yest_df["Order ID"].nunique()
    units_y  = yest_df["Quantity"].sum()
    pct      = ((rev_y - rev_prev) / rev_prev * 100) if rev_prev > 0 else 0
    arrow    = "&#9650;" if pct >= 0 else "&#9660;"
    color    = "#27ae60" if pct >= 0 else "#e74c3c"

    mtd_df    = df[df["Order_Date"] >= today.replace(day=1)]
    rev_mtd   = mtd_df["SKU Subtotal After Discount"].sum()
    orders_mtd = mtd_df["Order ID"].nunique()

    sections.append(f"""
    <div style="background:#f8f9fa;border-radius:8px;padding:20px;margin-bottom:20px;">
      <h2 style="color:#2c3e50;margin-top:0;">&#128202; Resumen de Ventas - {yesterday.strftime('%d/%m/%Y')}</h2>
      <table style="width:100%;border-collapse:collapse;">
        <tr>
          <td style="padding:6px;text-align:center;background:#fff;border-radius:8px;border:1px solid #eee;">
            <div style="font-size:13px;color:#7f8c8d;">Facturado Ayer</div>
            <div style="font-size:28px;font-weight:bold;color:#2c3e50;">${rev_y:,.2f}</div>
          </td>
          <td style="padding:6px;text-align:center;background:#fff;border-radius:8px;border:1px solid #eee;">
            <div style="font-size:13px;color:#7f8c8d;">Ordenes Ayer</div>
            <div style="font-size:28px;font-weight:bold;color:#2c3e50;">{orders_y}</div>
          </td>
          <td style="padding:6px;text-align:center;background:#fff;border-radius:8px;border:1px solid #eee;">
            <div style="font-size:13px;color:#7f8c8d;">Unidades Ayer</div>
            <div style="font-size:28px;font-weight:bold;color:#2c3e50;">{units_y:.0f}</div>
          </td>
          <td style="padding:6px;text-align:center;background:#fff;border-radius:8px;border:1px solid #eee;">
            <div style="font-size:13px;color:#7f8c8d;">vs Dia Anterior</div>
            <div style="font-size:28px;font-weight:bold;color:{color};">{arrow} {pct:+.1f}%</div>
          </td>
        </tr>
      </table>
      <div style="margin-top:12px;padding:10px;background:#eaf2f8;border-radius:6px;font-size:13px;color:#2c3e50;">
        <strong>Mes en curso ({today.strftime('%B %Y')}):</strong>
        ${rev_mtd:,.2f} facturado | {orders_mtd} ordenes
      </div>
    </div>
    """)

    # ── 2. VIRAL ALERTS ──────────────────────────────────────────────────────
    try:
        viral = _viral_alerts(db, store_id, threshold=20, days=5)
    except Exception:
        viral = pd.DataFrame()

    if not viral.empty:
        viral_rows = ""
        for _, r in viral.iterrows():
            viral_rows += f"""
            <tr>
              <td style="padding:8px;border:1px solid #eee;">{r['Creator Username']}</td>
              <td style="padding:8px;border:1px solid #eee;">{r['Product Name']}</td>
              <td style="padding:8px;border:1px solid #eee;text-align:center;font-weight:bold;color:#8e44ad;">{int(r['Unidades'])}</td>
              <td style="padding:8px;border:1px solid #eee;text-align:center;">{int(r['Ordenes'])}</td>
              <td style="padding:8px;border:1px solid #eee;text-align:center;">${r['GMV']:,.2f}</td>
            </tr>"""
        sections.append(f"""
        <div style="background:#f5eef8;border:2px solid #8e44ad;border-radius:8px;padding:20px;margin-bottom:20px;">
          <h2 style="color:#8e44ad;margin-top:0;">&#128640; {len(viral)} Alerta(s) Video Viral (ultimos 5 dias, &ge;20 uds)</h2>
          <table style="width:100%;border-collapse:collapse;font-size:13px;">
            <thead><tr style="background:#8e44ad;color:#fff;">
              <th style="padding:10px;text-align:left;">Creadora</th>
              <th style="padding:10px;text-align:left;">Producto</th>
              <th style="padding:10px;text-align:center;">Unidades</th>
              <th style="padding:10px;text-align:center;">Ordenes</th>
              <th style="padding:10px;text-align:center;">GMV</th>
            </tr></thead>
            <tbody>{viral_rows}</tbody>
          </table>
        </div>
        """)

    # ── 3. STOCK ALERTS ──────────────────────────────────────────────────────
    n_stock_alerts = 0
    n_negatives    = 0
    try:
        from app.services.analytics_service import _get_stock_df
        stock = _get_stock_df(db, store_id)
    except Exception:
        stock = pd.DataFrame()

    if not stock.empty:
        has_wh  = "Stock_Warehouse" in stock.columns
        has_fbt = "Stock_FBT"       in stock.columns

        alert_mask = (stock["Initial_Stock"] > 0) & (
            (stock["StockActualizado"] < 0) |
            ((stock["StockActualizado"] > 0) & (stock["StockActualizado"] < low_threshold))
        )
        if has_wh:
            alert_mask |= (stock["Initial_Stock"] > 0) & (
                (stock["Stock_Warehouse"] < 0) |
                ((stock["Stock_Warehouse"] > 0) & (stock["Stock_Warehouse"] < low_threshold))
            )
        if has_fbt:
            alert_mask |= (stock["Initial_Stock"] > 0) & (stock.get("FBT_Sent", pd.Series(0, index=stock.index)) > 0) & (
                (stock["Stock_FBT"] < 0) |
                ((stock["Stock_FBT"] > 0) & (stock["Stock_FBT"] < low_threshold))
            )

        low = stock[alert_mask].sort_values("Stock_Warehouse" if has_wh else "StockActualizado")
        n_stock_alerts = len(low)

        neg_mask = (stock["Initial_Stock"] > 0) & (stock["StockActualizado"] < 0)
        if has_wh:
            neg_mask |= (stock["Initial_Stock"] > 0) & (stock["Stock_Warehouse"] < 0)
        n_negatives = len(stock[neg_mask])

        if not low.empty:
            rows_html = ""
            for _, r in low.iterrows():
                sv  = r["StockActualizado"]
                swh = r.get("Stock_Warehouse", sv)
                sfbt = r.get("Stock_FBT", 0)
                bg  = "#f5b7b1" if sv < 0 else ("#fdedec" if sv <= 5 else ("#fef9e7" if sv <= 15 else "#fff"))
                cov = r.get("Days_Coverage", 999)
                cov_str = f"{cov:.0f}d" if cov < 999 else "N/A"
                sv_html = f"<span style='color:#e74c3c;font-weight:bold;'>{sv:.0f}</span>" if sv < 0 else f"{sv:.0f}"
                tipo    = r.get("Tipo", r.get("category", "-")) or "-"
                rows_html += f"""
                <tr style="background:{bg};">
                  <td style="padding:8px;border:1px solid #eee;">{r['ProductoNombre']}</td>
                  <td style="padding:8px;border:1px solid #eee;text-align:center;">{tipo}</td>
                  <td style="padding:8px;border:1px solid #eee;text-align:center;">{swh:.0f}</td>
                  <td style="padding:8px;border:1px solid #eee;text-align:center;">{"" if not has_fbt else f"{sfbt:.0f}"}</td>
                  <td style="padding:8px;border:1px solid #eee;text-align:center;">{sv_html}</td>
                  <td style="padding:8px;border:1px solid #eee;text-align:center;">{r.get('AvgVentas30d', 0):.1f}</td>
                  <td style="padding:8px;border:1px solid #eee;text-align:center;">{cov_str}</td>
                </tr>"""

            alert_title = f"&#9888; Alerta de Stock - {n_stock_alerts} productos"
            if n_negatives > 0:
                alert_title += f" ({n_negatives} en NEGATIVO)"

            sections.append(f"""
            <div style="background:#fff;border:2px solid #e74c3c;border-radius:8px;padding:20px;margin-bottom:20px;">
              <h2 style="color:#e74c3c;margin-top:0;">{alert_title}</h2>
              <p style="color:#666;margin:0 0 12px 0;">Productos con stock negativo o por debajo de {low_threshold} unidades</p>
              <table style="width:100%;border-collapse:collapse;font-size:13px;">
                <thead><tr style="background:#e74c3c;color:#fff;">
                  <th style="padding:10px;text-align:left;">Producto</th>
                  <th style="padding:10px;text-align:center;">Tipo</th>
                  <th style="padding:10px;text-align:center;">Warehouse</th>
                  <th style="padding:10px;text-align:center;">FBT</th>
                  <th style="padding:10px;text-align:center;">Total</th>
                  <th style="padding:10px;text-align:center;">Vta/Dia (30d)</th>
                  <th style="padding:10px;text-align:center;">Cobertura</th>
                </tr></thead>
                <tbody>{rows_html}</tbody>
              </table>
            </div>
            """)
        else:
            sections.append(f"""
            <div style="background:#eafaf1;border-radius:8px;padding:15px;margin-bottom:20px;">
              <h2 style="color:#27ae60;margin-top:0;">&#9989; Stock OK - Todos los productos por encima de {low_threshold} uds</h2>
            </div>
            """)

    # ── 4. AWAITING SHIPMENT ─────────────────────────────────────────────────
    awaiting_mask = pd.Series(False, index=df.index)
    if "Order Substatus" in df.columns:
        awaiting_mask |= df["Order Substatus"].astype(str).str.contains(
            "Awaiting shipment|Awaiting collection", case=False, na=False)
    if "Order Status" in df.columns:
        awaiting_mask |= df["Order Status"].astype(str).str.contains(
            "To ship", case=False, na=False)

    awaiting = df[awaiting_mask].copy()
    total_awaiting = awaiting["Order ID"].nunique() if not awaiting.empty else 0

    awaiting_wh_count  = 0
    awaiting_fbt_count = 0
    if not awaiting.empty and "Fulfillment Type" in awaiting.columns:
        fbt_mask = awaiting["Fulfillment Type"].astype(str).str.contains("TikTok", case=False, na=False)
        awaiting_wh_count  = awaiting[~fbt_mask]["Order ID"].nunique()
        awaiting_fbt_count = awaiting[fbt_mask]["Order ID"].nunique()

    if not awaiting.empty:
        sku_col = "SKU ID" if "SKU ID" in awaiting.columns else "SKU_ID_Clean"
        agg = awaiting.groupby("Order ID").agg(
            sku_count  =(sku_col,        "nunique"),
            total_qty  =("Quantity",     "sum"),
            created    =("Created Time", "first"),
            products   =("Product Name", lambda x: " | ".join([str(v) for v in x.unique()[:3] if str(v) != "nan"])),
        ).reset_index()
        agg["created"] = pd.to_datetime(agg["created"], errors="coerce")
        agg["days_waiting"] = (today - agg["created"]).dt.days.fillna(0).astype(int)

        flagged = agg[(agg["total_qty"] >= 2) | (agg["sku_count"] > 1)].sort_values(
            "days_waiting", ascending=False)
        stale   = agg[agg["days_waiting"] >= stale_days].sort_values("days_waiting", ascending=False)
        new_yesterday = len(agg[agg["days_waiting"] <= 1])

        warning_html = ""
        if not stale.empty:
            stale_items = "".join(
                f"<li><strong>{r['Order ID']}</strong> - {r['days_waiting']} dias - {r['products'][:60]}</li>"
                for _, r in stale.head(10).iterrows()
            )
            if len(stale) > 10:
                stale_items += f"<li>... y {len(stale)-10} ordenes mas</li>"
            warning_html = f"""
            <div style="background:#fdedec;border-left:4px solid #e74c3c;padding:12px;margin-bottom:15px;border-radius:4px;">
              <strong style="color:#e74c3c;">&#128680; ATENCION:</strong>
              {len(stale)} ordenes llevan <strong>{stale_days}+ dias</strong> sin prepararse:
              <ul style="margin:8px 0;">{stale_items}</ul>
            </div>"""

        flagged_table = ""
        if not flagged.empty:
            flagged_rows = ""
            for _, r in flagged.head(40).iterrows():
                bg = "#fdedec" if r["days_waiting"] >= stale_days else "#fff"
                flagged_rows += f"""
                <tr style="background:{bg};">
                  <td style="padding:6px 8px;border:1px solid #eee;font-family:monospace;">{r['Order ID']}</td>
                  <td style="padding:6px 8px;border:1px solid #eee;text-align:center;">{r['sku_count']}</td>
                  <td style="padding:6px 8px;border:1px solid #eee;text-align:center;font-weight:bold;">{r['total_qty']:.0f}</td>
                  <td style="padding:6px 8px;border:1px solid #eee;">{r['products'][:80]}</td>
                  <td style="padding:6px 8px;border:1px solid #eee;text-align:center;">{r['days_waiting']}</td>
                </tr>"""
            flagged_table = f"""
            <h3 style="color:#e67e22;margin:15px 0 8px;">Ordenes Combo x2+ o Multi-SKU ({len(flagged)})</h3>
            <table style="width:100%;border-collapse:collapse;font-size:12px;">
              <thead><tr style="background:#f39c12;color:#fff;">
                <th style="padding:8px;text-align:left;">Order ID</th>
                <th style="padding:8px;text-align:center;">SKUs</th>
                <th style="padding:8px;text-align:center;">Qty Total</th>
                <th style="padding:8px;text-align:left;">Productos</th>
                <th style="padding:8px;text-align:center;">Dias Espera</th>
              </tr></thead>
              <tbody>{flagged_rows}</tbody>
            </table>
            {"<p style='color:#999;font-size:11px;'>Mostrando 40 de " + str(len(flagged)) + "</p>" if len(flagged) > 40 else ""}"""

        sections.append(f"""
        <div style="background:#fff;border:2px solid #f39c12;border-radius:8px;padding:20px;margin-bottom:20px;">
          <h2 style="color:#e67e22;margin-top:0;">&#128230; Ordenes Pendientes de Envio</h2>
          <div style="margin-bottom:15px;padding:12px;background:#fef9e7;border-radius:6px;">
            <table style="width:100%;border-collapse:collapse;font-size:14px;">
              <tr>
                <td style="padding:8px;text-align:center;">
                  <div style="font-size:12px;color:#7f8c8d;">Total en espera</div>
                  <div style="font-size:24px;font-weight:bold;color:#e67e22;">{total_awaiting}</div>
                </td>
                <td style="padding:8px;text-align:center;">
                  <div style="font-size:12px;color:#7f8c8d;">Pendientes Warehouse</div>
                  <div style="font-size:24px;font-weight:bold;color:#2c3e50;">{awaiting_wh_count}</div>
                </td>
                <td style="padding:8px;text-align:center;">
                  <div style="font-size:12px;color:#7f8c8d;">Pendientes FBT</div>
                  <div style="font-size:24px;font-weight:bold;color:#3498db;">{awaiting_fbt_count}</div>
                </td>
                <td style="padding:8px;text-align:center;">
                  <div style="font-size:12px;color:#7f8c8d;">Combo x2+ / Multi-SKU</div>
                  <div style="font-size:24px;font-weight:bold;color:#e67e22;">{len(flagged)}</div>
                </td>
                <td style="padding:8px;text-align:center;">
                  <div style="font-size:12px;color:#7f8c8d;">Nuevas ayer</div>
                  <div style="font-size:24px;font-weight:bold;color:#2c3e50;">{new_yesterday}</div>
                </td>
                <td style="padding:8px;text-align:center;">
                  <div style="font-size:12px;color:#7f8c8d;">Mas de {stale_days} dias</div>
                  <div style="font-size:24px;font-weight:bold;color:#e74c3c;">{len(stale)}</div>
                </td>
              </tr>
            </table>
          </div>
          {warning_html}
          {flagged_table}
        </div>
        """)
    else:
        sections.append("""
        <div style="background:#eafaf1;border-radius:8px;padding:15px;margin-bottom:20px;">
          <h2 style="color:#27ae60;margin-top:0;">&#9989; No hay ordenes pendientes de envio</h2>
        </div>
        """)

    # ── Assemble HTML ────────────────────────────────────────────────────────
    now = datetime.now()
    html = f"""
    <!DOCTYPE html>
    <html>
    <head><meta charset="utf-8"></head>
    <body style="font-family:'Segoe UI',Arial,sans-serif;max-width:800px;margin:0 auto;padding:20px;background:#f5f6fa;">
      <div style="background:linear-gradient(135deg,#2c3e50,#3498db);color:#fff;padding:25px;border-radius:10px;margin-bottom:20px;text-align:center;">
        <h1 style="margin:0;font-size:24px;">{store_name} - Daily Report</h1>
        <p style="margin:8px 0 0;font-size:14px;opacity:0.9;">{now.strftime('%A %d de %B, %Y')}</p>
      </div>
      {"".join(sections)}
      <div style="text-align:center;padding:20px;color:#999;font-size:11px;">
        Generado automaticamente por Rodmat Dashboard V2<br>
        {now.strftime('%H:%M:%S')} UTC | Para dejar de recibir estos emails, contacta al administrador.
      </div>
    </body>
    </html>
    """

    # ── Build subject ────────────────────────────────────────────────────────
    subject = f"{store_name} Daily Report - {yesterday.strftime('%d/%m/%Y')} | ${rev_y:,.0f} | {orders_y} ordenes"
    if total_awaiting > 0:
        subject += f" | {total_awaiting} pendientes envio"
    if n_stock_alerts > 0:
        subject += f" | ALERTA {n_stock_alerts} stock bajo"
        if n_negatives > 0:
            subject += f" ({n_negatives} negativos)"

    return html, subject


def send_report(html: str, recipients: list[str], store_name: str, subject: str = None) -> bool:
    """Send HTML report via Resend API."""
    if not RESEND_API_KEY or not recipients:
        print("RESEND_API_KEY not set or no recipients, skipping send")
        return False
    if not subject:
        subject = f"{store_name} - Reporte Diario {datetime.now().strftime('%d/%m/%Y')}"
    try:
        import httpx
        to = [addr.lower() for addr in recipients]
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
    html, subject = build_report(db, store_id)
    return send_report(html, recipients, store.name, subject)


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
