"""
FARAWAY — Weekly Business Close Report (V2)
Data source: Neon PostgreSQL. Finance section omitted (not in V2 yet).
Logic identical to V1 except data loading layer.
"""
import re
from datetime import datetime

import pandas as pd
from sqlalchemy.orm import Session

from app.services.agents._base import (
    call_groq, send_email, get_recipients,
    load_orders_df, load_kpis, load_creator_df,
)

AGENT_NAME     = "FARAWAY"
AGENT_SUBTITLE = "Weekly Business Close"


# ── Snapshot ──────────────────────────────────────────────────────────────────

def extract_snapshot(db: Session, store_id: str) -> dict:
    orders_df  = load_orders_df(db, store_id)
    kpis       = load_kpis(db, store_id)
    creator_df = load_creator_df(db, store_id)
    today      = pd.Timestamp.now()

    days_since_monday = today.weekday()
    week_start = (today - pd.Timedelta(days=days_since_monday)).normalize()
    week_end   = today
    prev_start = week_start - pd.Timedelta(days=7)
    prev_end   = week_start

    active_statuses = ["Shipped", "Completed", "Delivered", "To ship"]

    def _week(df, start, end):
        return df[(df["Order_Date"] >= start) & (df["Order_Date"] < end) &
                  df["Order Status"].isin(active_statuses)]

    cur_orders  = _week(orders_df, week_start, week_end)
    prev_orders = _week(orders_df, prev_start, prev_end)

    def _pct(a, b): return round((a - b) / abs(b) * 100, 1) if b != 0 else None

    gmv_cur  = round(cur_orders["SKU Subtotal After Discount"].sum(), 2)
    gmv_prev = round(prev_orders["SKU Subtotal After Discount"].sum(), 2)
    ord_cur  = cur_orders["Order ID"].nunique()
    ord_prev = prev_orders["Order ID"].nunique()
    units_cur = int(cur_orders["Quantity"].sum())

    # MTD
    mtd_start = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    mtd = orders_df[(orders_df["Order_Date"] >= mtd_start) & orders_df["Order Status"].isin(active_statuses)]
    gmv_mtd       = round(mtd["SKU Subtotal After Discount"].sum(), 2)
    days_elapsed  = today.day
    gmv_projected = round(gmv_mtd / days_elapsed * 30, 2) if days_elapsed > 0 else 0

    # Top products this week
    top_products = []
    if not cur_orders.empty and "Product Name" in cur_orders.columns:
        tp = (cur_orders.groupby("Product Name")
              .agg(gmv=("SKU Subtotal After Discount","sum"), units=("Quantity","sum"))
              .nlargest(5,"gmv").reset_index())
        top_products = [{"name": r["Product Name"][:55], "gmv": round(r["gmv"],2), "units": int(r["units"])}
                        for _, r in tp.iterrows()]

    # Affiliate this week
    aff_gmv_cur = aff_orders_cur = 0
    top_creators = []
    if not creator_df.empty and "Time Created" in creator_df.columns:
        creator_df["Time Created"] = pd.to_datetime(creator_df["Time Created"], errors="coerce")
        cr_cur = creator_df[(creator_df["Time Created"] >= week_start) &
                            (creator_df["Time Created"] < week_end)]
        aff_gmv_cur    = round(cr_cur["Payment Amount"].sum(), 2) if "Payment Amount" in cr_cur.columns else 0
        aff_orders_cur = cr_cur["Order ID"].nunique() if "Order ID" in cr_cur.columns else 0
        if not cr_cur.empty and "Creator Username" in cr_cur.columns:
            tc = (cr_cur.groupby("Creator Username")
                  .agg(gmv=("Payment Amount","sum"), orders=("Order ID","nunique"))
                  .nlargest(5,"gmv").reset_index())
            top_creators = [{"name": r["Creator Username"], "gmv": round(r["gmv"],2), "orders": int(r["orders"])}
                            for _, r in tc.iterrows()]

    # Inventory highlights
    low_stock = []; top_movers = []
    if not kpis.empty:
        active = kpis[kpis["AvgVentas30d"] > 0.05].copy()
        low_stock = (active[active["StockActualizado"] < active["AvgVentas30d"] * 14]
                     [["ProductoNombre","StockActualizado","AvgVentas30d"]]
                     .nsmallest(5,"StockActualizado").to_dict("records"))
        top_movers = (active.nlargest(5,"AvgVentas30d")
                      [["ProductoNombre","AvgVentas30d","StockActualizado"]].to_dict("records"))

    # Monthly trend
    shipped = orders_df[orders_df["Order Status"].isin(["Shipped","Completed","Delivered"])].copy()
    shipped["Month"] = shipped["Order_Date"].dt.to_period("M")
    cutoff = today - pd.DateOffset(months=3)
    monthly = (shipped[shipped["Order_Date"] >= cutoff]
               .groupby("Month")
               .agg(GMV=("SKU Subtotal After Discount","sum"), Orders=("Order ID","nunique"))
               .reset_index())
    monthly_list = [{"month": str(r["Month"]), "gmv": round(r["GMV"],2), "orders": int(r["Orders"])}
                    for _, r in monthly.iterrows()]

    return {
        "analysis_date":    today.strftime("%Y-%m-%d"),
        "week_start":       week_start.strftime("%Y-%m-%d"),
        "week_end":         today.strftime("%Y-%m-%d"),
        "gmv_cur":          gmv_cur,
        "gmv_prev":         gmv_prev,
        "gmv_pct":          _pct(gmv_cur, gmv_prev),
        "orders_cur":       int(ord_cur),
        "orders_prev":      int(ord_prev),
        "units_cur":        units_cur,
        "gmv_mtd":          gmv_mtd,
        "gmv_projected":    gmv_projected,
        "days_elapsed":     days_elapsed,
        "top_products":     top_products,
        "aff_gmv_cur":      aff_gmv_cur,
        "aff_orders_cur":   int(aff_orders_cur),
        "top_creators":     top_creators,
        "low_stock":        low_stock,
        "top_movers":       top_movers,
        "monthly_trend":    monthly_list,
    }


# ── Groq prompt ───────────────────────────────────────────────────────────────

_PROMPT = """\
Eres FARAWAY, el agente de cierre semanal de negocio de Rodmat.
Rodmat vende fragancias Avon en TikTok Shop (EE.UU.).

Produce un informe de cierre semanal ejecutivo en ESPAÑOL con estas 4 secciones:

=== PERFORMANCE DE LA SEMANA ===
(GMV vs semana anterior, tendencia, drivers del cambio. ¿Fue buena o mala semana? ¿Por qué?)

=== CANAL DE AFILIADOS ===
(¿Qué % del GMV viene de creadores? ¿Los top performers de la semana? ¿Está creciendo el canal?)

=== ALERTAS DE INVENTARIO ===
(Productos con stock crítico (<14 días). ¿Hay riesgo de ruptura la próxima semana?)

=== PRIORIDADES SEMANA QUE VIENE ===
Lista de 3-5 acciones concretas y priorizadas para la semana siguiente.
"""


def _build_prompt(snapshot: dict) -> str:
    gmv_pct_str = f"{snapshot['gmv_pct']:+.1f}%" if snapshot["gmv_pct"] is not None else "N/A"
    top_prod_txt = "\n".join(f"  {p['name']}: ${p['gmv']:,.0f} ({p['units']} uds)"
                             for p in snapshot["top_products"]) or "  Sin datos."
    top_cr_txt = "\n".join(f"  {c['name']}: ${c['gmv']:,.0f} ({c['orders']} ordenes)"
                           for c in snapshot["top_creators"]) or "  Sin datos de afiliados."
    low_txt = "\n".join(f"  {r['ProductoNombre']}: stock={r['StockActualizado']:.0f} vel={r['AvgVentas30d']:.2f}/d"
                        for r in snapshot["low_stock"]) or "  Sin productos críticos."
    monthly_txt = "\n".join(f"  {m['month']}: ${m['gmv']:,.0f} ({m['orders']} ordenes)"
                            for m in snapshot["monthly_trend"])

    return _PROMPT + f"""

DATOS DE LA SEMANA ({snapshot['week_start']} → {snapshot['week_end']}):

VENTAS:
  Esta semana: ${snapshot['gmv_cur']:,.0f} ({int(snapshot['orders_cur'])} órdenes, {snapshot['units_cur']} uds)
  Semana anterior: ${snapshot['gmv_prev']:,.0f} | Cambio: {gmv_pct_str}
  MTD {snapshot['days_elapsed']}d: ${snapshot['gmv_mtd']:,.0f} → Proyección: ${snapshot['gmv_projected']:,.0f}

TOP PRODUCTOS ESTA SEMANA:
{top_prod_txt}

CANAL AFILIADOS ESTA SEMANA:
  GMV afiliados: ${snapshot['aff_gmv_cur']:,.0f} ({snapshot['aff_orders_cur']} órdenes)
{top_cr_txt}

TENDENCIA MENSUAL:
{monthly_txt}

STOCK CRÍTICO (<14d cobertura):
{low_txt}
"""


# ── HTML builder ──────────────────────────────────────────────────────────────

def _parse_sections(text: str) -> dict:
    labels = {
        "performance": r"=== PERFORMANCE DE LA SEMANA ===",
        "afiliados":   r"=== CANAL DE AFILIADOS ===",
        "inventario":  r"=== ALERTAS DE INVENTARIO ===",
        "prioridades": r"=== PRIORIDADES SEMANA QUE VIENE ===",
    }
    keys = list(labels.keys()); patterns = list(labels.values())
    sections = {}
    for i, (key, pat) in enumerate(zip(keys, patterns)):
        nxt = patterns[i + 1] if i + 1 < len(patterns) else None
        m = re.search(pat + r"(.*?)" + (nxt if nxt else r"$"), text, re.S | re.I)
        sections[key] = m.group(1).strip() if m else ""
    if not any(sections.values()):
        sections["performance"] = text.strip()
    return sections


def _card(title, content, border="#3498db", bg="#fff"):
    content = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', content).replace("\n", "<br>")
    return (f'<div style="background:{bg};border:1px solid #e0e0e0;border-left:4px solid {border};'
            f'border-radius:8px;padding:20px;margin-bottom:16px;">'
            f'<h3 style="color:#2c3e50;margin:0 0 12px;font-size:14px;text-transform:uppercase;">{title}</h3>'
            f'<div style="color:#444;font-size:13px;line-height:1.8;">{content}</div></div>')


def build_email_html(analysis_text: str, snapshot: dict, store_name: str = "Rodmat") -> str:
    today    = datetime.now()
    sections = _parse_sections(analysis_text)
    gmv_pct  = snapshot["gmv_pct"]
    pct_color = "#27ae60" if (gmv_pct or 0) >= 0 else "#e74c3c"
    pct_str   = f"{gmv_pct:+.1f}%" if gmv_pct is not None else "N/A"

    top_prod_rows = "".join(
        f"<tr><td style='padding:6px 10px;border-bottom:1px solid #f5f5f5;font-size:12px;'>{p['name']}</td>"
        f"<td style='padding:6px 10px;text-align:right;font-weight:bold;font-size:12px;'>${p['gmv']:,.0f}</td>"
        f"<td style='padding:6px 10px;text-align:center;font-size:12px;'>{p['units']}</td></tr>"
        for p in snapshot["top_products"])

    return f"""<!DOCTYPE html><html><head><meta charset="utf-8"></head>
<body style="font-family:'Segoe UI',Arial,sans-serif;max-width:860px;margin:0 auto;padding:20px;background:#f5f6fa;">
  <div style="background:linear-gradient(135deg,#134e5e,#71b280);color:#fff;padding:28px;border-radius:12px;margin-bottom:22px;">
    <table width="100%"><tr>
      <td><div style="font-size:10px;letter-spacing:4px;opacity:0.7;text-transform:uppercase;">{store_name} Operations</div>
        <div style="font-size:30px;font-weight:700;letter-spacing:3px;margin:4px 0;">{AGENT_NAME}</div>
        <div style="font-size:12px;opacity:0.8;">{AGENT_SUBTITLE}</div></td>
      <td style="text-align:right;vertical-align:top;">
        <div style="font-size:14px;font-weight:bold;">Weekly Close Report</div>
        <div style="font-size:12px;opacity:0.8;">{snapshot['week_start']} → {snapshot['week_end']}</div>
        <div style="margin-top:8px;background:rgba(255,255,255,0.2);padding:4px 14px;border-radius:20px;font-size:18px;font-weight:bold;display:inline-block;color:{pct_color if False else '#fff'};">{pct_str} vs prev week</div>
      </td>
    </tr></table>
  </div>
  <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:20px;border-collapse:collapse;"><tr>
    <td width="33%" style="padding:4px;">
      <div style="background:#fff;border:1px solid #e0e0e0;border-radius:8px;padding:14px;text-align:center;">
        <div style="font-size:11px;color:#999;text-transform:uppercase;">GMV Semana</div>
        <div style="font-size:22px;font-weight:800;color:#2c3e50;">${snapshot['gmv_cur']:,.0f}</div>
        <div style="font-size:12px;color:{pct_color};">{pct_str} vs anterior</div>
      </div></td>
    <td width="33%" style="padding:4px;">
      <div style="background:#fff;border:1px solid #e0e0e0;border-radius:8px;padding:14px;text-align:center;">
        <div style="font-size:11px;color:#999;text-transform:uppercase;">GMV MTD</div>
        <div style="font-size:22px;font-weight:800;color:#2c3e50;">${snapshot['gmv_mtd']:,.0f}</div>
        <div style="font-size:12px;color:#aaa;">Proy: ${snapshot['gmv_projected']:,.0f}</div>
      </div></td>
    <td width="33%" style="padding:4px;">
      <div style="background:#fff;border:1px solid #e0e0e0;border-radius:8px;padding:14px;text-align:center;">
        <div style="font-size:11px;color:#999;text-transform:uppercase;">Afiliados GMV</div>
        <div style="font-size:22px;font-weight:800;color:#2c3e50;">${snapshot['aff_gmv_cur']:,.0f}</div>
        <div style="font-size:12px;color:#aaa;">{snapshot['aff_orders_cur']} órdenes</div>
      </div></td>
  </tr></table>
  {_card("Performance de la Semana", sections.get("performance","—"), "#3498db")}
  {_card("Canal de Afiliados", sections.get("afiliados","—"), "#9b59b6")}
  {_card("Alertas de Inventario", sections.get("inventario","—"), "#e74c3c", "#fffafa")}
  {_card("Prioridades Semana que Viene", sections.get("prioridades","—"), "#27ae60")}
  <div style="background:#fff;border:1px solid #e0e0e0;border-radius:8px;padding:18px;margin-bottom:16px;">
    <h3 style="color:#2c3e50;margin:0 0 12px;font-size:13px;text-transform:uppercase;">Top Productos Semana</h3>
    <table width="100%" style="border-collapse:collapse;font-size:12px;">
      <thead><tr style="background:#2c3e50;color:#fff;">
        <th style="padding:7px 10px;text-align:left;">Producto</th>
        <th style="padding:7px 10px;text-align:right;">GMV</th>
        <th style="padding:7px 10px;text-align:center;">Uds</th></tr></thead>
      <tbody>{top_prod_rows}</tbody>
    </table>
  </div>
  <div style="text-align:center;padding:14px;color:#aaa;font-size:10px;border-top:1px solid #e0e0e0;">
    <strong style="color:#888;">{AGENT_NAME}</strong> · {store_name} · {AGENT_SUBTITLE}<br>
    {today.strftime('%Y-%m-%d %H:%M')}
  </div>
</body></html>"""


# ── Entry point ───────────────────────────────────────────────────────────────

def run(db: Session, store_id: str, force: bool = False) -> bool:
    from app.models.store import Store
    today = datetime.now()
    if not force and today.weekday() != 4:  # Friday
        return False
    store = db.query(Store).filter(Store.id == store_id).first()
    recipients = get_recipients(store)
    if not recipients:
        print(f"[FARAWAY] No recipients for store {store_id}")
        return False
    store_name = store.name if store else "Store"

    print(f"[FARAWAY] Extracting snapshot for {store_name}...")
    snapshot = extract_snapshot(db, store_id)
    pct_str = f"{snapshot['gmv_pct']:+.1f}%" if snapshot["gmv_pct"] is not None else "N/A"
    print(f"[FARAWAY] GMV ${snapshot['gmv_cur']:,.0f} ({pct_str} vs prev week)")

    print("[FARAWAY] Calling Groq...")
    analysis = call_groq(_build_prompt(snapshot))
    html = build_email_html(analysis, snapshot, store_name)
    subject = (f"FARAWAY · {snapshot['analysis_date']} · {store_name} · "
               f"Weekly Close ${snapshot['gmv_cur']:,.0f} ({pct_str})")
    ok = send_email(html, subject, recipients)
    print(f"[FARAWAY] Email {'sent' if ok else 'FAILED'}")
    return ok
