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
    call_groq, send_email, get_recipients, is_agent_enabled, get_business_context,
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

    # Semana Rodmat: sábado → viernes (Saturday=5, Friday=4).
    # Si hoy es sábado (inicio de semana nueva), retroceder al viernes anterior
    # para mostrar la semana que acaba de cerrar.
    ref = today if today.weekday() != 5 else today - pd.Timedelta(days=1)
    days_since_saturday = (ref.weekday() - 5) % 7
    week_start = (ref - pd.Timedelta(days=days_since_saturday)).normalize()
    week_end   = (week_start + pd.Timedelta(days=7)).normalize()  # hasta el sábado siguiente (exclusive)
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
Eres FARAWAY, agente de cierre semanal de {store_name}.
{business_context_line}Analiza el performance de la semana con perspectiva del negocio en su mercado.

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


def _build_prompt(snapshot: dict, store_name: str, business_context: str) -> str:
    gmv_pct_str = f"{snapshot['gmv_pct']:+.1f}%" if snapshot["gmv_pct"] is not None else "N/A"
    top_prod_txt = "\n".join(f"  {p['name']}: ${p['gmv']:,.0f} ({p['units']} uds)"
                             for p in snapshot["top_products"]) or "  Sin datos."
    top_cr_txt = "\n".join(f"  {c['name']}: ${c['gmv']:,.0f} ({c['orders']} ordenes)"
                           for c in snapshot["top_creators"]) or "  Sin datos de afiliados."
    low_txt = "\n".join(f"  {r['ProductoNombre']}: stock={r['StockActualizado']:.0f} vel={r['AvgVentas30d']:.2f}/d"
                        for r in snapshot["low_stock"]) or "  Sin productos críticos."
    monthly_txt = "\n".join(f"  {m['month']}: ${m['gmv']:,.0f} ({m['orders']} ordenes)"
                            for m in snapshot["monthly_trend"])

    bc_line = f"Contexto del negocio: {business_context}\n" if business_context else ""
    header = _PROMPT.format(store_name=store_name, business_context_line=bc_line)
    return header + f"""

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


def _card(title, content, accent="#00D4FF", bg=None):
    """Gmail-safe dark card with left accent."""
    import re as _re
    content = _re.sub(r'\*\*(.+?)\*\*', rf'<span style="color:{accent};font-weight:700;">\1</span>', content or "—")
    content = content.replace("\n", "<br>")
    _open = (
        f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" '
        f'bgcolor="#141a3d" style="background-color:#141a3d;border-radius:10px;'
        f'margin-bottom:12px;border-left:4px solid {accent};">'
        f'<tr><td bgcolor="#141a3d" style="background-color:#141a3d;padding:18px 20px;'
        f'border-radius:10px;border-left:4px solid {accent};">'
    )
    _inner = (
        f'<h3 style="color:{accent};margin:0 0 10px;font-size:12px;'
        f'text-transform:uppercase;letter-spacing:1.5px;font-weight:700;">{title}</h3>'
        f'<div style="color:#c5cdd6;font-size:13px;line-height:1.7;">{content}</div>'
    )
    return _open + _inner + '</td></tr></table>'


def build_email_html(analysis_text: str, snapshot: dict, store_name: str = "Store") -> str:
    today    = datetime.now()
    sections = _parse_sections(analysis_text)
    gmv_pct  = snapshot["gmv_pct"]
    pct_color = "#00FF88" if (gmv_pct or 0) >= 0 else "#FF3D6B"
    pct_str   = f"{gmv_pct:+.1f}%" if gmv_pct is not None else "N/A"

    top_prod_rows = "".join(
        f"<tr><td style='padding:6px 10px;border-bottom:1px solid #2a2f5c;font-size:12px;'>{p['name']}</td>"
        f"<td style='padding:6px 10px;text-align:right;font-weight:bold;font-size:12px;'>${p['gmv']:,.0f}</td>"
        f"<td style='padding:6px 10px;text-align:center;font-size:12px;'>{p['units']}</td></tr>"
        for p in snapshot["top_products"])

    return f"""<!DOCTYPE html><html><head><meta charset="utf-8"></head>
<body style="margin:0;padding:0;background-color:#0a0e27;"><table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" bgcolor="#0a0e27" style="background-color:#0a0e27;margin:0;padding:0;"><tr><td align="center" bgcolor="#0a0e27" style="background-color:#0a0e27;padding:20px 10px;"><table role="presentation" width="860" cellpadding="0" cellspacing="0" border="0" bgcolor="#0a0e27" style="max-width:860px;background-color:#0a0e27;color:#e4e9ff;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,Arial,sans-serif;"><tr><td bgcolor="#0a0e27" style="background-color:#0a0e27;padding:0;">
  <div style="background-color:#0f142f;color:#fff;padding:28px;border-radius:12px;margin-bottom:22px;">
    <table width="100%"><tr>
      <td><div style="font-size:10px;letter-spacing:4px;opacity:0.7;text-transform:uppercase;">{store_name} Operations</div>
        <div style="font-size:30px;font-weight:700;letter-spacing:3px;margin:4px 0;">{AGENT_NAME}</div>
        <div style="font-size:12px;opacity:0.8;">{AGENT_SUBTITLE}</div></td>
      <td style="text-align:right;vertical-align:top;">
        <div style="font-size:14px;font-weight:bold;">Weekly Close Report</div>
        <div style="font-size:12px;opacity:0.8;">{snapshot['week_start']} → {snapshot['week_end']}</div>
        <div style="margin-top:8px;background-color:#1a2050;padding:4px 14px;border-radius:20px;font-size:18px;font-weight:bold;display:inline-block;color:{pct_color if False else '#0f142f'};">{pct_str} vs prev week</div>
      </td>
    </tr></table>
  </div>
  <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:20px;border-collapse:collapse;"><tr>
    <td width="33%" style="padding:4px;">
      <div style="background-color:#0f142f;border:1px solid #2a2f5c;border-radius:8px;padding:14px;text-align:center;">
        <div style="font-size:11px;color:#8892b0;text-transform:uppercase;">GMV Semana</div>
        <div style="font-size:22px;font-weight:800;color:#00D4FF;">${snapshot['gmv_cur']:,.0f}</div>
        <div style="font-size:12px;color:{pct_color};">{pct_str} vs anterior</div>
      </div></td>
    <td width="33%" style="padding:4px;">
      <div style="background-color:#0f142f;border:1px solid #2a2f5c;border-radius:8px;padding:14px;text-align:center;">
        <div style="font-size:11px;color:#8892b0;text-transform:uppercase;">GMV MTD</div>
        <div style="font-size:22px;font-weight:800;color:#00D4FF;">${snapshot['gmv_mtd']:,.0f}</div>
        <div style="font-size:12px;color:#8892b0;">Proy: ${snapshot['gmv_projected']:,.0f}</div>
      </div></td>
    <td width="33%" style="padding:4px;">
      <div style="background-color:#0f142f;border:1px solid #2a2f5c;border-radius:8px;padding:14px;text-align:center;">
        <div style="font-size:11px;color:#8892b0;text-transform:uppercase;">Afiliados GMV</div>
        <div style="font-size:22px;font-weight:800;color:#00D4FF;">${snapshot['aff_gmv_cur']:,.0f}</div>
        <div style="font-size:12px;color:#8892b0;">{snapshot['aff_orders_cur']} órdenes</div>
      </div></td>
  </tr></table>
  {_card("Performance de la Semana", sections.get("performance","—"), "#00D4FF")}
  {_card("Canal de Afiliados", sections.get("afiliados","—"), "#7B61FF")}
  {_card("Alertas de Inventario", sections.get("inventario","—"), "#FF3D6B", "#141a3d")}
  {_card("Prioridades Semana que Viene", sections.get("prioridades","—"), "#00FF88")}
  <div style="background-color:#0f142f;border:1px solid #2a2f5c;border-radius:8px;padding:18px;margin-bottom:16px;">
    <h3 style="color:#00D4FF;margin:0 0 12px;font-size:13px;text-transform:uppercase;">Top Productos Semana</h3>
    <table width="100%" style="border-collapse:collapse;font-size:12px;">
      <thead><tr style="background-color:#1a2050;color:#fff;">
        <th style="padding:7px 10px;text-align:left;">Producto</th>
        <th style="padding:7px 10px;text-align:right;">GMV</th>
        <th style="padding:7px 10px;text-align:center;">Uds</th></tr></thead>
      <tbody>{top_prod_rows}</tbody>
    </table>
  </div>
  <div style="text-align:center;padding:14px;color:#8892b0;font-size:10px;border-top:1px solid #2a2f5c;">
    <strong style="color:#8892b0;">{AGENT_NAME}</strong> · {store_name} · {AGENT_SUBTITLE}<br>
    {today.strftime('%Y-%m-%d %H:%M')}
  </div>
</td></tr></table></td></tr></table></body></html>"""


# ── Entry point ───────────────────────────────────────────────────────────────

def run(db: Session, store_id: str, force: bool = False, test_email: str | None = None, brand_slug: str | None = None) -> bool:
    from app.models.store import Store
    today = datetime.now()
    if not force and today.weekday() != 4:  # Friday
        return False
    store = db.query(Store).filter(Store.id == store_id).first()
    if not is_agent_enabled(store, "faraway"):
        print(f"[FARAWAY] Disabled by tenant settings for store {store_id[:8]}")
        return False
    recipients = [test_email] if test_email else get_recipients(store)
    if not recipients:
        print(f"[FARAWAY] No recipients for store {store_id}")
        return False
    store_name = store.name if store else "Store"

    print(f"[FARAWAY] Extracting snapshot for {store_name}...")
    snapshot = extract_snapshot(db, store_id)
    pct_str = f"{snapshot['gmv_pct']:+.1f}%" if snapshot["gmv_pct"] is not None else "N/A"
    print(f"[FARAWAY] GMV ${snapshot['gmv_cur']:,.0f} ({pct_str} vs prev week)")

    print("[FARAWAY] Calling Groq...")
    business_context = get_business_context(store)
    analysis = call_groq(_build_prompt(snapshot, store_name, business_context))
    html = build_email_html(analysis, snapshot, store_name)
    subject = (f"FARAWAY · {snapshot['analysis_date']} · {store_name} · "
               f"Weekly Close ${snapshot['gmv_cur']:,.0f} ({pct_str})")
    ok = send_email(html, subject, recipients)
    print(f"[FARAWAY] Email {'sent' if ok else 'FAILED'}")
    return ok
