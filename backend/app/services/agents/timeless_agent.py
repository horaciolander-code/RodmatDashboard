"""
TIMELESS — Monthly Business Close + Year-End Projection
Runs on day 1 of every month, reporting the month that just closed.
Data source: sales_orders + affiliate_sales via the shared analytics helpers.
Same Groq LLM + Resend email pipeline as the other agents.

Naming: Avon Timeless Classic Collection — fragrance from the operator's
catalog. Evokes both the closed month (atemporal) and the year-end
horizon (proyección).
"""
import re
from calendar import monthrange
from datetime import datetime

import pandas as pd
from sqlalchemy.orm import Session

from app.services.agents._base import (
    call_groq, send_email, get_recipients,
    get_business_context, is_agent_enabled,
    load_orders_df, load_creator_df,
)

AGENT_NAME     = "TIMELESS"
AGENT_SUBTITLE = "Monthly Close + Year-End Projection"

_MONTHS_ES = ["", "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
              "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]


# ── Snapshot ──────────────────────────────────────────────────────────────────

def _bounds(year: int, month: int):
    """Return (start_ts, end_exclusive_ts) for a calendar month."""
    last = monthrange(year, month)[1]
    return pd.Timestamp(year, month, 1), pd.Timestamp(year, month, last) + pd.Timedelta(days=1)


def _prev_month(year: int, month: int):
    return (year, month - 1) if month > 1 else (year - 1, 12)


def extract_snapshot(db: Session, store_id: str) -> dict:
    orders_df  = load_orders_df(db, store_id)
    creator_df = load_creator_df(db, store_id)
    today      = pd.Timestamp.now()

    # Closed month = previous calendar month (regardless of today.day).
    cy, cm = _prev_month(today.year, today.month)
    month_start, month_end = _bounds(cy, cm)

    py, pm = _prev_month(cy, cm)
    prev_start, prev_end = _bounds(py, pm)

    yoy_year = cy - 1
    yoy_start, yoy_end = _bounds(yoy_year, cm)

    active_statuses = ["Shipped", "Completed", "Delivered", "To ship"]

    def _slice(df, start, end):
        if df.empty or "Order_Date" not in df.columns:
            return pd.DataFrame()
        return df[(df["Order_Date"] >= start) & (df["Order_Date"] < end) &
                  df["Order Status"].isin(active_statuses)]

    cur = _slice(orders_df, month_start, month_end)
    prev = _slice(orders_df, prev_start, prev_end)
    yoy = _slice(orders_df, yoy_start, yoy_end)

    def _pct(a, b):
        return round((a - b) / abs(b) * 100, 1) if b else None

    gmv_cur  = round(float(cur["SKU Subtotal After Discount"].sum()), 2) if not cur.empty else 0.0
    gmv_prev = round(float(prev["SKU Subtotal After Discount"].sum()), 2) if not prev.empty else 0.0
    gmv_yoy  = round(float(yoy["SKU Subtotal After Discount"].sum()), 2) if not yoy.empty else 0.0
    ord_cur  = int(cur["Order ID"].nunique()) if not cur.empty else 0
    ord_prev = int(prev["Order ID"].nunique()) if not prev.empty else 0
    ord_yoy  = int(yoy["Order ID"].nunique()) if not yoy.empty else 0
    units_cur = int(cur["Quantity"].sum()) if not cur.empty else 0

    # YTD (Jan 1 → end of closed month)
    ytd_start = pd.Timestamp(cy, 1, 1)
    ytd = _slice(orders_df, ytd_start, month_end)
    gmv_ytd = round(float(ytd["SKU Subtotal After Discount"].sum()), 2) if not ytd.empty else 0.0
    ord_ytd = int(ytd["Order ID"].nunique()) if not ytd.empty else 0

    # Prior-year YTD through the equivalent month
    py_ytd = _slice(orders_df, pd.Timestamp(cy - 1, 1, 1), _bounds(cy - 1, cm)[1])
    gmv_py_ytd = round(float(py_ytd["SKU Subtotal After Discount"].sum()), 2) if not py_ytd.empty else 0.0

    months_elapsed   = cm                # if closed=May (5), 5 months elapsed
    months_remaining = 12 - cm

    # Per-month GMV for the last 3 closed months (for run-rate + slope)
    last3 = []
    for offset in range(3):
        my, mm = cy, cm - offset
        while mm <= 0:
            my -= 1
            mm += 12
        ms, me = _bounds(my, mm)
        d = _slice(orders_df, ms, me)
        last3.append(float(d["SKU Subtotal After Discount"].sum()) if not d.empty else 0.0)
    avg_last3 = sum(last3) / 3 if last3 else 0.0

    # 3 projections for end-of-year
    proj_linear  = round(gmv_ytd / months_elapsed * 12, 2) if months_elapsed else 0.0
    proj_runrate = round(gmv_ytd + avg_last3 * months_remaining, 2)
    if len(last3) >= 3 and any(x > 0 for x in last3):
        slope = (last3[0] - last3[2]) / 2.0   # GMV per month change
        proj = gmv_ytd
        for r in range(1, months_remaining + 1):
            proj += max(0.0, last3[0] + slope * r)
        proj_trend = round(proj, 2)
    else:
        proj_trend = proj_runrate

    # Top 10 products of the closed month
    top_products = []
    if not cur.empty and "Product Name" in cur.columns:
        tp = (cur.groupby("Product Name")
              .agg(gmv=("SKU Subtotal After Discount", "sum"),
                   units=("Quantity", "sum"))
              .nlargest(10, "gmv").reset_index())
        top_products = [{"name": str(r["Product Name"])[:60],
                         "gmv": round(float(r["gmv"]), 2),
                         "units": int(r["units"])}
                        for _, r in tp.iterrows()]

    # Affiliates this month + top 5 creators
    aff_gmv = aff_orders = 0
    top_creators = []
    if not creator_df.empty and "Time Created" in creator_df.columns:
        creator_df = creator_df.copy()
        creator_df["Time Created"] = pd.to_datetime(creator_df["Time Created"], errors="coerce")
        cr = creator_df[(creator_df["Time Created"] >= month_start) &
                        (creator_df["Time Created"] < month_end)]
        if "Payment Amount" in cr.columns:
            aff_gmv = round(float(cr["Payment Amount"].sum()), 2)
        if "Order ID" in cr.columns:
            aff_orders = int(cr["Order ID"].nunique())
        if not cr.empty and "Creator Username" in cr.columns:
            tc = (cr.groupby("Creator Username")
                  .agg(gmv=("Payment Amount", "sum"),
                       orders=("Order ID", "nunique"))
                  .nlargest(5, "gmv").reset_index())
            top_creators = [{"name": str(r["Creator Username"]),
                             "gmv": round(float(r["gmv"]), 2),
                             "orders": int(r["orders"])}
                            for _, r in tc.iterrows()]

    aff_share_pct = round(aff_gmv / gmv_cur * 100, 1) if gmv_cur else 0.0

    return {
        "analysis_date":     today.strftime("%Y-%m-%d"),
        "closed_month_name": f"{_MONTHS_ES[cm]} {cy}",
        "closed_year":       cy,
        "closed_month":      cm,
        "month_start":       str(month_start.date()),
        "month_end":         str((month_end - pd.Timedelta(days=1)).date()),
        "gmv_cur":           gmv_cur,
        "gmv_prev":          gmv_prev,
        "gmv_yoy":           gmv_yoy,
        "gmv_mom_pct":       _pct(gmv_cur, gmv_prev),
        "gmv_yoy_pct":       _pct(gmv_cur, gmv_yoy),
        "orders_cur":        ord_cur,
        "orders_prev":       ord_prev,
        "orders_yoy":        ord_yoy,
        "units_cur":         units_cur,
        "gmv_ytd":           gmv_ytd,
        "orders_ytd":        ord_ytd,
        "gmv_py_ytd":        gmv_py_ytd,
        "ytd_yoy_pct":       _pct(gmv_ytd, gmv_py_ytd),
        "months_elapsed":    months_elapsed,
        "months_remaining":  months_remaining,
        "proj_linear":       proj_linear,
        "proj_runrate":      proj_runrate,
        "proj_trend":        proj_trend,
        "last3_avg_gmv":     round(avg_last3, 2),
        "last3_monthly":     [round(x, 2) for x in last3],
        "top_products":      top_products,
        "aff_gmv":           aff_gmv,
        "aff_orders":        aff_orders,
        "aff_share_pct":     aff_share_pct,
        "top_creators":      top_creators,
    }


# ── Prompt ────────────────────────────────────────────────────────────────────

_PROMPT = """\
Eres TIMELESS, agente de cierre mensual y proyección anual de {store_name}.
{business_context_line}
Produce un informe ejecutivo en ESPAÑOL con estas 5 secciones EXACTAS
(usa los headers con === tal cual, parsearé por ellos):

=== CIERRE DEL MES ===
Comenta GMV del mes vs mes anterior y vs mismo mes año pasado.
¿Drivers principales? ¿Producto/categoría/canal que lideró el
crecimiento o arrastró a la baja? ¿Qué movió la aguja? Sé concreto
con números.

=== PROYECCIÓN DE CIERRE DE AÑO ===
Estamos en mes {months_elapsed}/12, quedan {months_remaining} meses.
Comenta las 3 proyecciones que te paso (lineal naive, run-rate
últimos 3 meses, tendencia). ¿Cuál es más realista para este negocio
y por qué? Da una banda razonable (low / mid / high). Si el ritmo
actual se mantiene, ¿es un buen año? Propón un target ambicioso pero
alcanzable y di qué ritmo mensual harían falta para alcanzarlo.

=== TOP PRODUCTOS Y CANAL AFILIADOS ===
Top productos del mes y top creadores. ¿Hay producto que está
despegando o muriendo? ¿El canal afiliados crece, se estanca, o
pierde peso vs venta directa? Concentración: ¿el top-3 productos
pesa demasiado?

=== ALERTAS Y RIESGOS ===
Tendencias preocupantes — declives MoM o YoY persistentes,
dependencia de un único producto o creador, estacionalidad mal
aprovechada, GMV plano cuando debería crecer. Sé directo, no
diplomático.

=== PRIORIDADES MES QUE EMPIEZA ===
Lista de 3-5 acciones concretas, priorizadas y ejecutables para el
mes que comienza. Cada una con dueño implícito y métrica de éxito.
"""


def _build_prompt(snapshot: dict, store_name: str, business_context: str) -> str:
    mom = f"{snapshot['gmv_mom_pct']:+.1f}%" if snapshot["gmv_mom_pct"] is not None else "N/A"
    yoy = f"{snapshot['gmv_yoy_pct']:+.1f}%" if snapshot["gmv_yoy_pct"] is not None else "N/A (sin histórico)"
    ytd_yoy = f"{snapshot['ytd_yoy_pct']:+.1f}%" if snapshot["ytd_yoy_pct"] is not None else "N/A"

    top_prod_txt = "\n".join(
        f"  {p['name']}: ${p['gmv']:,.0f} ({p['units']} uds)"
        for p in snapshot["top_products"]
    ) or "  Sin datos del mes."
    top_cr_txt = "\n".join(
        f"  {c['name']}: ${c['gmv']:,.0f} ({c['orders']} órdenes)"
        for c in snapshot["top_creators"]
    ) or "  Sin datos de afiliados."

    bc_line = f"Contexto del negocio: {business_context}\n" if business_context else ""
    header = _PROMPT.format(
        store_name=store_name,
        business_context_line=bc_line,
        months_elapsed=snapshot["months_elapsed"],
        months_remaining=snapshot["months_remaining"],
    )

    return header + f"""

DATOS DEL MES CERRADO ({snapshot['closed_month_name']}):

VENTAS DEL MES:
  GMV:         ${snapshot['gmv_cur']:,.0f}  ({snapshot['orders_cur']} órdenes, {snapshot['units_cur']} uds)
  Mes ant.:    ${snapshot['gmv_prev']:,.0f}  → MoM: {mom}
  Mismo mes año pasado: ${snapshot['gmv_yoy']:,.0f}  → YoY: {yoy}

YEAR-TO-DATE (1 ene → fin de {snapshot['closed_month_name']}):
  GMV YTD:     ${snapshot['gmv_ytd']:,.0f}  ({snapshot['orders_ytd']} órdenes)
  YTD año ant: ${snapshot['gmv_py_ytd']:,.0f}  → YTD YoY: {ytd_yoy}

PROYECCIONES CIERRE {snapshot['closed_year']}:
  Lineal naive (YTD × 12/{snapshot['months_elapsed']}):                 ${snapshot['proj_linear']:,.0f}
  Run-rate (YTD + avg 3 meses × {snapshot['months_remaining']}):        ${snapshot['proj_runrate']:,.0f}
  Tendencia (proyectando slope últimos 3 meses):                        ${snapshot['proj_trend']:,.0f}
  (Avg GMV últimos 3 meses: ${snapshot['last3_avg_gmv']:,.0f})

TOP 10 PRODUCTOS DEL MES:
{top_prod_txt}

CANAL AFILIADOS DEL MES:
  GMV afiliados: ${snapshot['aff_gmv']:,.0f}  ({snapshot['aff_orders']} órdenes) — {snapshot['aff_share_pct']:.1f}% del GMV total
  Top 5 creadores:
{top_cr_txt}
"""


# ── HTML builder ──────────────────────────────────────────────────────────────

def _parse_sections(text: str) -> dict:
    """Parse the === HEADER === markers into a dict of {key: html_safe_block}."""
    out = {}
    sections = re.split(r"^=== (.*?) ===\s*$", text, flags=re.MULTILINE)
    # sections = [preamble, title1, body1, title2, body2, ...]
    for i in range(1, len(sections), 2):
        title = sections[i].strip().lower()
        body = sections[i + 1].strip() if i + 1 < len(sections) else ""
        key = (
            "cierre" if "cierre" in title and "año" not in title else
            "proyeccion" if "proyec" in title else
            "productos" if "producto" in title else
            "alertas" if "alerta" in title else
            "prioridades" if "prioridad" in title else
            title
        )
        # Convert plain text to simple HTML paragraphs
        body_html = "<br>".join(line for line in body.split("\n") if line.strip())
        out[key] = body_html
    return out


def _card(title: str, body_html: str, accent: str, bg: str = "#ffffff") -> str:
    return f"""
  <div style="background:{bg};border:1px solid #e0e0e0;border-left:5px solid {accent};
       border-radius:8px;padding:18px;margin-bottom:16px;">
    <h3 style="color:{accent};margin:0 0 10px;font-size:13px;text-transform:uppercase;
        letter-spacing:1.5px;">{title}</h3>
    <div style="color:#2c3e50;font-size:13px;line-height:1.55;">{body_html}</div>
  </div>"""


def build_email_html(analysis_text: str, snapshot: dict, store_name: str = "Store") -> str:
    today = datetime.now()
    sections = _parse_sections(analysis_text)

    mom = snapshot["gmv_mom_pct"]
    mom_color = "#27ae60" if (mom or 0) >= 0 else "#e74c3c"
    mom_str = f"{mom:+.1f}%" if mom is not None else "N/A"

    yoy = snapshot["gmv_yoy_pct"]
    yoy_str = f"{yoy:+.1f}%" if yoy is not None else "—"

    top_prod_rows = "".join(
        f"<tr><td style='padding:6px 10px;border-bottom:1px solid #f5f5f5;font-size:12px;'>{p['name']}</td>"
        f"<td style='padding:6px 10px;text-align:right;font-weight:bold;font-size:12px;'>${p['gmv']:,.0f}</td>"
        f"<td style='padding:6px 10px;text-align:center;font-size:12px;'>{p['units']}</td></tr>"
        for p in snapshot["top_products"]
    ) or "<tr><td colspan='3' style='padding:14px;color:#aaa;text-align:center;font-size:12px;'>Sin datos del mes.</td></tr>"

    # Projection band (use min/median/max of the 3)
    projs = sorted([snapshot["proj_linear"], snapshot["proj_runrate"], snapshot["proj_trend"]])

    return f"""<!DOCTYPE html><html><head><meta charset="utf-8"></head>
<body style="font-family:'Segoe UI',Arial,sans-serif;max-width:860px;margin:0 auto;padding:20px;background:#f5f6fa;">
  <div style="background:linear-gradient(135deg,#1a2980,#26d0ce);color:#fff;padding:28px;border-radius:12px;margin-bottom:22px;">
    <table width="100%"><tr>
      <td><div style="font-size:10px;letter-spacing:4px;opacity:0.7;text-transform:uppercase;">{store_name} Operations</div>
        <div style="font-size:30px;font-weight:700;letter-spacing:3px;margin:4px 0;">{AGENT_NAME}</div>
        <div style="font-size:12px;opacity:0.8;">{AGENT_SUBTITLE}</div></td>
      <td style="text-align:right;vertical-align:top;">
        <div style="font-size:14px;font-weight:bold;">Cierre {snapshot['closed_month_name']}</div>
        <div style="font-size:12px;opacity:0.8;">{snapshot['month_start']} → {snapshot['month_end']}</div>
        <div style="margin-top:8px;background:rgba(255,255,255,0.2);padding:4px 14px;border-radius:20px;font-size:18px;font-weight:bold;display:inline-block;">{mom_str} vs mes ant.</div>
      </td>
    </tr></table>
  </div>
  <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:20px;border-collapse:collapse;"><tr>
    <td width="33%" style="padding:4px;">
      <div style="background:#fff;border:1px solid #e0e0e0;border-radius:8px;padding:14px;text-align:center;">
        <div style="font-size:11px;color:#999;text-transform:uppercase;">GMV {snapshot['closed_month_name']}</div>
        <div style="font-size:22px;font-weight:800;color:#2c3e50;">${snapshot['gmv_cur']:,.0f}</div>
        <div style="font-size:12px;color:{mom_color};">{mom_str} MoM · YoY {yoy_str}</div>
      </div></td>
    <td width="33%" style="padding:4px;">
      <div style="background:#fff;border:1px solid #e0e0e0;border-radius:8px;padding:14px;text-align:center;">
        <div style="font-size:11px;color:#999;text-transform:uppercase;">GMV YTD ({snapshot['months_elapsed']}m)</div>
        <div style="font-size:22px;font-weight:800;color:#2c3e50;">${snapshot['gmv_ytd']:,.0f}</div>
        <div style="font-size:12px;color:#aaa;">{snapshot['orders_ytd']} órdenes</div>
      </div></td>
    <td width="33%" style="padding:4px;">
      <div style="background:#fff;border:1px solid #e0e0e0;border-radius:8px;padding:14px;text-align:center;">
        <div style="font-size:11px;color:#999;text-transform:uppercase;">Proyección Cierre {snapshot['closed_year']}</div>
        <div style="font-size:16px;font-weight:800;color:#2c3e50;">${projs[1]:,.0f}</div>
        <div style="font-size:11px;color:#aaa;">banda: ${projs[0]:,.0f} → ${projs[2]:,.0f}</div>
      </div></td>
  </tr></table>
  {_card("Cierre del Mes", sections.get("cierre","—"), "#3498db")}
  {_card("Proyección de Cierre de Año", sections.get("proyeccion","—"), "#1a2980")}
  {_card("Top Productos y Canal Afiliados", sections.get("productos","—"), "#9b59b6")}
  {_card("Alertas y Riesgos", sections.get("alertas","—"), "#e74c3c", "#fffafa")}
  {_card("Prioridades Mes que Empieza", sections.get("prioridades","—"), "#27ae60")}
  <div style="background:#fff;border:1px solid #e0e0e0;border-radius:8px;padding:18px;margin-bottom:16px;">
    <h3 style="color:#2c3e50;margin:0 0 12px;font-size:13px;text-transform:uppercase;">Top 10 Productos {snapshot['closed_month_name']}</h3>
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

def run(db: Session, store_id: str, force: bool = False, test_email: str | None = None) -> bool:
    from app.models.store import Store
    today = datetime.now()
    # Day-1-of-month gate, unless forced.
    if not force and today.day != 1:
        return False

    store = db.query(Store).filter(Store.id == store_id).first()
    if not is_agent_enabled(store, "timeless"):
        print(f"[TIMELESS] Disabled by tenant settings for store {store_id[:8]}")
        return False
    recipients = [test_email] if test_email else get_recipients(store)
    if not recipients:
        print(f"[TIMELESS] No recipients for store {store_id}")
        return False

    store_name = store.name if store else "Store"
    business_context = get_business_context(store)

    print(f"[TIMELESS] Extracting snapshot for {store_name}...")
    snapshot = extract_snapshot(db, store_id)
    mom = f"{snapshot['gmv_mom_pct']:+.1f}%" if snapshot["gmv_mom_pct"] is not None else "N/A"
    print(f"[TIMELESS] {snapshot['closed_month_name']} GMV ${snapshot['gmv_cur']:,.0f} ({mom} vs prev month)")

    print("[TIMELESS] Calling Groq...")
    analysis = call_groq(_build_prompt(snapshot, store_name, business_context), max_tokens=3072)
    html = build_email_html(analysis, snapshot, store_name)
    subject = (f"TIMELESS · {snapshot['closed_month_name']} · {store_name} · "
               f"Monthly Close ${snapshot['gmv_cur']:,.0f} ({mom}) · Proy año ~${sorted([snapshot['proj_linear'], snapshot['proj_runrate'], snapshot['proj_trend']])[1]:,.0f}")
    ok = send_email(html, subject, recipients)
    print(f"[TIMELESS] Email {'sent' if ok else 'FAILED'}")
    return ok
