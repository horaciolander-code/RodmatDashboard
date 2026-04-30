"""
HAIKU — Inventory Intelligence Agent (V2)
Data source: Neon PostgreSQL. Logic identical to V1.
"""
import re
from datetime import datetime

import pandas as pd
from sqlalchemy.orm import Session

from app.services.agents._base import (
    call_groq, send_email, get_recipients,
    load_orders_df, load_kpis, load_pending_df,
)

AGENT_NAME     = "HAIKU"
AGENT_SUBTITLE = "Inventory Intelligence Agent"
LEAD_TIME      = 21
MIN_ORDER      = 10000


# ── Snapshot ──────────────────────────────────────────────────────────────────

def extract_snapshot(db: Session, store_id: str) -> dict:
    kpis      = load_kpis(db, store_id)
    orders_df = load_orders_df(db, store_id)
    pend_df   = load_pending_df(db, store_id)
    today     = pd.Timestamp.now()

    # Monthly sales
    shipped = orders_df[
        orders_df["Order Status"].isin(["Shipped","Completed","Delivered"]) |
        orders_df["Shipped Time"].notna()
    ].copy()
    shipped["Month"] = shipped["Order_Date"].dt.to_period("M")
    cutoff = today - pd.DateOffset(months=6)
    monthly_agg = (
        shipped[shipped["Order_Date"] >= cutoff]
        .groupby("Month")
        .agg(GMV=("SKU Subtotal After Discount","sum"),
             Orders=("Order ID","nunique"),
             Units=("Quantity","sum"))
        .reset_index()
    )
    monthly_list = [{"month": str(r["Month"]), "gmv": round(r["GMV"],2),
                     "orders": int(r["Orders"]), "units": int(r["Units"])}
                    for _, r in monthly_agg.iterrows()]

    # MTD
    mtd_start = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    mtd = orders_df[
        (orders_df["Order_Date"] >= mtd_start) &
        orders_df["Order Status"].isin(["Shipped","Completed","Delivered","To ship"])
    ]
    rev_mtd   = round(mtd["SKU Subtotal After Discount"].sum(), 2)
    days_in   = today.day
    projected = round(rev_mtd / days_in * 30, 2) if days_in > 0 else 0

    # Pending orders
    pend_list = []
    days_until_pending = None
    pend_date_str = "none"
    last_order_date_str = "unknown"

    if not pend_df.empty:
        pend_active = pend_df[pend_df["Status"].astype(str).str.lower().str.contains("pending|pendiente", na=False)]
        for _, r in pend_active.iterrows():
            pend_list.append({
                "product":  str(r["Producto"]),
                "units":    int(r["Unidades pedidas"]) if pd.notna(r.get("Unidades pedidas")) else 0,
                "value":    round(float(r["Importe total"]), 2) if pd.notna(r.get("Importe total")) else 0,
                "expected": str(r["Fecha estimada entrega"])[:10] if pd.notna(r.get("Fecha estimada entrega")) else "unknown",
            })
        exp_date = pend_active["Fecha estimada entrega"].max()
        if pd.notna(exp_date):
            pend_date_str = str(exp_date)[:10]
            days_until_pending = (pd.Timestamp(exp_date) - today).days
        order_date = pend_active["Fecha pedido"].min()
        if pd.notna(order_date):
            last_order_date_str = str(order_date)[:10]

    pend_units = {}
    for item in pend_list:
        key = item["product"].lower().strip()
        pend_units[key] = pend_units.get(key, 0) + item["units"]

    days_to_pending = max(0, days_until_pending) if days_until_pending is not None else 0

    # Forward-looking stock analysis
    stock_rows = []
    active = kpis[(kpis.get("Initial_Stock", kpis.get("StockActualizado", 0)) > 0)].copy() if not kpis.empty else kpis
    for _, r in active.iterrows():
        key       = str(r["ProductoNombre"]).lower().strip()
        pending_u = pend_units.get(key, 0)
        vel       = r["AvgVentas30d"]
        cur       = r["StockActualizado"]
        total     = cur + pending_u
        cov_now   = round(cur   / vel, 1) if vel > 0.05 else 9999
        cov_full  = round(total / vel, 1) if vel > 0.05 else 9999
        stock_rows.append({
            "name":                      r["ProductoNombre"],
            "tipo":                      r["Tipo"],
            "stock_current":             int(cur),
            "stock_pending":             int(pending_u),
            "stock_total":               int(total),
            "daily_vel":                 round(vel, 2),
            "coverage_now":              cov_now,
            "coverage_days":             cov_full,
            "in_pending_order":          pending_u > 0,
            "stockout_before_pending":   (vel > 0.05) and (cov_now < days_to_pending),
            "stockout_before_new_order": (vel > 0.05) and (cov_full < LEAD_TIME),
        })
    stock_rows.sort(key=lambda x: x["coverage_days"] if x["coverage_days"] < 9000 else 9999)

    days_since_last_order = None
    if last_order_date_str != "unknown":
        try:
            days_since_last_order = (today - pd.Timestamp(last_order_date_str)).days
        except Exception:
            pass

    return {
        "analysis_date":           today.strftime("%Y-%m-%d"),
        "analysis_weekday":        today.strftime("%A"),
        "monthly_sales":           monthly_list,
        "mtd_revenue":             rev_mtd,
        "mtd_days_elapsed":        days_in,
        "mtd_projected":           projected,
        "pending_orders":          pend_list,
        "pending_expected":        pend_date_str,
        "days_until_pending":      days_until_pending,
        "days_to_pending_arrives": days_to_pending,
        "pending_overdue_days":    max(0, -(days_until_pending or 0)),
        "last_order_date":         last_order_date_str,
        "days_since_last_order":   days_since_last_order,
        "stock_with_pending":      stock_rows,
        "lead_time_days":          LEAD_TIME,
        "min_order_eur":           MIN_ORDER,
    }


# ── Groq prompt ───────────────────────────────────────────────────────────────

_PROMPT = """\
Eres HAIKU, el agente de inteligencia de inventario de Rodmat.
Rodmat vende fragancias Avon en TikTok Shop (EE.UU.).

REGLAS DE NEGOCIO:
1. Lead time proveedor (DC Company): SIEMPRE 21 días.
2. Política: UN pedido al mes, mínimo €10.000.
3. Umbral para pedir: ¿algún producto clave se queda sin stock antes del próximo pedido (hoy+21d)?
4. Si hay pedido pendiente, calcular cuántos días faltan y descontar unidades pendientes.
5. Si el pedido pendiente llega en <7 días → prácticamente ya llegó.
6. Si lleva retraso (days_until_pending < 0) → escalar con proveedor HOY.

Produce análisis ejecutivo en ESPAÑOL con estas 5 secciones:

=== RESUMEN EJECUTIVO ===
(3-4 frases: estado del negocio, riesgo principal, acción prioritaria)

=== TENDENCIA DE VENTAS ===
(GMV mensual, tendencia, proyección MTD, anomalías)

=== ESTADO DE INVENTARIO ===
Para cada producto problemático: ¿en el pedido pendiente? ¿días sin pedido? ¿crítico?

=== ORDEN PENDIENTE ===
(¿Cuántos días faltan o retraso? ¿Qué cubre? ¿Qué queda desprotegido?)

=== DECISIÓN DE COMPRA ===
VEREDICTO: [PEDIR YA / ESPERAR / NO PEDIR]
Lista 3-5 acciones concretas numeradas.
"""


def _build_prompt(snapshot: dict) -> str:
    monthly_txt = "\n".join(
        f"  {m['month']}: GMV=${m['gmv']:,.0f}  ordenes={m['orders']}  uds={m['units']}"
        for m in snapshot["monthly_sales"])
    dtp = snapshot["days_until_pending"]
    if dtp is None:   pending_timing = "Sin pedido pendiente activo."
    elif dtp < 0:     pending_timing = f"RETRASO: {abs(dtp)} días tarde (debía llegar el {snapshot['pending_expected']})"
    elif dtp == 0:    pending_timing = "Debería llegar HOY."
    else:             pending_timing = f"Llega en ~{dtp} días (estimado {snapshot['pending_expected']})"
    pending_txt = "\n".join(f"  {p['product']}: {p['units']} uds (${p['value']:.0f})"
                            for p in snapshot["pending_orders"]) or "  Sin pedidos pendientes."
    risky = [r for r in snapshot["stock_with_pending"]
             if r["daily_vel"] > 0.05 and
             (r["stockout_before_pending"] or r["stockout_before_new_order"] or r["coverage_days"] < 42)]
    risky_txt = "\n".join(
        f"  {r['name']}: stock={r['stock_current']} pend={r['stock_pending']} vel={r['daily_vel']}/d "
        f"| cob_sin={r['coverage_now']}d cob_con={r['coverage_days']}d "
        f"| en_pedido={'SI' if r['in_pending_order'] else 'NO'} "
        f"| crítico={'SI' if r['stockout_before_pending'] else 'no'}"
        for r in risky) or "  Ningún producto en riesgo en 6 semanas."
    days_since = snapshot.get("days_since_last_order")
    cadence_txt = (f"Último pedido hace {days_since} días (política: ~30 días)."
                   if days_since is not None else "Fecha último pedido no disponible.")
    return _PROMPT + f"""

DATOS ({snapshot['analysis_date']}):

VENTAS MENSUALES:
{monthly_txt}

MTD {snapshot['mtd_days_elapsed']} días: ${snapshot['mtd_revenue']:,.0f} → Proyección: ${snapshot['mtd_projected']:,.0f}

PEDIDO PENDIENTE:
  {pending_timing}
  Lead time si pido hoy: {snapshot['lead_time_days']} días
{pending_txt}
  Total: ${sum(p['value'] for p in snapshot['pending_orders']):,.0f} | Mínimo: €{snapshot['min_order_eur']:,}

CADENCIA: {cadence_txt}

PRODUCTOS CON RIESGO O COBERTURA <42d:
{risky_txt}
"""


# ── HTML builder ──────────────────────────────────────────────────────────────

def _parse_sections(text: str) -> dict:
    labels = {
        "resumen":    r"=== RESUMEN EJECUTIVO ===",
        "ventas":     r"=== TENDENCIA DE VENTAS ===",
        "inventario": r"=== ESTADO DE INVENTARIO ===",
        "orden":      r"=== ORDEN PENDIENTE ===",
        "decision":   r"=== DECISIÓN DE COMPRA ===",
    }
    keys = list(labels.keys()); patterns = list(labels.values())
    sections = {}
    for i, (key, pat) in enumerate(zip(keys, patterns)):
        nxt = patterns[i + 1] if i + 1 < len(patterns) else None
        m = re.search(pat + r"(.*?)" + (nxt if nxt else r"$"), text, re.S | re.I)
        sections[key] = m.group(1).strip() if m else ""
    if not any(sections.values()):
        sections["resumen"] = text.strip()
    return sections


def _section(title, content, border="#3498db"):
    content = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', content).replace("\n", "<br>")
    return (f'<div style="background:#fff;border-left:4px solid {border};border-radius:8px;'
            f'padding:20px;margin-bottom:18px;box-shadow:0 2px 6px rgba(0,0,0,0.06);">'
            f'<h3 style="color:#2c3e50;margin:0 0 12px;font-size:15px;text-transform:uppercase;">{title}</h3>'
            f'<div style="color:#444;font-size:14px;line-height:1.7;">{content}</div></div>')


def _coverage_badge(days):
    if days < 14:   return f"<span style='background:#e74c3c;color:#fff;padding:2px 8px;border-radius:10px;font-size:11px;'>{days:.0f}d 🔴</span>"
    if days < 30:   return f"<span style='background:#f39c12;color:#fff;padding:2px 8px;border-radius:10px;font-size:11px;'>{days:.0f}d 🟠</span>"
    if days < 9000: return f"<span style='background:#27ae60;color:#fff;padding:2px 8px;border-radius:10px;font-size:11px;'>{days:.0f}d ✅</span>"
    return "<span style='color:#ccc;font-size:11px;'>—</span>"


def build_email_html(analysis_text: str, snapshot: dict, store_name: str = "Rodmat") -> str:
    today    = datetime.now()
    sections = _parse_sections(analysis_text)

    verdict_match = re.search(r"VEREDICTO[:\s]*([^\n]+)", analysis_text, re.I)
    raw_verdict = verdict_match.group(1).strip() if verdict_match else ""
    bold_match  = re.search(r"\*\*([^*]+)\*\*", raw_verdict)
    verdict = (bold_match.group(1).strip() if bold_match
               else re.sub(r"\[.*?\]", "", raw_verdict).strip("* ").strip() or "ESPERAR")
    vup = verdict.upper()
    v_color = "#e74c3c" if "PEDIR YA" in vup else ("#8e44ad" if "NO PEDIR" in vup else "#f39c12")

    sales_rows = "".join(
        f"<tr><td style='padding:7px 12px;border-bottom:1px solid #f0f0f0;'>{m['month']}</td>"
        f"<td style='padding:7px 12px;text-align:right;font-weight:bold;'>${m['gmv']:,.0f}</td>"
        f"<td style='padding:7px 12px;text-align:center;'>{m['orders']}</td>"
        f"<td style='padding:7px 12px;text-align:center;'>{m['units']}</td></tr>"
        for m in snapshot["monthly_sales"])

    show = sorted([r for r in snapshot["stock_with_pending"] if r["daily_vel"] > 0.05],
                  key=lambda x: x["coverage_days"] if x["coverage_days"] < 9000 else 9999)
    stock_rows = ""
    for r in show:
        pend_str = (f"<span style='color:#3498db;font-weight:bold;'>+{r['stock_pending']}</span>"
                    if r["stock_pending"] > 0 else "<span style='color:#ccc;'>—</span>")
        bg = "#fff0f0" if r.get("stockout_before_pending") else ("#fffbf0" if r.get("stockout_before_new_order") else "#fff")
        cov_now  = r.get("coverage_now", r["coverage_days"])
        cov_full = r["coverage_days"]
        cov_cell = (f"{_coverage_badge(cov_now)} → {_coverage_badge(cov_full)}"
                    if r["stock_pending"] > 0 else _coverage_badge(cov_full))
        flags = ""
        if r.get("stockout_before_pending"):    flags += "<span style='color:#e74c3c;font-size:10px;font-weight:bold;'> ⚠️sin pend</span>"
        if r.get("stockout_before_new_order"):  flags += "<span style='color:#8e44ad;font-size:10px;font-weight:bold;'> 🔴pedir</span>"
        stock_rows += (
            f"<tr style='background:{bg};'>"
            f"<td style='padding:6px 10px;font-size:12px;'>{r['name']}{flags}</td>"
            f"<td style='padding:6px 10px;text-align:center;font-size:11px;color:#888;'>{r['tipo']}</td>"
            f"<td style='padding:6px 10px;text-align:center;font-weight:bold;'>{r['stock_current']}</td>"
            f"<td style='padding:6px 10px;text-align:center;'>{pend_str}</td>"
            f"<td style='padding:6px 10px;text-align:center;font-size:11px;'>{r['daily_vel']:.1f}/d</td>"
            f"<td style='padding:6px 10px;text-align:center;'>{cov_cell}</td></tr>")

    overdue = snapshot["pending_overdue_days"]
    pend_total = sum(p["value"] for p in snapshot["pending_orders"])

    return f"""<!DOCTYPE html><html><head><meta charset="utf-8"></head>
<body style="font-family:'Segoe UI',Arial,sans-serif;max-width:860px;margin:0 auto;padding:20px;background:#f5f6fa;">
  <div style="background:linear-gradient(135deg,#0f0c29,#302b63,#24243e);color:#fff;padding:28px;border-radius:12px;margin-bottom:22px;">
    <table width="100%"><tr>
      <td><div style="font-size:10px;letter-spacing:4px;opacity:0.6;text-transform:uppercase;">{store_name} Operations</div>
        <div style="font-size:30px;font-weight:700;letter-spacing:3px;margin:4px 0;">{AGENT_NAME}</div>
        <div style="font-size:12px;opacity:0.7;">{AGENT_SUBTITLE}</div></td>
      <td style="text-align:right;vertical-align:top;">
        <div style="font-size:14px;font-weight:bold;">Weekly Inventory Report</div>
        <div style="font-size:12px;opacity:0.65;">{today.strftime('%A %d %B, %Y')}</div>
        <div style="margin-top:8px;background:{v_color};color:#fff;padding:4px 14px;border-radius:20px;font-size:13px;font-weight:bold;display:inline-block;">{verdict}</div>
      </td>
    </tr></table>
  </div>
  {_section("Resumen Ejecutivo", sections.get("resumen","—"), "#2ecc71")}
  {_section("Tendencia de Ventas", sections.get("ventas","—"), "#3498db")}
  <div style="background:#fff;border-radius:8px;padding:20px;margin-bottom:18px;box-shadow:0 2px 6px rgba(0,0,0,0.06);">
    <h3 style="color:#2c3e50;margin:0 0 12px;font-size:15px;text-transform:uppercase;">Datos de Ventas</h3>
    <table width="100%" style="border-collapse:collapse;font-size:13px;">
      <thead><tr style="background:#2c3e50;color:#fff;">
        <th style="padding:9px 12px;">Mes</th><th style="padding:9px 12px;text-align:right;">GMV</th>
        <th style="padding:9px 12px;text-align:center;">Órdenes</th><th style="padding:9px 12px;text-align:center;">Uds</th></tr></thead>
      <tbody>{sales_rows}</tbody>
    </table>
    <div style="margin-top:12px;padding:10px 14px;background:#eaf2f8;border-radius:6px;font-size:13px;">
      <strong>{today.strftime('%B')} MTD ({snapshot['mtd_days_elapsed']}d):</strong>
      ${snapshot['mtd_revenue']:,.0f} → <strong>Proy: ${snapshot['mtd_projected']:,.0f}</strong>
    </div>
  </div>
  {_section("Estado de Inventario", sections.get("inventario","—"), "#e67e22")}
  <div style="background:#fff;border-radius:8px;padding:20px;margin-bottom:18px;box-shadow:0 2px 6px rgba(0,0,0,0.06);">
    <h3 style="color:#2c3e50;margin:0 0 6px;font-size:15px;text-transform:uppercase;">Stock — Productos Activos</h3>
    <table width="100%" style="border-collapse:collapse;font-size:12px;">
      <thead><tr style="background:#34495e;color:#fff;">
        <th style="padding:8px 10px;text-align:left;">Producto</th><th style="padding:8px 10px;text-align:center;">Tipo</th>
        <th style="padding:8px 10px;text-align:center;">Stock</th><th style="padding:8px 10px;text-align:center;color:#85c1e9;">+Pend.</th>
        <th style="padding:8px 10px;text-align:center;">Vel./día</th><th style="padding:8px 10px;text-align:center;">Cobertura</th></tr></thead>
      <tbody>{stock_rows}</tbody>
    </table>
  </div>
  {_section("Orden Pendiente" + (f" — ⚠️ {overdue}d retraso" if overdue > 0 else ""),
            sections.get("orden","—") + f"<br><br>💰 Valor total: <strong>${pend_total:,.0f}</strong>",
            "#e74c3c" if overdue > 0 else "#27ae60")}
  {_section("Decisión de Compra", sections.get("decision","—"), v_color)}
  <div style="text-align:center;padding:16px;color:#aaa;font-size:11px;border-top:1px solid #e8e8e8;">
    <strong style="color:#888;">{AGENT_NAME}</strong> · {store_name} · {AGENT_SUBTITLE}<br>
    {today.strftime('%Y-%m-%d %H:%M')}
  </div>
</body></html>"""


# ── Entry point ───────────────────────────────────────────────────────────────

def run(db: Session, store_id: str, force: bool = False) -> bool:
    from app.models.store import Store
    today = datetime.now()
    if not force and today.weekday() != 2:
        return False
    store = db.query(Store).filter(Store.id == store_id).first()
    recipients = get_recipients(store)
    if not recipients:
        print(f"[HAIKU] No recipients for store {store_id}")
        return False
    store_name = store.name if store else "Store"

    print(f"[HAIKU] Extracting snapshot for {store_name}...")
    snapshot = extract_snapshot(db, store_id)
    overdue  = snapshot["pending_overdue_days"]
    print(f"[HAIKU] {len(snapshot['stock_with_pending'])} products · {len(snapshot['pending_orders'])} pending SKUs · {overdue}d overdue")

    print("[HAIKU] Calling Groq...")
    analysis = call_groq(_build_prompt(snapshot))

    html    = build_email_html(analysis, snapshot, store_name)
    subject = f"HAIKU · {snapshot['analysis_date']} · {store_name} · Inventario"
    if overdue > 0:
        subject += f" · ⚠️ Orden {overdue}d retraso"
    ok = send_email(html, subject, recipients)
    print(f"[HAIKU] Email {'sent' if ok else 'FAILED'}")
    return ok
