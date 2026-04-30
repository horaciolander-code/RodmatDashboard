"""
PRISM — Product Research & Intelligence for Strategic Markets (V2)
Data source: Neon PostgreSQL via SQLAlchemy (store-aware, multi-tenant).
Logic identical to V1; only data loading layer changed.
"""
import re
from datetime import datetime

import pandas as pd
from sqlalchemy.orm import Session

from app.services.agents._base import (
    call_groq, send_email, get_recipients,
    load_orders_df, load_kpis, load_creator_df,
)

AGENT_NAME = "PRISM"
AGENT_FULL = "Product Research & Intelligence for Strategic Markets"

# ── Data loading ──────────────────────────────────────────────────────────────

def _shipped(orders_df: pd.DataFrame) -> pd.DataFrame:
    return orders_df[
        orders_df["Order Status"].isin(["Shipped", "Completed", "Delivered"]) |
        orders_df["Shipped Time"].notna()
    ].copy()


# ── 7 Intelligence modules (identical to V1) ─────────────────────────────────

def module_velocity_shifts(kpis: pd.DataFrame) -> dict:
    rows = []
    for _, r in kpis[kpis["AvgVentas30d"] > 0.1].iterrows():
        v30 = r["AvgVentas30d"]
        v60 = r.get("AvgVentas60d", 0)
        v7  = r.get("Sales_7d", 0) / 7 if r.get("Sales_7d", 0) > 0 else 0
        momentum_7_30  = (v7 - v30) / v30 if v30 > 0 else 0
        momentum_30_60 = (v30 - v60) / v60 if v60 > 0.05 else 0
        if v60 < 0.05:          trend = "NEW"
        elif momentum_7_30 > 0.25: trend = "ACCELERATING"
        elif momentum_7_30 < -0.25: trend = "DECELERATING"
        else:                    trend = "STABLE"
        rows.append({
            "name": r["ProductoNombre"], "tipo": r["Tipo"],
            "vel_7d": round(v7, 2), "vel_30d": round(v30, 2), "vel_60d": round(v60, 2),
            "momentum_7_30": round(momentum_7_30 * 100, 1),
            "momentum_30_60": round(momentum_30_60 * 100, 1),
            "trend": trend,
            "sell_through": round(r.get("SellThroughRate", 0), 1),
            "price": round(r.get("PRECIO", 0), 2),
            "days_coverage": round(r.get("Days_Coverage", 0), 0),
        })
    rows.sort(key=lambda x: x["vel_30d"], reverse=True)
    return {
        "products":      rows,
        "accelerating":  [r for r in rows if r["trend"] == "ACCELERATING"],
        "decelerating":  [r for r in rows if r["trend"] == "DECELERATING"],
        "new":           [r for r in rows if r["trend"] == "NEW"],
    }


def module_category_mix(shipped: pd.DataFrame) -> dict:
    shipped = shipped.copy()
    shipped["Month"] = shipped["Order_Date"].dt.to_period("M")
    cutoff = pd.Timestamp.now() - pd.DateOffset(months=5)
    recent = shipped[shipped["Order_Date"] >= cutoff]
    cat_monthly = (
        recent.groupby(["Month", "Product Category"])["SKU Subtotal After Discount"]
        .sum().unstack(fill_value=0)
    )
    cat_share = cat_monthly.div(cat_monthly.sum(axis=1), axis=0) * 100
    if len(cat_share) >= 2:
        change = (cat_share.iloc[-1] - cat_share.iloc[-2]).sort_values(ascending=False)
    else:
        change = pd.Series(dtype=float)
    months_list = [
        {"month": str(p), "total_gmv": round(row.sum(), 0),
         "breakdown": {c: round(v, 0) for c, v in row.items() if v > 0}}
        for p, row in cat_monthly.iterrows()
    ]
    return {
        "months":       months_list,
        "growing":      [{"cat": c, "share_delta": round(v, 1)} for c, v in change.items() if v > 1.5],
        "declining":    [{"cat": c, "share_delta": round(v, 1)} for c, v in change.items() if v < -1.5],
        "top_category": str(cat_monthly.sum().idxmax()) if not cat_monthly.empty else "—",
    }


def module_geographic_demand(shipped: pd.DataFrame) -> dict:
    shipped = shipped.copy()
    today = pd.Timestamp.now()
    cutoff = today - pd.DateOffset(months=3)
    recent = shipped[shipped["Order_Date"] >= cutoff]
    state_gmv = recent.groupby("State")["SKU Subtotal After Discount"].sum().sort_values(ascending=False)
    total_gmv = state_gmv.sum()
    top10 = [{"state": s, "gmv": round(v, 0), "share_pct": round(v / total_gmv * 100, 1)}
             for s, v in state_gmv.head(10).items()]
    top5_share = state_gmv.head(5).sum() / total_gmv * 100 if total_gmv > 0 else 0
    last_30 = shipped[shipped["Order_Date"] >= today - pd.DateOffset(days=30)]
    prev_30 = shipped[(shipped["Order_Date"] >= today - pd.DateOffset(days=60)) &
                      (shipped["Order_Date"] < today - pd.DateOffset(days=30))]
    emerging_list = []
    if not last_30.empty and not prev_30.empty:
        share_now  = last_30.groupby("State")["SKU Subtotal After Discount"].sum() / last_30["SKU Subtotal After Discount"].sum()
        share_prev = prev_30.groupby("State")["SKU Subtotal After Discount"].sum() / prev_30["SKU Subtotal After Discount"].sum()
        combined   = pd.DataFrame({"now": share_now, "prev": share_prev}).fillna(0)
        combined["delta"] = combined["now"] - combined["prev"]
        for s, v in combined[combined["delta"] > 0.01].sort_values("delta", ascending=False).head(5).iterrows():
            emerging_list.append({"state": s, "delta_pct": round(v["delta"] * 100, 1)})
    return {
        "top10": top10,
        "top5_concentration_pct": round(top5_share, 1),
        "emerging_states": emerging_list,
        "total_states_active": int((state_gmv > 10).sum()),
    }


def module_portfolio_matrix(kpis: pd.DataFrame) -> dict:
    active = kpis[kpis["AvgVentas30d"] > 0.1].copy()
    if active.empty:
        return {"star": [], "grower": [], "cash": [], "dog": []}
    vel_med = active["AvgVentas30d"].median()
    st_med  = active["SellThroughRate"].median()
    def classify(r):
        hv = r["AvgVentas30d"] >= vel_med
        hs = r["SellThroughRate"] >= st_med
        if hv and hs: return "STAR"
        if hv:        return "GROWER"
        if hs:        return "CASH"
        return "DOG"
    active["quadrant"] = active.apply(classify, axis=1)
    result = {}
    for q in ["STAR", "GROWER", "CASH", "DOG"]:
        result[q.lower()] = [
            {"name": r["ProductoNombre"], "tipo": r["Tipo"],
             "vel_30d": round(r["AvgVentas30d"], 2),
             "sell_through": round(r["SellThroughRate"], 1),
             "days_coverage": round(r.get("Days_Coverage", 0), 0)}
            for _, r in active[active["quadrant"] == q].iterrows()
        ]
    result["vel_median"] = round(float(vel_med), 2)
    result["st_median"]  = round(float(st_med), 1)
    return result


def module_creator_impact(creator_df: pd.DataFrame, shipped: pd.DataFrame) -> dict:
    if creator_df is None or creator_df.empty:
        return {"creator_gmv_pct": 0, "creator_gmv_usd": 0, "top_products": [], "top_creators": []}
    active = creator_df[creator_df["Order Status"].isin(["COMPLETED","Completed","SETTLED","TO_SETTLE","VALID"])]
    creator_gmv = active["Payment Amount"].sum() if "Payment Amount" in active.columns else 0
    total_gmv   = shipped["SKU Subtotal After Discount"].sum()
    pct = round(creator_gmv / total_gmv * 100, 1) if total_gmv > 0 else 0
    top_products = []
    if "Product Name" in active.columns:
        tp = active.groupby("Product Name")["Payment Amount"].sum().sort_values(ascending=False).head(5)
        top_products = [{"product": p, "creator_gmv": round(v, 0)} for p, v in tp.items()]
    top_creators = []
    if "Creator Username" in active.columns:
        tc = active.groupby("Creator Username")["Payment Amount"].sum().sort_values(ascending=False).head(5)
        top_creators = [{"creator": c, "gmv": round(v, 0)} for c, v in tc.items()]
    return {"creator_gmv_pct": pct, "creator_gmv_usd": round(creator_gmv, 0),
            "top_products": top_products, "top_creators": top_creators}


def module_price_bands(kpis: pd.DataFrame, shipped: pd.DataFrame) -> dict:
    price_map = kpis.set_index("ProductoNombre")["PRECIO"].to_dict()
    def band(price):
        if price < 3: return "Budget (<$3)"
        if price <= 7: return "Mid ($3-$7)"
        return "Premium (>$7)"
    shipped2 = shipped.copy()
    if "Product Name" in shipped2.columns:
        shipped2["Band"] = shipped2["Product Name"].map(lambda n: band(price_map.get(str(n), 5)))
    else:
        shipped2["Band"] = "Mid ($3-$7)"
    band_gmv  = shipped2.groupby("Band")["SKU Subtotal After Discount"].sum().sort_values(ascending=False)
    total     = band_gmv.sum()
    last_30   = shipped2[shipped2["Order_Date"] >= pd.Timestamp.now() - pd.DateOffset(days=30)]
    band_units = last_30.groupby("Band")["Quantity"].sum() / 30
    return {"bands": [{"band": b, "gmv": round(v, 0),
                       "share_pct": round(v / total * 100, 1) if total > 0 else 0,
                       "units_per_day": round(float(band_units.get(b, 0)), 1)}
                      for b, v in band_gmv.items()]}


def module_opportunity_signals(velocity, category, portfolio, geographic, creator) -> list:
    signals = []
    for p in portfolio.get("grower", []):
        if p["days_coverage"] < 30:
            signals.append({"type": "SUPPLY GAP", "score": "HIGH", "product": p["name"],
                "detail": f"High velocity ({p['vel_30d']}/day) + low sell-through + {p['days_coverage']}d stock.",
                "action": "Increase order quantity in next PO."})
    for p in velocity.get("accelerating", []):
        signals.append({"type": "MOMENTUM", "score": "HIGH", "product": p["name"],
            "detail": f"7d velocity +{p['momentum_7_30']}% vs 30d avg. Current: {p['vel_7d']}/day.",
            "action": "Ensure adequate stock. Feature in next promotion."})
    for c in category.get("growing", []):
        signals.append({"type": "CATEGORY GROWTH", "score": "MEDIUM", "product": c["cat"],
            "detail": f"Category share +{c['share_delta']}pp month-over-month.",
            "action": "Expand SKU depth in this category."})
    for p in velocity.get("decelerating", []):
        if p["vel_30d"] > 1.0:
            signals.append({"type": "DECELERATION", "score": "MEDIUM", "product": p["name"],
                "detail": f"7d velocity {p['momentum_7_30']}% vs 30d. Possible saturation.",
                "action": "Check stock-out. Review price or content strategy."})
    if geographic.get("top5_concentration_pct", 0) > 60:
        signals.append({"type": "GEO CONCENTRATION", "score": "LOW", "product": "Portfolio",
            "detail": f"Top 5 states = {geographic['top5_concentration_pct']}% of GMV.",
            "action": "Target emerging states with creator content: " +
                      ", ".join(s["state"] for s in geographic.get("emerging_states", [])[:3])})
    for s in geographic.get("emerging_states", [])[:3]:
        signals.append({"type": "GEO OPPORTUNITY", "score": "LOW", "product": s["state"],
            "detail": f"Share grew +{s['delta_pct']}pp in last 30 days.",
            "action": f"Activate creator content targeting {s['state']} audience."})
    pct = creator.get("creator_gmv_pct", 0)
    if pct < 15:
        signals.append({"type": "CREATOR CHANNEL", "score": "MEDIUM", "product": "Affiliate Program",
            "detail": f"Creator GMV = {pct}% of total. Below 20-30% benchmark.",
            "action": "Recruit 5-10 new micro-creators for top 3 SKUs."})
    elif pct > 40:
        signals.append({"type": "CREATOR DEPENDENCY", "score": "LOW", "product": "Affiliate Program",
            "detail": f"Creator GMV = {pct}% of total. High dependency on affiliates.",
            "action": "Invest in organic search and product listing optimization."})
    signals.sort(key=lambda x: {"HIGH": 0, "MEDIUM": 1, "LOW": 2}.get(x["score"], 3))
    return signals


# ── Snapshot ──────────────────────────────────────────────────────────────────

def extract_snapshot(db: Session, store_id: str) -> dict:
    kpis       = load_kpis(db, store_id)
    orders_df  = load_orders_df(db, store_id)
    creator_df = load_creator_df(db, store_id)
    today      = pd.Timestamp.now()
    shipped    = _shipped(orders_df)

    vel  = module_velocity_shifts(kpis)
    cat  = module_category_mix(shipped)
    geo  = module_geographic_demand(shipped)
    port = module_portfolio_matrix(kpis)
    cre  = module_creator_impact(creator_df, shipped)
    band = module_price_bands(kpis, shipped)
    opp  = module_opportunity_signals(vel, cat, port, geo, cre)

    mtd_start = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    mtd = orders_df[
        (orders_df["Order_Date"] >= mtd_start) &
        orders_df["Order Status"].isin(["Shipped", "Completed", "Delivered", "To ship"])
    ]
    rev_mtd   = round(mtd["SKU Subtotal After Discount"].sum(), 0)
    days_in   = today.day
    projected = round(rev_mtd / days_in * 30, 0) if days_in > 0 else 0

    cutoff = today - pd.DateOffset(months=6)
    shipped2 = shipped.copy()
    shipped2["Month"] = shipped2["Order_Date"].dt.to_period("M")
    monthly_agg = (shipped2[shipped2["Order_Date"] >= cutoff]
                   .groupby("Month")
                   .agg(GMV=("SKU Subtotal After Discount", "sum"), Orders=("Order ID", "nunique"))
                   .reset_index())
    monthly_list = [{"month": str(r["Month"]), "gmv": round(r["GMV"], 0), "orders": int(r["Orders"])}
                    for _, r in monthly_agg.iterrows()]

    return {
        "analysis_date":    today.strftime("%Y-%m-%d"),
        "analysis_weekday": today.strftime("%A"),
        "monthly_sales":    monthly_list,
        "mtd_revenue":      rev_mtd,
        "mtd_projected":    projected,
        "mtd_days_elapsed": days_in,
        "velocity":         vel,
        "categories":       cat,
        "geography":        geo,
        "portfolio":        port,
        "creator":          cre,
        "price_bands":      band,
        "opportunities":    opp,
        "total_active_skus": int((kpis["AvgVentas30d"] > 0.1).sum()),
    }


# ── Groq prompt ───────────────────────────────────────────────────────────────

_PROMPT = """\
Eres PRISM, analista senior de inteligencia de mercado para Rodmat.
Rodmat vende fragancias Avon en TikTok Shop (EE.UU.) — mercado altamente emocional y visual.

PRINCIPIOS DE ANÁLISIS:
1. Las tendencias internas (velocidad, sell-through, geografía) SON el mercado — datos reales, no predicciones.
2. Una aceleración de velocidad en 7d vs 30d = señal de demanda emergente AHORA.
3. Products GROWER (alta vel + bajo sell-through) = demanda reprimida por falta de stock.
4. Las categorías con share creciente indican dónde está el mercado yendo.
5. La concentración geográfica en >60% top-5 estados es un riesgo de plataforma y logístico.
6. El canal de creadores debe representar 20-30% del GMV; fuera de ese rango, acción.

Produce un análisis de inteligencia de mercado en ESPAÑOL con exactamente estas 4 secciones:

=== TENDENCIAS DE MERCADO ===
(¿Qué está creciendo? ¿Qué está cayendo? Patrón general del negocio esta semana. Máximo 5 frases.)

=== OPORTUNIDADES DE PRODUCTO ===
(Basado en los signals: ¿qué productos o categorías tienen potencial sin explotar? Sé específico. Máximo 5 frases.)

=== SEÑALES DE RIESGO ===
(Productos en deceleración, saturación, concentración geográfica, dependencia de canal. Máximo 4 frases.)

=== RECOMENDACIONES ESTRATÉGICAS ===
Lista numerada de 4-6 acciones concretas: QUÉ hacer, POR QUÉ (dato), CUÁNDO.
"""


def _build_prompt(snapshot: dict) -> str:
    vel = snapshot["velocity"]; cat = snapshot["categories"]
    geo = snapshot["geography"]; port = snapshot["portfolio"]
    cre = snapshot["creator"];   opp = snapshot["opportunities"]
    bands = snapshot["price_bands"]

    monthly_txt = "\n".join(f"  {m['month']}: GMV=${m['gmv']:,.0f} ({m['orders']} ordenes)"
                            for m in snapshot["monthly_sales"])
    acc_txt = "\n".join(f"  {p['name']}: vel 7d={p['vel_7d']}/d (+{p['momentum_7_30']}% vs 30d)"
                        for p in vel.get("accelerating", [])) or "  Ninguno."
    dec_txt = "\n".join(f"  {p['name']}: vel 7d={p['vel_7d']}/d ({p['momentum_7_30']}% vs 30d)"
                        for p in vel.get("decelerating", [])) or "  Ninguno."
    star_txt  = ", ".join(p["name"] for p in port.get("star",  [])[:5]) or "—"
    grow_txt  = ", ".join(f"{p['name']} ({p['days_coverage']}d)" for p in port.get("grower", [])[:5]) or "—"
    cat_grow  = ", ".join(f"{c['cat']} (+{c['share_delta']}pp)" for c in cat.get("growing",  [])[:4]) or "ninguna"
    cat_dec   = ", ".join(f"{c['cat']} ({c['share_delta']}pp)"  for c in cat.get("declining",[])[:4]) or "ninguna"
    geo_top5  = ", ".join(f"{g['state']} ({g['share_pct']}%)" for g in geo["top10"][:5])
    geo_emerg = ", ".join(f"{g['state']} (+{g['delta_pct']}pp)" for g in geo.get("emerging_states",[])[:4]) or "ninguno"
    band_txt  = "\n".join(f"  {b['band']}: ${b['gmv']:,.0f} ({b['share_pct']}% GMV, {b['units_per_day']}/d)"
                          for b in bands.get("bands", []))
    opp_txt   = "\n".join(f"  [{o['score']}] {o['type']} — {o['product']}: {o['detail']}"
                          for o in opp[:8]) or "  Sin signals."

    return _PROMPT + f"""

DATOS DE ESTA SEMANA ({snapshot['analysis_date']}):

VENTAS MENSUALES:
{monthly_txt}
MTD {snapshot['mtd_days_elapsed']}d: ${snapshot['mtd_revenue']:,.0f} | Proyeccion: ${snapshot['mtd_projected']:,.0f}

PRODUCTOS ACELERANDO (7d > 30d):
{acc_txt}

PRODUCTOS DESACELERANDO (7d < 30d):
{dec_txt}

CUADRANTE PORTAFOLIO:
  STARS (alto vel + alto sell-through): {star_txt}
  GROWERS (demanda reprimida): {grow_txt}

TENDENCIAS POR CATEGORIA:
  Creciendo: {cat_grow}
  Cayendo:   {cat_dec}

BANDAS DE PRECIO:
{band_txt}

GEOGRAFIA (top 5 estados, {geo['top5_concentration_pct']}% concentracion):
  Top: {geo_top5}
  Emergentes (30d): {geo_emerg}

CANAL CREADORES: {cre['creator_gmv_pct']}% del GMV total = ${cre['creator_gmv_usd']:,.0f}

SIGNALS DE OPORTUNIDAD:
{opp_txt}
"""


# ── HTML builder (identical to V1) ────────────────────────────────────────────

def _parse_sections(text: str) -> dict:
    labels = {
        "tendencias":    r"=== TENDENCIAS DE MERCADO ===",
        "oportunidades": r"=== OPORTUNIDADES DE PRODUCTO ===",
        "riesgos":       r"=== SEÑALES DE RIESGO ===",
        "acciones":      r"=== RECOMENDACIONES ESTRATÉGICAS ===",
    }
    keys = list(labels.keys()); patterns = list(labels.values())
    sections = {}
    for i, (key, pat) in enumerate(zip(keys, patterns)):
        nxt = patterns[i + 1] if i + 1 < len(patterns) else None
        m = re.search(pat + r"(.*?)" + (nxt if nxt else r"$"), text, re.S | re.I)
        sections[key] = m.group(1).strip() if m else ""
    if not any(sections.values()):
        sections["tendencias"] = text.strip()
    return sections


def _card(title, content, border="#3498db", bg="#fff"):
    content = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', content).replace("\n", "<br>")
    return (f'<div style="background:{bg};border-left:4px solid {border};border-radius:8px;'
            f'padding:20px;margin-bottom:16px;box-shadow:0 2px 6px rgba(0,0,0,0.06);">'
            f'<h3 style="color:#2c3e50;margin:0 0 12px;font-size:14px;text-transform:uppercase;'
            f'letter-spacing:1.5px;">{title}</h3>'
            f'<div style="color:#444;font-size:13px;line-height:1.8;">{content}</div></div>')


def _score_badge(score):
    bg = {"HIGH": "#e74c3c", "MEDIUM": "#f39c12", "LOW": "#27ae60"}.get(score, "#95a5a6")
    return f"<span style='background:{bg};color:#fff;padding:2px 7px;border-radius:8px;font-size:10px;font-weight:bold;'>{score}</span>"


def build_email_html(analysis_text: str, snapshot: dict, store_name: str = "Rodmat") -> str:
    today    = datetime.now()
    sections = _parse_sections(analysis_text)
    vel = snapshot["velocity"]; geo = snapshot["geography"]
    port = snapshot["portfolio"]; opp = snapshot["opportunities"]
    cat = snapshot["categories"]; cre = snapshot["creator"]

    sales_rows = "".join(
        f"<tr><td style='padding:6px 12px;border-bottom:1px solid #f0f0f0;'>{m['month']}</td>"
        f"<td style='padding:6px 12px;border-bottom:1px solid #f0f0f0;text-align:right;font-weight:bold;'>${m['gmv']:,.0f}</td>"
        f"<td style='padding:6px 12px;border-bottom:1px solid #f0f0f0;text-align:center;'>{m['orders']}</td></tr>"
        for m in snapshot["monthly_sales"])

    trend_icon = {"ACCELERATING": "🚀", "DECELERATING": "📉", "STABLE": "➡️", "NEW": "🆕"}
    vel_rows = ""
    for p in vel["products"][:15]:
        mc = "#27ae60" if p["momentum_7_30"] > 0 else "#e74c3c"
        vel_rows += (f"<tr><td style='padding:5px 8px;border-bottom:1px solid #f5f5f5;font-size:11px;'>"
                     f"{trend_icon.get(p['trend'],'')} {p['name']}</td>"
                     f"<td style='padding:5px 8px;font-size:10px;color:#888;text-align:center;'>{p['tipo']}</td>"
                     f"<td style='padding:5px 8px;text-align:center;font-weight:bold;'>{p['vel_30d']}</td>"
                     f"<td style='padding:5px 8px;text-align:center;color:{mc};font-weight:bold;'>{p['momentum_7_30']:+.0f}%</td>"
                     f"<td style='padding:5px 8px;text-align:center;'>{p['sell_through']:.0f}%</td>"
                     f"<td style='padding:5px 8px;text-align:center;'>${p['price']:.2f}</td></tr>")

    opp_rows = "".join(
        f"<tr><td style='padding:6px 10px;border-bottom:1px solid #f5f5f5;'>{_score_badge(o['score'])}</td>"
        f"<td style='padding:6px 10px;font-weight:bold;font-size:11px;'>{o['type']}</td>"
        f"<td style='padding:6px 10px;font-size:11px;'>{o['product']}</td>"
        f"<td style='padding:6px 10px;font-size:11px;color:#555;'>{o['detail'][:90]}...</td>"
        f"<td style='padding:6px 10px;font-size:10px;color:#3498db;'>{o['action'][:70]}...</td></tr>"
        for o in opp)

    geo_rows = "".join(
        f"<tr><td style='padding:5px 10px;font-size:11px;'>{g['state']}</td>"
        f"<td style='padding:5px 10px;text-align:right;font-weight:bold;font-size:11px;'>${g['gmv']:,.0f}</td>"
        f"<td style='padding:5px 10px;text-align:center;font-size:11px;'>{g['share_pct']}%</td></tr>"
        for g in geo["top10"][:8])

    def _plist(items, limit=4):
        return "<br>".join(f"<span style='font-size:11px;'>• {p['name']} ({p['vel_30d']}/d)</span>"
                           for p in items[:limit]) or "<span style='color:#ccc;font-size:11px;'>—</span>"

    high_count = sum(1 for o in opp if o["score"] == "HIGH")

    return f"""<!DOCTYPE html><html><head><meta charset="utf-8"></head>
<body style="font-family:'Segoe UI',Arial,sans-serif;max-width:900px;margin:0 auto;padding:20px;background:#f0f2f5;">
  <div style="background:linear-gradient(135deg,#1a1a2e,#16213e,#0f3460);color:#fff;padding:28px 32px;border-radius:12px;margin-bottom:20px;">
    <table width="100%"><tr>
      <td><div style="font-size:10px;letter-spacing:4px;opacity:0.55;text-transform:uppercase;margin-bottom:4px;">{store_name} · Market Intelligence</div>
        <div style="font-size:32px;font-weight:800;letter-spacing:4px;color:#e94560;">PRISM</div>
        <div style="font-size:11px;opacity:0.6;">{AGENT_FULL}</div></td>
      <td style="text-align:right;vertical-align:top;">
        <div style="font-size:13px;font-weight:bold;">Weekly Market Report</div>
        <div style="font-size:11px;opacity:0.6;">{today.strftime('%A %d %B, %Y')}</div>
        <div style="margin-top:10px;"><span style="background:#e94560;color:#fff;padding:5px 14px;border-radius:20px;font-size:12px;font-weight:bold;">{high_count} HIGH-PRIORITY SIGNALS</span></div>
      </td>
    </tr></table>
  </div>
  <table width="100%" cellpadding="0" cellspacing="8" style="margin-bottom:20px;"><tr>
    <td width="25%" style="background:#fff;border-radius:8px;padding:14px 16px;text-align:center;box-shadow:0 2px 6px rgba(0,0,0,0.05);">
      <div style="font-size:11px;color:#999;text-transform:uppercase;">MTD Revenue</div>
      <div style="font-size:22px;font-weight:800;color:#2c3e50;">${snapshot['mtd_revenue']:,.0f}</div>
      <div style="font-size:10px;color:#aaa;">Proy: ${snapshot['mtd_projected']:,.0f}</div></td>
    <td width="25%" style="background:#fff;border-radius:8px;padding:14px 16px;text-align:center;box-shadow:0 2px 6px rgba(0,0,0,0.05);">
      <div style="font-size:11px;color:#999;text-transform:uppercase;">SKUs Activos</div>
      <div style="font-size:22px;font-weight:800;color:#2c3e50;">{snapshot['total_active_skus']}</div>
      <div style="font-size:10px;color:#aaa;">{len(vel.get('accelerating',[]))} acelerando</div></td>
    <td width="25%" style="background:#fff;border-radius:8px;padding:14px 16px;text-align:center;box-shadow:0 2px 6px rgba(0,0,0,0.05);">
      <div style="font-size:11px;color:#999;text-transform:uppercase;">Creator GMV</div>
      <div style="font-size:22px;font-weight:800;color:#2c3e50;">{cre['creator_gmv_pct']}%</div>
      <div style="font-size:10px;color:#aaa;">${cre['creator_gmv_usd']:,.0f}</div></td>
    <td width="25%" style="background:#fff;border-radius:8px;padding:14px 16px;text-align:center;box-shadow:0 2px 6px rgba(0,0,0,0.05);">
      <div style="font-size:11px;color:#999;text-transform:uppercase;">Geo Spread</div>
      <div style="font-size:22px;font-weight:800;color:#2c3e50;">{geo['total_states_active']}</div>
      <div style="font-size:10px;color:#aaa;">estados activos</div></td>
  </tr></table>
  {_card("Tendencias de Mercado", sections.get("tendencias","—"), "#3498db")}
  {_card("Oportunidades de Producto", sections.get("oportunidades","—"), "#27ae60")}
  {_card("Senales de Riesgo", sections.get("riesgos","—"), "#e74c3c", "#fffafa")}
  {_card("Recomendaciones Estrategicas", sections.get("acciones","—"), "#9b59b6")}
  <div style="background:#fff;border-radius:8px;padding:20px;margin-bottom:16px;box-shadow:0 2px 6px rgba(0,0,0,0.06);">
    <h3 style="color:#2c3e50;margin:0 0 14px;font-size:14px;text-transform:uppercase;">Opportunity Signals ({len(opp)})</h3>
    <table width="100%" style="border-collapse:collapse;font-size:12px;">
      <thead><tr style="background:#2c3e50;color:#fff;">
        <th style="padding:8px 10px;width:60px;">Score</th><th style="padding:8px 10px;text-align:left;">Tipo</th>
        <th style="padding:8px 10px;text-align:left;">Producto</th><th style="padding:8px 10px;text-align:left;">Detalle</th>
        <th style="padding:8px 10px;text-align:left;">Accion</th></tr></thead>
      <tbody>{opp_rows}</tbody>
    </table>
  </div>
  <div style="background:#fff;border-radius:8px;padding:20px;margin-bottom:16px;box-shadow:0 2px 6px rgba(0,0,0,0.06);">
    <h3 style="color:#2c3e50;margin:0 0 14px;font-size:14px;text-transform:uppercase;">Velocidad de Productos — Top 15</h3>
    <table width="100%" style="border-collapse:collapse;font-size:12px;">
      <thead><tr style="background:#34495e;color:#fff;">
        <th style="padding:7px 8px;text-align:left;">Producto</th><th style="padding:7px 8px;text-align:center;">Tipo</th>
        <th style="padding:7px 8px;text-align:center;">Vel 30d</th><th style="padding:7px 8px;text-align:center;">7d vs 30d</th>
        <th style="padding:7px 8px;text-align:center;">Sell-Thru</th><th style="padding:7px 8px;text-align:center;">Precio</th></tr></thead>
      <tbody>{vel_rows}</tbody>
    </table>
  </div>
  <table width="100%" cellpadding="0" cellspacing="10" style="margin-bottom:16px;"><tr>
    <td width="50%" valign="top">
      <div style="background:#fff;border-radius:8px;padding:18px;box-shadow:0 2px 6px rgba(0,0,0,0.06);">
        <h3 style="color:#2c3e50;margin:0 0 12px;font-size:13px;text-transform:uppercase;">Portfolio Matrix</h3>
        <table width="100%"><tr>
          <td style="padding:8px;background:#eafaf1;border-radius:6px;vertical-align:top;width:50%;">
            <div style="font-size:10px;font-weight:bold;color:#27ae60;">STARS</div>{_plist(port.get("star",[]))}</td>
          <td width="8px"></td>
          <td style="padding:8px;background:#fef9e7;border-radius:6px;vertical-align:top;width:50%;">
            <div style="font-size:10px;font-weight:bold;color:#f39c12;">GROWERS</div>{_plist(port.get("grower",[]))}</td>
        </tr><tr><td colspan="3" style="height:8px;"></td></tr><tr>
          <td style="padding:8px;background:#eaf2f8;border-radius:6px;vertical-align:top;">
            <div style="font-size:10px;font-weight:bold;color:#3498db;">CASH</div>{_plist(port.get("cash",[]))}</td>
          <td></td>
          <td style="padding:8px;background:#fdedec;border-radius:6px;vertical-align:top;">
            <div style="font-size:10px;font-weight:bold;color:#e74c3c;">DOGS</div>{_plist(port.get("dog",[]))}</td>
        </tr></table>
      </div>
    </td>
    <td width="50%" valign="top">
      <div style="background:#fff;border-radius:8px;padding:18px;box-shadow:0 2px 6px rgba(0,0,0,0.06);">
        <h3 style="color:#2c3e50;margin:0 0 12px;font-size:13px;text-transform:uppercase;">Top 8 Estados — GMV</h3>
        <table width="100%" style="border-collapse:collapse;">
          <thead><tr style="background:#ecf0f1;">
            <th style="padding:5px 10px;font-size:10px;text-align:left;">Estado</th>
            <th style="padding:5px 10px;font-size:10px;text-align:right;">GMV</th>
            <th style="padding:5px 10px;font-size:10px;text-align:center;">%</th></tr></thead>
          <tbody>{geo_rows}</tbody>
        </table>
        <div style="margin-top:10px;font-size:10px;color:#888;">
          Concentracion top-5: <strong>{geo['top5_concentration_pct']}%</strong>&nbsp;|&nbsp;
          Emergentes: {', '.join(s['state'] for s in geo.get('emerging_states',[])[:3]) or '—'}
        </div>
      </div>
    </td>
  </tr></table>
  <div style="background:#fff;border-radius:8px;padding:18px;margin-bottom:16px;box-shadow:0 2px 6px rgba(0,0,0,0.06);">
    <h3 style="color:#2c3e50;margin:0 0 12px;font-size:13px;text-transform:uppercase;">Ventas Mensuales</h3>
    <table width="100%" style="border-collapse:collapse;font-size:12px;">
      <thead><tr style="background:#2c3e50;color:#fff;">
        <th style="padding:7px 12px;text-align:left;">Mes</th><th style="padding:7px 12px;text-align:right;">GMV</th>
        <th style="padding:7px 12px;text-align:center;">Ordenes</th></tr></thead>
      <tbody>{sales_rows}</tbody>
    </table>
  </div>
  <div style="text-align:center;padding:14px;color:#aaa;font-size:10px;border-top:1px solid #e0e0e0;">
    <strong style="color:#e94560;">PRISM</strong> &nbsp;·&nbsp; {store_name} &nbsp;·&nbsp; {AGENT_FULL}<br>
    {today.strftime('%Y-%m-%d %H:%M')}
  </div>
</body></html>"""


# ── Entry point ───────────────────────────────────────────────────────────────

def run(db: Session, store_id: str, force: bool = False) -> bool:
    from app.models.store import Store
    today = datetime.now()
    if not force and today.weekday() != 0:
        return False
    store = db.query(Store).filter(Store.id == store_id).first()
    recipients = get_recipients(store)
    if not recipients:
        print(f"[PRISM] No recipients for store {store_id}")
        return False
    store_name = store.name if store else "Store"

    print(f"[PRISM] Extracting snapshot for {store_name}...")
    snapshot = extract_snapshot(db, store_id)
    high_n = sum(1 for o in snapshot["opportunities"] if o["score"] == "HIGH")
    print(f"[PRISM] {len(snapshot['opportunities'])} signals ({high_n} HIGH) · "
          f"{len(snapshot['velocity'].get('accelerating',[]))} accelerating")

    print("[PRISM] Calling Groq...")
    analysis = call_groq(_build_prompt(snapshot))
    print(f"[PRISM] Response: {len(analysis)} chars")

    html    = build_email_html(analysis, snapshot, store_name)
    subject = (f"PRISM · {snapshot['analysis_date']} · {store_name} · "
               f"{high_n} HIGH signals · "
               f"{len(snapshot['velocity'].get('accelerating',[]))} SKUs acelerando")
    ok = send_email(html, subject, recipients)
    print(f"[PRISM] Email {'sent' if ok else 'FAILED'} -> {recipients}")
    return ok
