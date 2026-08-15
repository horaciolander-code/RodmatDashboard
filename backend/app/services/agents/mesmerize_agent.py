"""
MESMERIZE — Fragrance Market Intelligence & Catalog Opportunity Agent (V2)
First Monday of each month. Market knowledge base is hardcoded (update manually).
"""
import re
from datetime import datetime

import pandas as pd
from sqlalchemy.orm import Session

from app.services.agents._base import (
    call_groq, send_email, get_recipients, is_agent_enabled,
    load_orders_df, load_kpis, load_creator_df,
)

AGENT_NAME     = "MESMERIZE"
AGENT_SUBTITLE = "Fragrance Market Intelligence"

# ── Market knowledge base (update manually when research changes) ─────────────
MARKET_INTEL = {
    "tiktok_shop_market": {
        "total_fragrance_gmv_2025": 162_000_000,
        "top_brands_by_revenue": [
            {"brand": "Lattafa",          "monthly_gmv": 4_900_000, "price_range": "$20-90",   "tier": "volume",   "notes": "Arab fragrance #1, Yara/Asad viral"},
            {"brand": "Oakcha",           "monthly_gmv": 1_400_000, "price_range": "$55-110",  "tier": "mid",      "notes": "Dupe brand, 125% growth 2024→2025"},
            {"brand": "Phlur",            "monthly_gmv": 1_200_000, "price_range": "$88-120",  "tier": "premium",  "notes": "3800+ creators, Sephora brand"},
            {"brand": "Armaf",            "monthly_gmv": 1_100_000, "price_range": "$45-85",   "tier": "mid",      "notes": "Club de Nuit = Creed dupe, viral"},
            {"brand": "Sol de Janeiro",   "monthly_gmv":   900_000, "price_range": "$38-80",   "tier": "mid",      "notes": "850M TikTok views, $1B brand"},
            {"brand": "Maison Margiela",  "monthly_gmv":   600_000, "price_range": "$75-160",  "tier": "premium",  "notes": "Replica line viral"},
            {"brand": "Valentino",        "monthly_gmv":   500_000, "price_range": "$75-140",  "tier": "premium",  "notes": "Born in Roma domina comparativas"},
            {"brand": "Prada",            "monthly_gmv":   480_000, "price_range": "$80-150",  "tier": "premium",  "notes": "Paradoxe vs Valentino = contenido infinito"},
            {"brand": "Parfums de Marly", "monthly_gmv":   350_000, "price_range": "$120-250", "tier": "luxury",   "notes": "Delina + Pegasus, botella icónica"},
        ],
        "trending_scent_profiles": [
            "Vanilla gourmand (highest search volume)",
            "Oud & amber oriental (Lattafa/Armaf effect)",
            "Floral chypre aspiracional (Paradoxe, Delina)",
            "Fresh aquatic masculino (Luna Rossa, Club de Nuit)",
            "Musky skin scents (Phlur, Maison Margiela Replica)",
        ],
        "market_insights": [
            "76% of TikTok fragrance buyers purchase through livestream",
            "Premium fragrances +12% projected growth 2025-2026",
            "Niche perfumery is the new luxury status symbol — Gen Z driven",
            "Dupe culture is gateway: buyer tries dupe, upgrades to original",
            "Creator comparisons ('X vs Y') drive 30-40% of fragrance discovery",
        ],
    },
    "avon_position": {
        "strengths": [
            "Accessible price point ($2-8) — mass market TikTok sweet spot",
            "Wide variety: florals, orientals, fresh, gourmand",
            "Strong dupe potential for premium brands",
            "TikTok Shop FBT fulfillment = fast delivery, less friction",
        ],
        "weaknesses": [
            "Brand recognition low among Gen Z vs Lattafa, Armaf",
            "No single viral 'hero' scent yet",
            "Limited creator content vs competitors",
        ],
        "opportunity_gap": [
            "Budget oriental/oud segment: Lattafa is expensive, Avon can fill $3-6 price point",
            "Vanilla gourmand: massive demand, Avon has SKUs that match",
            "Dupe comparisons: position Avon SKUs vs $80+ niche brands",
        ],
    },
}


# ── Snapshot ──────────────────────────────────────────────────────────────────

def extract_snapshot(db: Session, store_id: str) -> dict:
    kpis       = load_kpis(db, store_id)
    orders_df  = load_orders_df(db, store_id)
    creator_df = load_creator_df(db, store_id)
    today      = pd.Timestamp.now()

    # Velocity by category
    shipped = orders_df[orders_df["Order Status"].isin(["Shipped","Completed","Delivered"])].copy()
    shipped["Month"] = shipped["Order_Date"].dt.to_period("M")
    cutoff = today - pd.DateOffset(months=4)
    recent_shipped = shipped[shipped["Order_Date"] >= cutoff]
    cat_monthly = (
        recent_shipped.groupby(["Month", "Product Category"])["SKU Subtotal After Discount"]
        .sum().unstack(fill_value=0)
    ) if "Product Category" in recent_shipped.columns and not recent_shipped.empty else pd.DataFrame()
    cat_trends = []
    if not cat_monthly.empty and len(cat_monthly) >= 2:
        latest = cat_monthly.iloc[-1]; prev = cat_monthly.iloc[-2]
        for cat in cat_monthly.columns:
            pct = ((latest[cat] - prev[cat]) / prev[cat] * 100) if prev[cat] > 0 else 0
            cat_trends.append({"category": cat, "gmv_last_month": round(latest[cat], 0),
                                "pct_change": round(pct, 1)})
        cat_trends.sort(key=lambda x: x["gmv_last_month"], reverse=True)

    # Top products by velocity
    top_skus = []
    if not kpis.empty:
        top = kpis[kpis["AvgVentas30d"] > 0.05].nlargest(10,"AvgVentas30d")
        top_skus = [{"name": r["ProductoNombre"], "tipo": r["Tipo"],
                     "vel_30d": round(r["AvgVentas30d"],2),
                     "price": round(r.get("PRECIO",0),2),
                     "sell_through": round(r.get("SellThroughRate",0),1)}
                    for _, r in top.iterrows()]

    # Creator performance
    creator_gmv = 0; total_gmv = 0; creator_pct = 0
    if not creator_df.empty and "Payment Amount" in creator_df.columns:
        creator_gmv = creator_df["Payment Amount"].sum()
        total_gmv   = shipped["SKU Subtotal After Discount"].sum()
        creator_pct = round(creator_gmv / total_gmv * 100, 1) if total_gmv > 0 else 0

    # Monthly revenue last 3 months
    monthly = (shipped[shipped["Order_Date"] >= today - pd.DateOffset(months=3)]
               .groupby("Month")
               .agg(GMV=("SKU Subtotal After Discount","sum"))
               .reset_index())
    monthly_list = [{"month": str(r["Month"]), "gmv": round(r["GMV"],0)} for _, r in monthly.iterrows()]

    return {
        "analysis_date":  today.strftime("%Y-%m-%d"),
        "top_skus":        top_skus,
        "category_trends": cat_trends,
        "creator_gmv_pct": creator_pct,
        "creator_gmv_usd": round(creator_gmv, 0),
        "monthly_trend":   monthly_list,
        "market_intel":    MARKET_INTEL,
    }


# ── Groq prompt ───────────────────────────────────────────────────────────────

_PROMPT = """\
Eres MESMERIZE, agente de inteligencia de catálogo de fragancias para Rodmat.
Rodmat vende fragancias Avon en TikTok Shop (EE.UU.) — precio bajo ($2-8).

Tu misión: analizar el mercado de fragancias en TikTok Shop y encontrar oportunidades
para el catálogo Avon de Rodmat. ¿Qué fragancias deberían promover más? ¿Contra qué
marcas premium posicionarse? ¿Qué tendencias de aroma están creciendo?

Produce análisis en ESPAÑOL con estas 4 secciones:

=== ESTADO DEL MERCADO ===
(¿Qué está pasando en el mercado de fragancias TikTok Shop este mes?
¿Qué marcas están ganando/perdiendo? ¿Qué tendencias de aroma dominan? Máximo 5 frases.)

=== POSICIÓN COMPETITIVA DE AVON ===
(¿Cómo está posicionado Avon vs la competencia?
¿Qué ventajas tiene? ¿Dónde hay gaps que explotar? Máximo 4 frases.)

=== OPORTUNIDADES DE CATÁLOGO ===
Identifica 3-5 oportunidades específicas para el catálogo Avon:
- ¿Qué SKUs de Avon encajan con tendencias actuales del mercado?
- ¿Qué posicionamiento de precio es más efectivo?
- ¿Qué dupes de marcas premium debería Rodmat destacar?

=== RECOMENDACIONES DE CONTENIDO Y CREADORES ===
Lista 3-4 acciones concretas para creadores: qué tipo de contenido, contra qué marcas comparar,
qué hashtags/tendencias explotar este mes.
"""


def _build_prompt(snapshot: dict) -> str:
    mkt = snapshot["market_intel"]["tiktok_shop_market"]
    avon = snapshot["market_intel"]["avon_position"]

    brands_txt = "\n".join(
        f"  {b['brand']}: ${b['monthly_gmv']/1e6:.1f}M/mes ({b['price_range']}) — {b['notes']}"
        for b in mkt["top_brands_by_revenue"])
    trends_txt = "\n".join(f"  • {t}" for t in mkt["trending_scent_profiles"])
    insights_txt = "\n".join(f"  • {i}" for i in mkt["market_insights"])
    strengths_txt = "\n".join(f"  • {s}" for s in avon["strengths"])
    opps_txt = "\n".join(f"  • {o}" for o in avon["opportunity_gap"])

    top_skus_txt = "\n".join(
        f"  {s['name']} ({s['tipo']}): ${s['price']} | {s['vel_30d']}/día | ST={s['sell_through']}%"
        for s in snapshot["top_skus"][:8]) or "  Sin datos."
    cat_txt = "\n".join(
        f"  {c['category']}: ${c['gmv_last_month']:,.0f} ({c['pct_change']:+.1f}% vs mes ant.)"
        for c in snapshot["category_trends"][:6]) or "  Sin datos."

    return _PROMPT + f"""

DATOS INTERNOS RODMAT ({snapshot['analysis_date']}):

TOP SKUs POR VELOCIDAD:
{top_skus_txt}

TENDENCIAS POR CATEGORÍA:
{cat_txt}

CANAL CREADORES: {snapshot['creator_gmv_pct']}% del GMV = ${snapshot['creator_gmv_usd']:,.0f}

CONTEXTO DE MERCADO TIKTOK SHOP FRAGANCIAS:

TOP MARCAS POR GMV:
{brands_txt}

TENDENCIAS DE AROMA DOMINANTES:
{trends_txt}

INSIGHTS DEL MERCADO:
{insights_txt}

FORTALEZAS AVON EN TIKTOK SHOP:
{strengths_txt}

OPORTUNIDADES GAP PARA AVON:
{opps_txt}
"""


# ── HTML builder ──────────────────────────────────────────────────────────────

def _parse_sections(text: str) -> dict:
    labels = {
        "mercado":    r"=== ESTADO DEL MERCADO ===",
        "posicion":   r"=== POSICIÓN COMPETITIVA DE AVON ===",
        "catalogo":   r"=== OPORTUNIDADES DE CATÁLOGO ===",
        "contenido":  r"=== RECOMENDACIONES DE CONTENIDO Y CREADORES ===",
    }
    keys = list(labels.keys()); patterns = list(labels.values())
    sections = {}
    for i, (key, pat) in enumerate(zip(keys, patterns)):
        nxt = patterns[i + 1] if i + 1 < len(patterns) else None
        m = re.search(pat + r"(.*?)" + (nxt if nxt else r"$"), text, re.S | re.I)
        sections[key] = m.group(1).strip() if m else ""
    if not any(sections.values()):
        sections["mercado"] = text.strip()
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


def build_email_html(analysis_text: str, snapshot: dict, store_name: str = "Rodmat") -> str:
    today    = datetime.now()
    sections = _parse_sections(analysis_text)
    mkt = snapshot["market_intel"]["tiktok_shop_market"]

    brands_rows = "".join(
        f"<tr><td style='padding:6px 10px;font-size:12px;font-weight:bold;'>{b['brand']}</td>"
        f"<td style='padding:6px 10px;font-size:11px;'>{b['tier'].upper()}</td>"
        f"<td style='padding:6px 10px;font-size:11px;text-align:right;'>{b['price_range']}</td>"
        f"<td style='padding:6px 10px;font-size:11px;color:#8892b0;'>{b['notes']}</td></tr>"
        for b in mkt["top_brands_by_revenue"][:6])

    top_skus_rows = "".join(
        f"<tr><td style='padding:6px 10px;font-size:12px;'>{s['name']}</td>"
        f"<td style='padding:6px 10px;font-size:11px;color:#8892b0;'>{s['tipo']}</td>"
        f"<td style='padding:6px 10px;text-align:center;font-weight:bold;'>${s['price']:.2f}</td>"
        f"<td style='padding:6px 10px;text-align:center;'>{s['vel_30d']}/d</td>"
        f"<td style='padding:6px 10px;text-align:center;'>{s['sell_through']}%</td></tr>"
        for s in snapshot["top_skus"][:8])

    return f"""<!DOCTYPE html><html><head><meta charset="utf-8"></head>
<body style="margin:0;padding:0;background-color:#0a0e27;"><table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" bgcolor="#0a0e27" style="background-color:#0a0e27;margin:0;padding:0;"><tr><td align="center" bgcolor="#0a0e27" style="background-color:#0a0e27;padding:20px 10px;"><table role="presentation" width="900" cellpadding="0" cellspacing="0" border="0" bgcolor="#0a0e27" style="max-width:900px;background-color:#0a0e27;color:#e4e9ff;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,Arial,sans-serif;"><tr><td bgcolor="#0a0e27" style="background-color:#0a0e27;padding:0;">
  <div style="background-color:#0f142f;color:#fff;padding:28px 32px;border-radius:12px;margin-bottom:20px;">
    <table width="100%"><tr>
      <td><div style="font-size:10px;letter-spacing:4px;opacity:0.7;text-transform:uppercase;">{store_name} · Catalog Intelligence</div>
        <div style="font-size:32px;font-weight:800;letter-spacing:4px;margin:4px 0;">MESMERIZE</div>
        <div style="font-size:11px;opacity:0.7;">{AGENT_SUBTITLE} · Monthly Report</div></td>
      <td style="text-align:right;vertical-align:top;">
        <div style="font-size:13px;font-weight:bold;">Monthly Market Brief</div>
        <div style="font-size:11px;opacity:0.7;">{today.strftime('%B %Y')}</div>
        <div style="margin-top:10px;background-color:#1a2050;color:#fff;padding:5px 14px;border-radius:20px;font-size:12px;font-weight:bold;display:inline-block;">Creator GMV: {snapshot['creator_gmv_pct']}%</div>
      </td>
    </tr></table>
  </div>
  {_card("Estado del Mercado", sections.get("mercado","—"), "#7B61FF")}
  {_card("Posición Competitiva de Avon", sections.get("posicion","—"), "#FF3D6B")}
  {_card("Oportunidades de Catálogo", sections.get("catalogo","—"), "#00FF88")}
  {_card("Recomendaciones de Contenido y Creadores", sections.get("contenido","—"), "#00D4FF")}
  <div style="background-color:#0f142f;border-radius:8px;padding:18px;margin-bottom:16px;">
    <h3 style="color:#00D4FF;margin:0 0 12px;font-size:13px;text-transform:uppercase;">Competencia — Top Marcas TikTok Shop</h3>
    <table width="100%" style="border-collapse:collapse;font-size:12px;">
      <thead><tr style="background-color:#1a2050;color:#fff;">
        <th style="padding:7px 10px;text-align:left;">Marca</th><th style="padding:7px 10px;">Tier</th>
        <th style="padding:7px 10px;text-align:right;">Precio</th><th style="padding:7px 10px;text-align:left;">Notas</th></tr></thead>
      <tbody>{brands_rows}</tbody>
    </table>
  </div>
  <div style="background-color:#0f142f;border-radius:8px;padding:18px;margin-bottom:16px;">
    <h3 style="color:#00D4FF;margin:0 0 12px;font-size:13px;text-transform:uppercase;">Top SKUs Rodmat — Velocidad</h3>
    <table width="100%" style="border-collapse:collapse;font-size:12px;">
      <thead><tr style="background-color:#1a2050;color:#fff;">
        <th style="padding:7px 8px;text-align:left;">Producto</th><th style="padding:7px 8px;">Tipo</th>
        <th style="padding:7px 8px;text-align:center;">Precio</th><th style="padding:7px 8px;text-align:center;">Vel/día</th>
        <th style="padding:7px 8px;text-align:center;">ST%</th></tr></thead>
      <tbody>{top_skus_rows}</tbody>
    </table>
  </div>
  <div style="text-align:center;padding:14px;color:#8892b0;font-size:10px;border-top:1px solid #2a2f5c;">
    <strong style="color:#7B61FF;">{AGENT_NAME}</strong> · {store_name} · {AGENT_SUBTITLE}<br>
    {today.strftime('%B %Y')} · {today.strftime('%Y-%m-%d %H:%M')}
  </div>
</td></tr></table></td></tr></table></body></html>"""


# ── Entry point ───────────────────────────────────────────────────────────────

def _is_first_monday() -> bool:
    today = datetime.now()
    return today.weekday() == 0 and today.day <= 7


def run(db: Session, store_id: str, force: bool = False, test_email: str | None = None, brand_slug: str | None = None) -> bool:
    from app.models.store import Store
    if not force and not _is_first_monday():
        return False
    store = db.query(Store).filter(Store.id == store_id).first()
    if not is_agent_enabled(store, "mesmerize"):
        print(f"[MESMERIZE] Disabled by tenant settings for store {store_id[:8]}")
        return False
    recipients = [test_email] if test_email else get_recipients(store)
    if not recipients:
        print(f"[MESMERIZE] No recipients for store {store_id}")
        return False
    store_name = store.name if store else "Store"

    print(f"[MESMERIZE] Extracting snapshot for {store_name}...")
    snapshot = extract_snapshot(db, store_id)

    print("[MESMERIZE] Calling Groq...")
    analysis = call_groq(_build_prompt(snapshot))
    html = build_email_html(analysis, snapshot, store_name)
    subject = f"MESMERIZE · {snapshot['analysis_date']} · {store_name} · Monthly Fragrance Intelligence"
    ok = send_email(html, subject, recipients)
    print(f"[MESMERIZE] Email {'sent' if ok else 'FAILED'}")
    return ok
