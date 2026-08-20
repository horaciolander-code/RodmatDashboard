"""KHAMRAH agent — Weekly TikTok Statement report (Rodmat brand-aware).

Runs every Monday 09:00 AM Miami timezone.
Reports LAST WEEK (Mon 00:00 → Sun 23:59):
  - Ventas facturadas (revenue del statement)
  - Ventas cobradas (settled, lo que llegó al banco)
  - Pending settlement
  - Waterfall Income → COGS real → Margen NETO REAL
  - Fees breakdown TikTok
  - Top 5 productos por margen

Multi-brand aware: si el store tiene brands_enabled + brands_recipients configurado,
envía email SEPARADO por brand. Sino, envía email agregado.

Named after Lattafa Khamrah (celebration in Arabic) — celebrating weekly
bank deposits.
"""
from __future__ import annotations
from datetime import date, datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.services.agents._base import (
    send_email, send_email_branded, get_recipients, get_brand_recipients_legacy,
    get_brand, is_agent_enabled,
)

AGENT_NAME = "KHAMRAH"
AGENT_FULL = "Weekly TikTok Statement Report"


def _last_week_bounds(today: date) -> tuple[date, date]:
    """Return (mon_start, sun_end) of the week BEFORE today."""
    # weekday(): Mon=0 .. Sun=6
    days_since_mon = today.weekday()
    this_mon = today - timedelta(days=days_since_mon)
    last_mon = this_mon - timedelta(days=7)
    last_sun = last_mon + timedelta(days=6)
    return last_mon, last_sun


def _calc_cogs_real(db: Session, store_id: str, order_ids: list[str]) -> float:
    """COGS real via combos + products.price_cost decomposition."""
    if not order_ids:
        return 0.0
    ph = ",".join([f":o{i}" for i, _ in enumerate(order_ids)])
    params = {"sid": store_id, **{f"o{i}": v for i, v in enumerate(order_ids)}}
    row = db.execute(text(f"""
        WITH sales_expanded AS (
          SELECT so.seller_sku, so.quantity
          FROM sales_orders so
          WHERE so.store_id=:sid AND so.tiktok_order_id IN ({ph})
            AND so.status NOT ILIKE '%%cancel%%'
        ),
        cogs_direct AS (
          SELECT COALESCE(SUM(se.quantity * COALESCE(p.price_cost, 0)), 0) AS c
          FROM sales_expanded se
          JOIN products p ON p.sku = se.seller_sku AND p.store_id = :sid
        ),
        cogs_combo AS (
          SELECT COALESCE(SUM(se.quantity * ci.quantity * COALESCE(p.price_cost, 0)), 0) AS c
          FROM sales_expanded se
          JOIN combos c ON c.combo_sku = se.seller_sku AND c.store_id = :sid
          JOIN combo_items ci ON ci.combo_id = c.id
          JOIN products p ON p.id = ci.product_id
        )
        SELECT (SELECT c FROM cogs_direct) + (SELECT c FROM cogs_combo) AS cogs
    """), params).fetchone()
    return float(row[0] or 0)


def _fetch_week_data(db: Session, store_id: str, mon: date, sun: date, brand_id: str | None):
    """Aggregate stats for the week window, optionally filtered by brand."""
    bc = " AND brand_id = :bid " if brand_id else ""
    params = {"sid": store_id, "mon": mon, "sun": sun}
    if brand_id: params["bid"] = brand_id

    kpis = db.execute(text(f"""
        SELECT COALESCE(SUM(order_income), 0)                              AS revenue,
               COALESCE(SUM(net_order_margin), 0)                          AS margin_tt,
               COALESCE(SUM(CASE WHEN order_settled_date IS NOT NULL THEN order_income ELSE 0 END), 0) AS settled,
               COALESCE(SUM(CASE WHEN order_settled_date IS NULL THEN order_income ELSE 0 END), 0)     AS pending,
               COALESCE(SUM(referral_fee + smart_promo_fee + smart_promo_camp_fee + managed_service_fee + tiktok_shipping_fee + fbt_shipping_fee), 0) AS fees_total,
               COALESCE(SUM(affiliate_commission), 0)                      AS affiliate,
               COALESCE(SUM(fbt_shipping_fee + tiktok_shipping_fee), 0)    AS shipping,
               COALESCE(SUM(referral_fee), 0)                              AS referral,
               COALESCE(SUM(smart_promo_fee + smart_promo_camp_fee), 0)    AS smart_promo,
               COUNT(DISTINCT order_id)                                    AS orders
        FROM tiktok_statement_lines
        WHERE store_id=:sid AND order_paid_date BETWEEN :mon AND :sun {bc}
    """), params).fetchone()

    oids = [r[0] for r in db.execute(text(f"""
        SELECT DISTINCT order_id FROM tiktok_statement_lines
        WHERE store_id=:sid AND order_paid_date BETWEEN :mon AND :sun {bc}
    """), params).fetchall()]
    cogs = _calc_cogs_real(db, store_id, oids)

    top = db.execute(text(f"""
        SELECT product_name, SUM(net_order_margin) AS m, SUM(order_income) AS r, SUM(sold_quantity) AS u
        FROM tiktok_statement_lines
        WHERE store_id=:sid AND order_paid_date BETWEEN :mon AND :sun {bc}
        GROUP BY product_name ORDER BY m DESC NULLS LAST LIMIT 5
    """), params).fetchall()

    revenue = float(kpis.revenue)
    tt_fees = float(kpis.fees_total) + float(kpis.affiliate)
    net_real = revenue - tt_fees - cogs
    return {
        "revenue": revenue, "settled": float(kpis.settled), "pending": float(kpis.pending),
        "margin_tt": float(kpis.margin_tt), "cogs_real": cogs, "net_real": net_real,
        "net_pct": (net_real/revenue*100) if revenue else 0,
        "fees_total": float(kpis.fees_total), "shipping": float(kpis.shipping),
        "referral": float(kpis.referral), "smart_promo": float(kpis.smart_promo),
        "affiliate": float(kpis.affiliate),
        "orders": int(kpis.orders),
        "top": [{"name": (r.product_name or "")[:60], "margin": float(r.m or 0),
                 "revenue": float(r.r or 0), "units": int(r.u or 0)} for r in top],
    }


def _render_html(store_name: str, brand_display: str | None, mon: date, sun: date, d: dict) -> str:
    """Render dark-futuristic email HTML (paleta neon)."""
    def _fmt(v): return f"${v:,.2f}"
    def _pct(v): return f"{v:.1f}%"
    top_html = "".join(
        f'<tr><td style="padding:6px 8px;color:#e4e9ff;">{i+1}. {t["name"]}</td>'
        f'<td style="padding:6px 8px;text-align:right;color:#00FF88;font-weight:600;">{_fmt(t["margin"])}</td>'
        f'<td style="padding:6px 8px;text-align:right;color:#8892b0;">{t["units"]}u</td></tr>'
        for i, t in enumerate(d["top"])
    ) or '<tr><td colspan="3" style="padding:12px;text-align:center;color:#8892b0;">Sin ventas esta semana</td></tr>'

    brand_line = f' — <span style="color:#7B61FF;">{brand_display}</span>' if brand_display else ""
    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"/>
<style>
  body {{margin:0;padding:20px;background:#0a0e27;color:#e4e9ff;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,Arial,sans-serif;}}
  .card {{max-width:640px;margin:0 auto;background:linear-gradient(135deg,#0f142f,#141a3d);border-radius:14px;padding:24px;border:1px solid rgba(123,97,255,0.2);}}
  .h {{font-size:20px;font-weight:700;background:linear-gradient(90deg,#00D4FF,#7B61FF);-webkit-background-clip:text;-webkit-text-fill-color:transparent;margin:0 0 4px;}}
  .sub {{color:#8892b0;font-size:12px;text-transform:uppercase;letter-spacing:1px;margin-bottom:18px;}}
  .kpi-row {{display:flex;gap:8px;margin-bottom:18px;flex-wrap:wrap;}}
  .kpi {{flex:1 1 45%;background:rgba(15,20,47,0.7);border:1px solid rgba(123,97,255,0.15);border-radius:10px;padding:12px 14px;min-width:150px;}}
  .kpi-l {{color:#8892b0;font-size:10px;text-transform:uppercase;letter-spacing:0.5px;}}
  .kpi-v {{color:#fff;font-size:22px;font-weight:700;margin-top:4px;}}
  .kpi.settled .kpi-v {{color:#00D4FF;}}
  .kpi.pending .kpi-v {{color:#FF9F45;}}
  .kpi.net .kpi-v {{color:#00FF88;}}
  .section-title {{color:#8892b0;font-size:11px;text-transform:uppercase;letter-spacing:1px;margin:16px 0 8px;font-weight:700;}}
  .waterfall {{background:rgba(15,20,47,0.5);border-radius:8px;padding:12px;}}
  .wf-row {{display:flex;justify-content:space-between;padding:6px 0;border-bottom:1px solid rgba(123,97,255,0.08);}}
  .wf-row:last-child {{border:none;font-weight:700;}}
  .wf-l {{color:#c5cdd6;font-size:13px;}}
  .wf-v {{font-family:'SF Mono',Menlo,monospace;font-weight:600;}}
  .wf-pos {{color:#00FF88;}} .wf-neg {{color:#FF6B35;}} .wf-final {{color:#00D4FF;font-size:16px;}}
  table.top {{width:100%;font-size:13px;border-collapse:collapse;background:rgba(15,20,47,0.5);border-radius:8px;overflow:hidden;}}
  table.top th {{padding:8px;color:#8892b0;font-size:10px;text-transform:uppercase;text-align:left;border-bottom:1px solid rgba(123,97,255,0.15);}}
  .foot {{color:#576177;font-size:11px;text-align:center;margin-top:20px;padding-top:14px;border-top:1px solid rgba(123,97,255,0.1);}}
</style></head><body>
<div class="card">
  <div class="h">✨ KHAMRAH — Reporte Semanal</div>
  <div class="sub">{store_name}{brand_line} · {mon.strftime('%d %b')} → {sun.strftime('%d %b %Y')}</div>

  <div class="section-title">📦 Facturado vs 💰 Cobrado</div>
  <div class="kpi-row">
    <div class="kpi"><div class="kpi-l">Facturado</div><div class="kpi-v">{_fmt(d['revenue'])}</div><div style="font-size:10px;color:#8892b0;">{d['orders']} órdenes</div></div>
    <div class="kpi settled"><div class="kpi-l">Cobrado (banco)</div><div class="kpi-v">{_fmt(d['settled'])}</div></div>
    <div class="kpi pending"><div class="kpi-l">Pending settlement</div><div class="kpi-v">{_fmt(d['pending'])}</div></div>
    <div class="kpi net"><div class="kpi-l">Margen NETO real</div><div class="kpi-v">{_fmt(d['net_real'])}</div><div style="font-size:10px;color:#00FF88;">{_pct(d['net_pct'])} sobre revenue</div></div>
  </div>

  <div class="section-title">🌊 Cascada Income → Margen Neto Real</div>
  <div class="waterfall">
    <div class="wf-row"><span class="wf-l">Income (bruto TikTok)</span><span class="wf-v wf-pos">{_fmt(d['revenue'])}</span></div>
    <div class="wf-row"><span class="wf-l">− Fees TT (referral+promo+managed+shipping)</span><span class="wf-v wf-neg">-{_fmt(d['fees_total'])}</span></div>
    <div class="wf-row"><span class="wf-l">− Affiliate commission</span><span class="wf-v wf-neg">-{_fmt(d['affiliate'])}</span></div>
    <div class="wf-row"><span class="wf-l">− COGS mercancía (real)</span><span class="wf-v wf-neg">-{_fmt(d['cogs_real'])}</span></div>
    <div class="wf-row"><span class="wf-l">= Margen NETO REAL</span><span class="wf-v wf-final">{_fmt(d['net_real'])}  ({_pct(d['net_pct'])})</span></div>
  </div>

  <div class="section-title">🏆 Top 5 productos por margen</div>
  <table class="top">
    <thead><tr><th>Producto</th><th style="text-align:right;">Margen</th><th style="text-align:right;">Ud.</th></tr></thead>
    <tbody>{top_html}</tbody>
  </table>

  <div class="foot">Datos: TikTok Merchant Statement (Profit & Loss) · último import · Rodmat Dashboard</div>
</div></body></html>"""


def run(db: Session, store_id: str, force: bool = False, test_email: str | None = None, brand_slug: str | None = None) -> bool:
    """Execute KHAMRAH weekly report.
    - Only runs on Mondays (or if force=True)
    - Only if store has settings.modules_enabled.tiktok_statements=true
    - If brand_slug provided, filters by that brand + uses brand-specific recipients
    - Else: sends aggregated to store recipients"""
    from app.models import Store
    today = date.today()

    if not force and today.weekday() != 0:  # 0=Mon
        return False

    store = db.query(Store).filter(Store.id == store_id).first()
    if not store:
        return False
    settings = store.settings or {}
    if not (settings.get("modules_enabled") or {}).get("tiktok_statements"):
        return False
    if not is_agent_enabled(store, "khamrah"):
        return False

    mon, sun = _last_week_bounds(today)
    brand = None
    if brand_slug:
        brand = get_brand(db, store_id, brand_slug)
        if not brand:
            return False

    data = _fetch_week_data(db, store_id, mon, sun, brand.id if brand else None)
    if data["revenue"] == 0 and data["orders"] == 0 and not force:
        # No hay data para la semana pasada, skip
        return False

    html = _render_html(store.name, brand.display_name if brand else None, mon, sun, data)
    subject = f"✨ KHAMRAH · {store.name}{' – '+brand.display_name if brand else ''} · Sem {mon.strftime('%d %b')}–{sun.strftime('%d %b')}"

    if test_email:
        recipients = [test_email]
    else:
        recipients = get_brand_recipients_legacy(store, brand) if brand else get_recipients(store)
    if not recipients:
        return False

    ok = send_email_branded(html, subject, recipients, brand) if brand else send_email(html, subject, recipients)
    return ok
