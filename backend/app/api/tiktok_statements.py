"""TikTok Merchant Statement — endpoints Finance.
Gate: settings.modules_enabled.tiktok_statements=true por store."""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Optional
from datetime import date, timedelta

from app.database import get_db
from app.models import User, Store, Brand
from app.dependencies import get_current_user, get_user_brand_id

router = APIRouter(prefix="/api/tiktok-statements", tags=["tiktok-statements"])


def _require_flag(db: Session, store_id: str):
    store = db.query(Store).filter(Store.id == store_id).first()
    settings = (store.settings or {}) if store else {}
    if not (settings.get("modules_enabled") or {}).get("tiktok_statements"):
        raise HTTPException(status_code=403, detail="Module tiktok_statements not enabled")


def _resolve_brand_slug(user, db, brand_slug):
    if user.brand_id:
        b = db.query(Brand).filter(Brand.id == user.brand_id).first()
        return b.slug if b else None
    return brand_slug


def _brand_clause(user, db, brand_slug):
    """Return (clause_sql, params_dict) para filtrar por brand."""
    bs = _resolve_brand_slug(user, db, brand_slug)
    if not bs:
        return "", {}
    r = db.execute(text("SELECT id FROM brands WHERE store_id=:sid AND slug=:s LIMIT 1"),
                   {"sid": user.store_id, "s": bs}).fetchone()
    if not r:
        return " AND brand_id = 'NEVER_MATCH' ", {}
    return " AND brand_id = :brand_id ", {"brand_id": r[0]}


@router.get("/kpis")
def kpis(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    brand_slug: Optional[str] = None,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    _require_flag(db, user.store_id)
    bc, bp = _brand_clause(user, db, brand_slug)
    date_c = ""
    params = {"sid": user.store_id, **bp}
    if date_from:
        date_c += " AND order_paid_date >= :df "
        params["df"] = date_from
    if date_to:
        date_c += " AND order_paid_date <= :dt "
        params["dt"] = date_to

    row = db.execute(text(f"""
        SELECT
          COALESCE(SUM(order_income), 0)      AS revenue,
          COALESCE(SUM(order_cost), 0)        AS tt_cost,
          COALESCE(SUM(net_order_margin), 0)  AS margin_tt,
          COALESCE(SUM(CASE WHEN order_settled_date IS NOT NULL THEN order_income ELSE 0 END), 0) AS settled,
          COALESCE(SUM(CASE WHEN order_settled_date IS NULL THEN order_income ELSE 0 END), 0) AS pending,
          COALESCE(SUM(referral_fee + smart_promo_fee + smart_promo_camp_fee + managed_service_fee + tiktok_shipping_fee + fbt_shipping_fee), 0) AS fees_total,
          COALESCE(SUM(affiliate_commission), 0) AS affiliate,
          COALESCE(SUM(fbt_shipping_fee + tiktok_shipping_fee), 0) AS shipping,
          COUNT(*) AS lines,
          COUNT(DISTINCT order_id) AS orders,
          COUNT(DISTINCT statement_id) AS statements
        FROM tiktok_statement_lines
        WHERE store_id = :sid {date_c} {bc}
    """), params).fetchone()

    # COGS real (JOIN sales_orders + combos + products.price_cost)
    cogs_row = db.execute(text(f"""
        WITH order_ids AS (
          SELECT DISTINCT order_id FROM tiktok_statement_lines
          WHERE store_id = :sid {date_c} {bc}
        ),
        sales_expanded AS (
          SELECT so.seller_sku, so.quantity
          FROM sales_orders so JOIN order_ids oi ON oi.order_id = so.tiktok_order_id
          WHERE so.store_id = :sid AND so.status NOT ILIKE '%%cancel%%'
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
    cogs = float(cogs_row[0] or 0)
    revenue = float(row.revenue)
    net_real = revenue - abs(float(row.tt_cost)) - cogs

    return {
        "revenue":       revenue,
        "tt_cost":       float(row.tt_cost),
        "margin_tt":     float(row.margin_tt),
        "cogs_real":     cogs,
        "net_margin_real": net_real,
        "net_margin_pct":  (net_real / revenue * 100) if revenue else 0,
        "settled":       float(row.settled),
        "pending":       float(row.pending),
        "settlement_pct": (float(row.settled) / revenue * 100) if revenue else 0,
        "fees_total":    float(row.fees_total),
        "shipping":      float(row.shipping),
        "affiliate":     float(row.affiliate),
        "orders":        int(row.orders),
        "lines":         int(row.lines),
        "statements":    int(row.statements),
    }


@router.get("/weekly")
def weekly(
    weeks: int = Query(12, ge=1, le=52),
    brand_slug: Optional[str] = None,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    _require_flag(db, user.store_id)
    bc, bp = _brand_clause(user, db, brand_slug)
    params = {"sid": user.store_id, "weeks": weeks, **bp}
    rows = db.execute(text(f"""
        SELECT DATE_TRUNC('week', order_paid_date)::date AS week,
               SUM(order_income) AS revenue,
               SUM(CASE WHEN order_settled_date IS NOT NULL THEN order_income ELSE 0 END) AS settled,
               SUM(CASE WHEN order_settled_date IS NULL THEN order_income ELSE 0 END) AS pending,
               SUM(net_order_margin) AS margin_tt,
               COUNT(DISTINCT order_id) AS orders
        FROM tiktok_statement_lines
        WHERE store_id = :sid AND order_paid_date IS NOT NULL {bc}
        GROUP BY 1 ORDER BY 1 DESC LIMIT :weeks
    """), params).fetchall()
    return [{"week": str(r.week), "revenue": float(r.revenue),
             "settled": float(r.settled), "pending": float(r.pending),
             "margin_tt": float(r.margin_tt), "orders": r.orders} for r in reversed(rows)]


@router.get("/fees-breakdown")
def fees(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    brand_slug: Optional[str] = None,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    _require_flag(db, user.store_id)
    bc, bp = _brand_clause(user, db, brand_slug)
    date_c = ""; params = {"sid": user.store_id, **bp}
    if date_from: date_c += " AND order_paid_date >= :df "; params["df"] = date_from
    if date_to: date_c += " AND order_paid_date <= :dt "; params["dt"] = date_to
    row = db.execute(text(f"""
        SELECT COALESCE(SUM(referral_fee),0) AS referral,
               COALESCE(SUM(smart_promo_fee + smart_promo_camp_fee),0) AS smart_promo,
               COALESCE(SUM(managed_service_fee),0) AS managed,
               COALESCE(SUM(tiktok_shipping_fee + fbt_shipping_fee),0) AS shipping,
               COALESCE(SUM(affiliate_commission),0) AS affiliate,
               COALESCE(SUM(seller_discount),0) AS seller_discount
        FROM tiktok_statement_lines
        WHERE store_id = :sid {date_c} {bc}
    """), params).fetchone()
    return {"referral": float(row.referral), "smart_promo": float(row.smart_promo),
            "managed": float(row.managed), "shipping": float(row.shipping),
            "affiliate": float(row.affiliate), "seller_discount": float(row.seller_discount)}


@router.get("/top-products")
def top(
    n: int = Query(10, ge=1, le=50),
    brand_slug: Optional[str] = None,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    _require_flag(db, user.store_id)
    bc, bp = _brand_clause(user, db, brand_slug)
    rows = db.execute(text(f"""
        SELECT product_name,
               SUM(order_income) AS revenue,
               SUM(net_order_margin) AS margin,
               SUM(sold_quantity) AS units,
               COUNT(*) AS lines
        FROM tiktok_statement_lines
        WHERE store_id = :sid {bc}
        GROUP BY product_name ORDER BY margin DESC NULLS LAST LIMIT :n
    """), {"sid": user.store_id, "n": n, **bp}).fetchall()
    return [{"product_name": r.product_name, "revenue": float(r.revenue),
             "margin": float(r.margin), "units": r.units, "lines": r.lines} for r in rows]


@router.get("/statements")
def statements(
    brand_slug: Optional[str] = None,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Lista de payouts (uno por bank deposit)."""
    _require_flag(db, user.store_id)
    _resolve_brand_slug(user, db, brand_slug)  # forces user brand if scoped
    rows = db.execute(text("""
        SELECT statement_id, payout_id, total_income, total_cost, total_margin, total_fees,
               total_orders, period_start, period_end, settled_date
        FROM tiktok_statements WHERE store_id = :sid
        ORDER BY settled_date DESC NULLS LAST, period_end DESC LIMIT 200
    """), {"sid": user.store_id}).fetchall()
    return [{
        "statement_id": r.statement_id, "payout_id": r.payout_id,
        "total_income": float(r.total_income or 0), "total_cost": float(r.total_cost or 0),
        "total_margin": float(r.total_margin or 0), "total_fees": float(r.total_fees or 0),
        "total_orders": r.total_orders, "period_start": str(r.period_start) if r.period_start else None,
        "period_end": str(r.period_end) if r.period_end else None,
        "settled_date": str(r.settled_date) if r.settled_date else None,
    } for r in rows]
