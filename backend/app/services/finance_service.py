"""
Finance service — P&L engine + custom lines CRUD.

P&L se computa OnDemand desde sales_orders + affiliate_sales + products.
Las líneas custom de la calculadora (alquileres, sueldos, etc.) viven en finance_custom_lines.
"""
from __future__ import annotations
from datetime import date
from decimal import Decimal
from typing import List, Optional
from collections import defaultdict

from sqlalchemy.orm import Session
from sqlalchemy import text

from app.models.finance import FinanceCustomLine


# Fees por plataforma (defaults — pueden ajustarse en el futuro via store.settings)
FEE_RATES = {
    "tiktok": {"referral": 0.06,  "smart_promo": 0.035, "smart_promo_campaign": 0.01},
    "amazon": {"referral": 0.15,  "smart_promo": 0.0,   "smart_promo_campaign": 0.0},
}


def _period_bounds(year: int, period: str) -> tuple[date, date, str, Optional[int]]:
    """
    period puede ser 'YTD' o 'MM' (zero-padded como '05').
    Devuelve (start, end_exclusivo, label, month_int|None).
    """
    if period.upper() == "YTD":
        return date(year, 1, 1), date(year + 1, 1, 1), f"YTD {year}", None
    m = int(period)
    if m < 1 or m > 12:
        raise ValueError("month out of range")
    start = date(year, m, 1)
    end = date(year + 1, 1, 1) if m == 12 else date(year, m + 1, 1)
    MES_ES = ["", "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
              "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
    return start, end, f"{MES_ES[m]} {year}", m


def _build_combo_map(db: Session, store_id: str):
    rows = db.execute(text("""
        SELECT c.combo_sku, ci.product_id, ci.quantity
        FROM combos c JOIN combo_items ci ON ci.combo_id = c.id
        WHERE c.store_id = :sid
    """), {"sid": store_id}).fetchall()
    out: dict[str, list] = {}
    for r in rows:
        out.setdefault(r.combo_sku, []).append((r.product_id, int(r.quantity)))
    return out


def _build_product_maps(db: Session, store_id: str):
    rows = db.execute(text("""
        SELECT id, name, COALESCE(price_cost, 0) AS price_cost
        FROM products WHERE store_id = :sid
    """), {"sid": store_id}).fetchall()
    by_id = {r.id: {"name": r.name, "cost": float(r.price_cost)} for r in rows}
    by_name = {r.name.strip().lower(): r.id for r in rows}
    return by_id, by_name


def _build_amazon_map(db: Session, store_id: str):
    rows = db.execute(text("""
        SELECT amazon_sku, product_id FROM amazon_sku_map WHERE store_id = :sid
    """), {"sid": store_id}).fetchall()
    return {r.amazon_sku: r.product_id for r in rows}


def _aggregate_sales(db: Session, store_id: str, platform: str, start: date, end: date):
    """Agrega columnas raw de sales_orders para una plataforma + ventana."""
    row = db.execute(text("""
        SELECT
            COALESCE(SUM(CAST(sku_subtotal_after_discount AS NUMERIC)
                        + CAST(sku_seller_discount AS NUMERIC)
                        + CAST(sku_platform_discount AS NUMERIC)), 0) AS gross,
            COALESCE(SUM(CAST(sku_seller_discount AS NUMERIC)), 0)     AS sd,
            COALESCE(SUM(CAST(sku_platform_discount AS NUMERIC)), 0)   AS pd,
            COALESCE(SUM(CAST(sku_subtotal_after_discount AS NUMERIC)),0) AS gmv,
            COALESCE(SUM(CAST(shipping_fee_after_discount AS NUMERIC)),0) AS shb,
            COALESCE(SUM(CAST(original_shipping_fee AS NUMERIC)),0)     AS shc,
            COALESCE(SUM(CAST(order_amount AS NUMERIC)),0)              AS oa,
            COALESCE(SUM(CAST(order_refund_amount AS NUMERIC)),0)       AS ref
        FROM sales_orders
        WHERE store_id = :sid
          AND order_date >= :start AND order_date < :end
          AND platform = :plat
          AND status NOT ILIKE '%%cancel%%'
    """), {"sid": store_id, "start": start, "end": end, "plat": platform}).fetchone()
    return {
        "gross_subtotal":   float(row.gross),
        "seller_discount":  float(row.sd),
        "platform_discount":float(row.pd),
        "gmv":              float(row.gmv),
        "shipping_buyer":   float(row.shb),
        "shipping_carrier": float(row.shc),
        "order_amount":     float(row.oa),
        "refunds":          float(row.ref),
    }


def _cogs_for_platform(db: Session, store_id: str, platform: str, start: date, end: date,
                       combo_map, products_by_id, products_by_name, amazon_map) -> float:
    """COGS = sum(units × price_cost), expandiendo combos y mapping Amazon."""
    rows = db.execute(text("""
        SELECT seller_sku, sku, product_name, quantity
        FROM sales_orders
        WHERE store_id = :sid
          AND order_date >= :start AND order_date < :end
          AND platform = :plat
          AND status NOT ILIKE '%%cancel%%'
    """), {"sid": store_id, "start": start, "end": end, "plat": platform}).fetchall()
    total = 0.0
    for r in rows:
        sku = (r.seller_sku or "").strip()
        qty = int(r.quantity or 0)
        if sku in combo_map:
            for pid, qpc in combo_map[sku]:
                total += qty * qpc * products_by_id.get(pid, {}).get("cost", 0)
        elif sku in amazon_map:
            pid = amazon_map[sku]
            total += qty * products_by_id.get(pid, {}).get("cost", 0)
        else:
            key = (r.product_name or "").strip().lower()
            pid = products_by_name.get(key)
            if pid:
                total += qty * products_by_id[pid]["cost"]
    return total


def _creators_commission(db: Session, store_id: str, start: date, end: date) -> float:
    row = db.execute(text("""
        SELECT COALESCE(SUM(CAST(commission AS NUMERIC)), 0) AS c
        FROM affiliate_sales
        WHERE store_id = :sid
          AND time_created >= :start AND time_created < :end
    """), {"sid": store_id, "start": start, "end": end}).fetchone()
    return float(row.c)


def _compute_platform_block(agg: dict, platform: str) -> dict:
    """Toma agregados raw y añade fees + platform_adjustment + net_order_amount."""
    rates = FEE_RATES.get(platform, FEE_RATES["tiktok"])
    gmv = agg["gmv"]
    referral = gmv * rates["referral"]
    promo    = gmv * rates["smart_promo"]
    promo_c  = gmv * rates["smart_promo_campaign"]
    fees_total = referral + promo + promo_c
    platform_adj = agg["order_amount"] - agg["gmv"] - agg["shipping_buyer"]
    net_oa = agg["order_amount"] - agg["refunds"]
    return {
        **agg,
        "platform_adjustment":      platform_adj,
        "net_order_amount":         net_oa,
        "referral_fee":             referral,
        "smart_promo_fee":          promo,
        "smart_promo_campaign_fee": promo_c,
        "fees_total":               fees_total,
        "cogs":                     0.0,  # se rellena luego
        "creators_commission":      0.0,  # se rellena solo en TOTAL
    }


def compute_pl(db: Session, store_id: str, year: int, period: str) -> dict:
    """Computa el P&L estructurado para un store + período (mes 'MM' o 'YTD')."""
    start, end, label, month = _period_bounds(year, period)
    combo_map = _build_combo_map(db, store_id)
    products_by_id, products_by_name = _build_product_maps(db, store_id)
    amazon_map = _build_amazon_map(db, store_id)

    blocks = {}
    for plat in ("tiktok", "amazon"):
        agg = _aggregate_sales(db, store_id, plat, start, end)
        cogs = _cogs_for_platform(db, store_id, plat, start, end,
                                  combo_map, products_by_id, products_by_name, amazon_map)
        block = _compute_platform_block(agg, plat)
        block["cogs"] = cogs
        blocks[plat] = block

    # Total = suma de tiktok + amazon (todos los campos)
    total = {k: 0.0 for k in blocks["tiktok"].keys()}
    for plat_block in blocks.values():
        for k, v in plat_block.items():
            total[k] += v
    total["creators_commission"] = _creators_commission(db, store_id, start, end)

    # Resumen
    shipping_net = total["shipping_carrier"] - total["shipping_buyer"]
    gross_margin = (
        total["gmv"]
        - total["cogs"]
        - shipping_net
        - total["fees_total"]
        - total["creators_commission"]
        - total["refunds"]
    )

    # Custom lines del período
    if period.upper() == "YTD":
        lines = db.query(FinanceCustomLine).filter(
            FinanceCustomLine.store_id == store_id,
            FinanceCustomLine.year_month.like(f"{year}-%"),
        ).order_by(FinanceCustomLine.year_month, FinanceCustomLine.sort_order, FinanceCustomLine.id).all()
    else:
        ym = f"{year}-{month:02d}"
        lines = db.query(FinanceCustomLine).filter(
            FinanceCustomLine.store_id == store_id,
            FinanceCustomLine.year_month == ym,
        ).order_by(FinanceCustomLine.sort_order, FinanceCustomLine.id).all()

    custom_income  = sum(float(l.amount) for l in lines if float(l.amount) > 0)
    custom_expense = sum(float(l.amount) for l in lines if float(l.amount) < 0)  # negativo
    custom_net     = custom_income + custom_expense
    net_result     = gross_margin + custom_net

    return {
        "store_id":     store_id,
        "period_label": label,
        "period_type":  "ytd" if period.upper() == "YTD" else "month",
        "year":         year,
        "month":        month,
        "period_start": start.isoformat(),
        "period_end":   end.isoformat(),
        "tiktok":       blocks["tiktok"],
        "amazon":       blocks["amazon"],
        "total":        total,
        "gross_margin": gross_margin,
        "shipping_net": shipping_net,
        "custom_lines": [
            {"id": l.id, "description": l.description, "amount": float(l.amount), "sort_order": float(l.sort_order)}
            for l in lines
        ],
        "custom_total_income":  custom_income,
        "custom_total_expense": abs(custom_expense),
        "custom_net":           custom_net,
        "net_result":           net_result,
    }


# -------- Custom lines CRUD --------

def list_custom_lines(db: Session, store_id: str, year: int, period: str) -> List[FinanceCustomLine]:
    if period.upper() == "YTD":
        return db.query(FinanceCustomLine).filter(
            FinanceCustomLine.store_id == store_id,
            FinanceCustomLine.year_month.like(f"{year}-%"),
        ).order_by(FinanceCustomLine.year_month, FinanceCustomLine.sort_order).all()
    _, _, _, month = _period_bounds(year, period)
    ym = f"{year}-{month:02d}"
    return db.query(FinanceCustomLine).filter(
        FinanceCustomLine.store_id == store_id,
        FinanceCustomLine.year_month == ym,
    ).order_by(FinanceCustomLine.sort_order, FinanceCustomLine.id).all()


def replace_custom_lines(db: Session, store_id: str, year: int, period: str, lines_in: list) -> List[FinanceCustomLine]:
    """Reemplaza TODAS las líneas del mes con la nueva lista (atómico)."""
    if period.upper() == "YTD":
        raise ValueError("No se pueden editar líneas en modo YTD — selecciona un mes concreto")
    _, _, _, month = _period_bounds(year, period)
    ym = f"{year}-{month:02d}"
    db.query(FinanceCustomLine).filter(
        FinanceCustomLine.store_id == store_id,
        FinanceCustomLine.year_month == ym,
    ).delete(synchronize_session=False)
    new_rows = []
    for i, l in enumerate(lines_in):
        desc = (l.description or "").strip()
        if not desc:
            continue
        row = FinanceCustomLine(
            store_id=store_id, year_month=ym,
            description=desc, amount=l.amount,
            sort_order=(l.sort_order if l.sort_order is not None else i),
        )
        db.add(row)
        new_rows.append(row)
    db.commit()
    for r in new_rows: db.refresh(r)
    return new_rows


def copy_from_previous_month(db: Session, store_id: str, year: int, period: str) -> List[FinanceCustomLine]:
    """Copia las líneas del mes anterior al período actual."""
    if period.upper() == "YTD":
        raise ValueError("Copia solo aplica a meses")
    _, _, _, month = _period_bounds(year, period)
    if month == 1:
        prev_year, prev_month = year - 1, 12
    else:
        prev_year, prev_month = year, month - 1
    prev_ym = f"{prev_year}-{prev_month:02d}"
    curr_ym = f"{year}-{month:02d}"

    prev_lines = db.query(FinanceCustomLine).filter(
        FinanceCustomLine.store_id == store_id,
        FinanceCustomLine.year_month == prev_ym,
    ).order_by(FinanceCustomLine.sort_order).all()

    if not prev_lines:
        return []

    # Si ya hay líneas en el mes actual, NO copiar (no machacar)
    existing = db.query(FinanceCustomLine).filter(
        FinanceCustomLine.store_id == store_id,
        FinanceCustomLine.year_month == curr_ym,
    ).count()
    if existing > 0:
        raise ValueError(f"El mes ya tiene {existing} líneas. Vacíalas primero si quieres copiar del anterior.")

    new_rows = []
    for pl in prev_lines:
        row = FinanceCustomLine(
            store_id=store_id, year_month=curr_ym,
            description=pl.description, amount=pl.amount, sort_order=pl.sort_order,
        )
        db.add(row)
        new_rows.append(row)
    db.commit()
    for r in new_rows: db.refresh(r)
    return new_rows
