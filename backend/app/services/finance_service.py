"""
Finance Service — bank transaction management, ported from V1 finance_data.py + finance_classifier.py.
Classifier: rule-based (regex) only, no sklearn dependency.
"""
from __future__ import annotations

import re
import uuid
from datetime import date, datetime, timedelta
from typing import Optional

from sqlalchemy.orm import Session

from app.models.finance import BankTransaction
from app.models.store import Store

# ---------------------------------------------------------------------------
# Column normalization (bank export headers → standard schema)
# ---------------------------------------------------------------------------
COLUMN_MAP: dict[str, str] = {
    "date": "date", "Date": "date", "DATE": "date",
    "fecha": "date", "Fecha": "date", "FECHA": "date",
    "description": "description", "Description": "description", "DESCRIPTION": "description",
    "concepto": "description", "Concepto": "description", "DESC": "description",
    "memo": "description", "Memo": "description", "MEMO": "description",
    "amount": "amount", "Amount": "amount", "AMOUNT": "amount",
    "importe": "amount", "Importe": "amount", "monto": "amount", "Monto": "amount",
    "debit": "amount", "Debit": "amount", "credit": "amount", "Credit": "amount",
    "running_balance": "running_balance",
    "Running Bal.": "running_balance", "Running Bal,": "running_balance",
    "Running Balance": "running_balance", "running bal": "running_balance",
    "saldo": "running_balance", "Saldo": "running_balance", "SALDO": "running_balance",
    "balance": "running_balance", "Balance": "running_balance", "BALANCE": "running_balance",
}

# ---------------------------------------------------------------------------
# Default classification catalog
# ---------------------------------------------------------------------------
DEFAULT_CLASSIFICATIONS: dict[str, list[str]] = {
    "Balance": [
        "Capital social",
        "Cuenta por cobrar",
        "Prestamo",
        "Prestamo personal",
    ],
    "Gastos": [
        "Alquiler oficina",
        "Amortización credito",
        "Asesoria",
        "CXP Proveedor",
        "Comida",
        "Comisión bancaria",
        "Equipo oficina",
        "Gasolina",
        "Gasto de contabilidad",
        "Gastos de envío",
        "Gastos de oficina",
        "Gastos vehiculo",
        "Intereses bancarios",
        "Internet",
        "Luz",
        "Mobiliario de oficina",
        "Reparacion y mto. Equipos de oficina",
        "Sueldo Horacio",
        "Sueldo Oralia",
        "Sueldos y salarios warehouse",
        "Suscripciones",
        "Taxi",
    ],
    "Pendiente": [],
}

# ---------------------------------------------------------------------------
# Rule-based classifier (ported from V1 finance_classifier.py)
# ---------------------------------------------------------------------------
_RULES: list[tuple[str, str, str]] = [
    (r"(?i)zelle\s+to\s+oralia", "Gastos", "Sueldo Oralia"),
    (r"(?i)zelle\s+to\s+(sophia|emilio)", "Gastos", "Sueldos y salarios warehouse"),
    (r"(?i)zelle\s+to\s+gabriela\s+perez", "Gastos", "Gasto de contabilidad"),
    (r"(?i)zelle.*(factura|proveedor)", "Gastos", "CXP Proveedor"),
    (r"(?i)(factura|proveedor)", "Gastos", "CXP Proveedor"),
    (r"(?i)(adobe|godaddy|expressvpn|netflix|spotify|microsoft|dropbox|zoom|slack)", "Gastos", "Suscripciones"),
    (r"(?i)murphy.*purchase", "Gastos", "Gasolina"),
    (r"(?i)(shell|bp|exxon|chevron|mobil|gas\s+station|gasolinera)", "Gastos", "Gasolina"),
    (r"(?i)(coffee|fuddruckers|fresh\s*mark|restaurant|mcdonald|starbucks|subway|domino|pizza)", "Gastos", "Comida"),
    (r"(?i)pirate\s+ship", "Gastos", "Gastos de envío"),
    (r"(?i)(ups|fedex|usps|dhl|shipstation|stamps\.com)", "Gastos", "Gastos de envío"),
    (r"(?i)uber.*trip", "Gastos", "Taxi"),
    (r"(?i)(lyft|taxi|cabify|blablacar)", "Gastos", "Taxi"),
    (r"(?i)parafin\s+capital", "Gastos", "Amortización credito"),
    (r"(?i)(loan\s+payment|amortizaci[oó]n|credito|credit\s+payment)", "Gastos", "Amortización credito"),
    (r"(?i)transfer\s+amazon\.com", "Balance", "Cuenta por cobrar"),
    (r"(?i)transfer\s+stor\s+rb", "Balance", "Cuenta por cobrar"),
    (r"(?i)(tiktok.*payment|tiktok.*payout|tiktok.*transfer)", "Balance", "Cuenta por cobrar"),
    (r"(?i)(loyalty\s+underwrit|amazon\s+mktpl)", "Gastos", "Asesoria"),
    (r"(?i)(wire\s+in|wire\s+type.*in|capital\s+social)", "Balance", "Capital social"),
    (r"(?i)(rent|alquiler|lease\s+payment)", "Gastos", "Alquiler oficina"),
    (r"(?i)(electric|utility|pge|duke\s+energy|light\s+bill|electricity)", "Gastos", "Luz"),
    (r"(?i)(comcast|spectrum|att\s+internet|xfinity|internet|broadband)", "Gastos", "Internet"),
    (r"(?i)(apple|dell|best\s+buy|newegg|office\s+depot|staples)", "Gastos", "Equipo oficina"),
    (r"(?i)(bank\s+fee|service\s+charge|monthly\s+fee|wire\s+fee|overdraft)", "Gastos", "Comisión bancaria"),
    (r"(?i)(interest\s+charge|finance\s+charge)", "Gastos", "Intereses bancarios"),
    (r"(?i)zelle\s+to\s+horacio", "Gastos", "Sueldo Horacio"),
    (r"(?i)transfer.*horacio", "Gastos", "Sueldo Horacio"),
    (r"(?i)(personal\s+loan|prestamo\s+personal)", "Balance", "Prestamo personal"),
    (r"(?i)(loan\s+proceeds|prestamo(?!\s+personal))", "Balance", "Prestamo"),
]


def classify_transaction(description: str, amount: float) -> tuple[Optional[str], Optional[str], float, str]:
    """Returns (tipo, clasificacion, confidence, method). method: 'rule_based' | 'pending'."""
    for pattern, tipo, clasif in _RULES:
        if re.search(pattern, description):
            return tipo, clasif, 1.0, "rule_based"
    return None, None, 0.0, "pending"


# ---------------------------------------------------------------------------
# Column normalization helper
# ---------------------------------------------------------------------------
def normalize_columns(df) -> "pd.DataFrame":
    import pandas as pd
    rename = {col: COLUMN_MAP[col] for col in df.columns if col in COLUMN_MAP}
    df = df.rename(columns=rename)
    if "date" in df.columns and not pd.api.types.is_datetime64_any_dtype(df["date"]):
        parsed = pd.to_datetime(df["date"], format="%m/%d/%Y", errors="coerce")
        nat_mask = parsed.isna()
        if nat_mask.any():
            parsed[nat_mask] = pd.to_datetime(df["date"][nat_mask], errors="coerce")
        df["date"] = parsed
    return df


def _make_key(row_date, description: str, amount: float) -> str:
    date_str = str(row_date)[:10] if row_date else ""
    desc = str(description).strip().lower()[:50]
    amt = round(float(amount), 2)
    return f"{date_str}_{desc}_{amt}"


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------
def get_transactions(
    db: Session,
    store_id: str,
    skip: int = 0,
    limit: int = 500,
    tipo: str = "Todos",
    estado: str = "Todas",
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
) -> list[dict]:
    q = db.query(BankTransaction).filter(BankTransaction.store_id == store_id)
    if date_from:
        q = q.filter(BankTransaction.date >= date_from)
    if date_to:
        q = q.filter(BankTransaction.date <= date_to)
    if tipo != "Todos":
        q = q.filter(BankTransaction.tipo == tipo)
    if estado == "Pendientes":
        q = q.filter(BankTransaction.is_pending_review == True)
    elif estado == "Auto-clasificadas":
        q = q.filter(BankTransaction.classification_method.in_(["rule_based", "ml"]))
    elif estado == "Manuales":
        q = q.filter(BankTransaction.classification_method == "manual")
    rows = q.order_by(BankTransaction.date.desc().nullslast()).offset(skip).limit(limit).all()
    return [_tx_to_dict(r) for r in rows]


def get_pending_count(db: Session, store_id: str) -> int:
    return db.query(BankTransaction).filter(
        BankTransaction.store_id == store_id,
        BankTransaction.is_pending_review == True,
    ).count()


def update_transaction(
    db: Session,
    store_id: str,
    tx_id: str,
    tipo: str,
    clasificacion: str,
    comentarios: Optional[str] = None,
) -> Optional[dict]:
    tx = db.query(BankTransaction).filter(
        BankTransaction.id == tx_id,
        BankTransaction.store_id == store_id,
    ).first()
    if not tx:
        return None
    tx.tipo = tipo
    tx.clasificacion = clasificacion
    if comentarios is not None:
        tx.comentarios = comentarios
    tx.is_pending_review = False
    tx.classification_method = "manual"
    tx.classification_confidence = 1.0
    db.commit()
    db.refresh(tx)
    return _tx_to_dict(tx)


def delete_transaction(db: Session, store_id: str, tx_id: str) -> bool:
    tx = db.query(BankTransaction).filter(
        BankTransaction.id == tx_id,
        BankTransaction.store_id == store_id,
    ).first()
    if not tx:
        return False
    db.delete(tx)
    db.commit()
    return True


def _tx_to_dict(tx: BankTransaction) -> dict:
    return {
        "id": tx.id,
        "date": tx.date.isoformat() if tx.date else None,
        "description": tx.description,
        "amount": tx.amount,
        "running_balance": tx.running_balance,
        "tipo": tx.tipo,
        "clasificacion": tx.clasificacion,
        "comentarios": tx.comentarios,
        "is_pending_review": tx.is_pending_review,
        "classification_method": tx.classification_method,
        "classification_confidence": tx.classification_confidence,
    }


# ---------------------------------------------------------------------------
# Preview (file → classified rows, no DB write)
# ---------------------------------------------------------------------------
def preview_file(file_bytes: bytes, filename: str) -> dict:
    import io
    import pandas as pd

    if filename.endswith(".csv"):
        df_raw = pd.read_csv(io.BytesIO(file_bytes))
    else:
        df_raw = pd.read_excel(io.BytesIO(file_bytes))

    detected = []
    for col in df_raw.columns:
        mapped = COLUMN_MAP.get(col)
        detected.append({"original": col, "mapped": mapped or "—", "ok": bool(mapped)})

    required = {"date", "description", "amount"}
    mapped_targets = {COLUMN_MAP[c] for c in df_raw.columns if c in COLUMN_MAP}
    missing = list(required - mapped_targets)

    if missing:
        return {"ok": False, "missing": missing, "detected": detected, "rows": []}

    df = normalize_columns(df_raw.copy())
    rows = []
    for _, row in df.iterrows():
        desc = str(row.get("description", ""))
        amt = float(row.get("amount", 0) or 0)
        rb = row.get("running_balance")
        fecha = row.get("date")
        tipo, clasif, conf, method = classify_transaction(desc, amt)
        rows.append({
            "fecha": fecha.strftime("%Y-%m-%d") if hasattr(fecha, "strftime") else (str(fecha)[:10] if fecha else None),
            "description": desc[:120],
            "amount": amt,
            "running_balance": float(rb) if rb is not None and str(rb) not in ("nan", "None") else None,
            "tipo": tipo or "Pendiente",
            "clasificacion": clasif or "",
            "confidence": conf,
            "method": method,
        })

    return {"ok": True, "missing": [], "detected": detected, "rows": rows}


# ---------------------------------------------------------------------------
# Import (confirmed rows → DB)
# ---------------------------------------------------------------------------
def import_rows(db: Session, store_id: str, rows: list[dict]) -> dict:
    existing_keys = {
        tx.transaction_key
        for tx in db.query(BankTransaction.transaction_key)
        .filter(BankTransaction.store_id == store_id)
        .all()
    }

    added, duplicates, pending = 0, 0, 0
    for r in rows:
        fecha = r.get("fecha")
        key = _make_key(fecha, r.get("description", ""), r.get("amount", 0))
        if key in existing_keys:
            duplicates += 1
            continue

        tipo = (r.get("tipo") or "").strip()
        clasif = (r.get("clasificacion") or "").strip()
        _invalid = {"", "nan", "—", "Pendiente", "None"}
        is_classified = tipo not in _invalid and tipo != "Pendiente"
        is_pending = not is_classified
        if is_pending:
            pending += 1

        tx = BankTransaction(
            id=str(uuid.uuid4()),
            store_id=store_id,
            transaction_key=key,
            date=_parse_date(fecha),
            description=r.get("description", ""),
            amount=float(r.get("amount", 0)),
            running_balance=r.get("running_balance"),
            tipo=tipo if not is_pending else "Pendiente",
            clasificacion=clasif if not is_pending else "",
            is_pending_review=is_pending,
            classification_method="manual" if is_classified else "pending",
            classification_confidence=1.0 if is_classified else 0.0,
        )
        db.add(tx)
        existing_keys.add(key)
        added += 1

    db.commit()
    return {"added": added, "duplicates": duplicates, "pending": pending}


def _parse_date(fecha_str) -> Optional[date]:
    if not fecha_str:
        return None
    try:
        return datetime.strptime(str(fecha_str)[:10], "%Y-%m-%d").date()
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Reclassify pending
# ---------------------------------------------------------------------------
def reclassify_pending(db: Session, store_id: str) -> dict:
    rows = db.query(BankTransaction).filter(
        BankTransaction.store_id == store_id,
        BankTransaction.is_pending_review == True,
    ).all()
    classified = 0
    for tx in rows:
        tipo, clasif, conf, method = classify_transaction(tx.description, tx.amount)
        if tipo and conf >= 0.80:
            tx.tipo = tipo
            tx.clasificacion = clasif
            tx.classification_method = method
            tx.classification_confidence = conf
            tx.is_pending_review = False
            classified += 1
    db.commit()
    still_pending = get_pending_count(db, store_id)
    return {"classified": classified, "still_pending": still_pending}


# ---------------------------------------------------------------------------
# Fix inverted dates
# ---------------------------------------------------------------------------
def fix_inverted_dates(db: Session, store_id: str) -> dict:
    today = date.today()
    rows = db.query(BankTransaction).filter(
        BankTransaction.store_id == store_id,
        BankTransaction.date > today,
    ).all()
    fixed = 0
    for tx in rows:
        d = tx.date
        try:
            swapped = d.replace(month=d.day, day=d.month)
            if swapped <= today:
                tx.date = swapped
                fixed += 1
        except ValueError:
            pass
    db.commit()
    return {"fixed": fixed}


# ---------------------------------------------------------------------------
# Dashboard analytics
# ---------------------------------------------------------------------------
def get_dashboard(db: Session, store_id: str) -> dict:
    import pandas as pd

    rows = db.query(BankTransaction).filter(BankTransaction.store_id == store_id).all()
    if not rows:
        return {
            "balance_actual": 0, "ingresos_mes": 0, "gastos_mes": 0, "net_mes": 0,
            "ingresos_prev": 0, "gastos_prev": 0,
            "monthly_cashflow": [], "top_gastos_cat": [], "pivot_6m": [], "recent_transactions": [],
        }

    data = [_tx_to_dict(r) for r in rows]
    df = pd.DataFrame(data)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["month"] = df["date"].dt.to_period("M").astype(str)

    now = datetime.now()
    this_month = now.strftime("%Y-%m")
    last_month = (now - timedelta(days=32)).strftime("%Y-%m")

    df_this = df[df["month"] == this_month]
    df_last = df[df["month"] == last_month]

    bal_rows = df.dropna(subset=["running_balance"])
    balance_actual = float(bal_rows["running_balance"].iloc[-1]) if not bal_rows.empty else 0.0

    ingresos_mes = float(df_this[df_this["tipo"] == "Balance"]["amount"].sum())
    gastos_mes = float(df_this[df_this["tipo"] == "Gastos"]["amount"].abs().sum())
    net_mes = ingresos_mes - gastos_mes
    ingresos_prev = float(df_last[df_last["tipo"] == "Balance"]["amount"].sum())
    gastos_prev = float(df_last[df_last["tipo"] == "Gastos"]["amount"].abs().sum())

    monthly = (
        df.groupby(["month", "tipo"])["amount"]
        .sum()
        .reset_index()
        .sort_values("month")
    )
    monthly_cashflow = monthly.to_dict("records")

    gastos_cat = (
        df_this[df_this["tipo"] == "Gastos"]
        .groupby("clasificacion")["amount"]
        .sum()
        .abs()
        .sort_values(ascending=False)
        .head(10)
        .reset_index()
    )
    top_gastos_cat = gastos_cat.to_dict("records")

    today_month = now.strftime("%Y-%m")
    months_6 = sorted(m for m in df["month"].unique() if m <= today_month)[-6:]
    df_6m = df[df["month"].isin(months_6)]
    if not df_6m.empty:
        pivot = (
            df_6m.groupby(["clasificacion", "month"])["amount"]
            .sum()
            .abs()
            .unstack(fill_value=0)
            .reset_index()
        )
        pivot_6m = pivot.to_dict("records")
    else:
        pivot_6m = []

    recent = df.sort_values("date", ascending=False).head(50)[
        ["date", "description", "amount", "tipo", "clasificacion", "classification_method"]
    ]
    recent["date"] = recent["date"].astype(str)
    recent_transactions = recent.to_dict("records")

    return {
        "balance_actual": balance_actual,
        "ingresos_mes": ingresos_mes,
        "gastos_mes": gastos_mes,
        "net_mes": net_mes,
        "ingresos_prev": ingresos_prev,
        "gastos_prev": gastos_prev,
        "monthly_cashflow": monthly_cashflow,
        "top_gastos_cat": top_gastos_cat,
        "pivot_6m": pivot_6m,
        "recent_transactions": recent_transactions,
    }


# ---------------------------------------------------------------------------
# Insights analytics
# ---------------------------------------------------------------------------
def get_insights(db: Session, store_id: str) -> dict:
    import pandas as pd

    rows = db.query(BankTransaction).filter(BankTransaction.store_id == store_id).all()
    if not rows:
        return {
            "ingresos_tiktok": 0, "burn_rate": 0, "runway_months": 0,
            "margen_pct": 0, "cxc_cobrado": 0, "gap_tiktok_banco": 0,
            "alertas": [], "recurrentes": [],
        }

    data = [_tx_to_dict(r) for r in rows]
    df = pd.DataFrame(data)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["month"] = df["date"].dt.to_period("M").astype(str)

    now = datetime.now()
    this_month = now.strftime("%Y-%m")
    last_month = (now - timedelta(days=32)).strftime("%Y-%m")

    try:
        from app.services.analytics_service import get_overview_metrics
        overview = get_overview_metrics(db, store_id)
        ingresos_tiktok = float(overview.get("TITKOKGMVOrderAmount", 0))
    except Exception:
        ingresos_tiktok = 0.0

    df_gastos = df[df["tipo"] == "Gastos"]
    total_gastos = float(df_gastos["amount"].abs().sum())

    gastos_por_mes = df_gastos.groupby("month")["amount"].sum().abs()
    burn_rate = float(gastos_por_mes.mean()) if not gastos_por_mes.empty else 0.0

    bal_rows = df.dropna(subset=["running_balance"])
    balance_actual = float(bal_rows["running_balance"].iloc[-1]) if not bal_rows.empty else 0.0
    runway_months = (balance_actual / burn_rate) if burn_rate > 0 else 0.0

    ingresos_banco = float(df[df["tipo"] == "Balance"]["amount"].sum())
    margen_pct = ((ingresos_banco - total_gastos) / ingresos_banco * 100) if ingresos_banco > 0 else 0.0

    cxc_cobrado = float(df[df["clasificacion"] == "Cuenta por cobrar"]["amount"].sum())
    gap_tiktok_banco = ingresos_tiktok - cxc_cobrado

    gastos_pivot = (
        df_gastos[df_gastos["month"].isin([this_month, last_month])]
        .groupby(["clasificacion", "month"])["amount"]
        .sum()
        .abs()
        .unstack(fill_value=0)
    )
    alertas = []
    for clasif in gastos_pivot.index:
        curr = float(gastos_pivot.at[clasif, this_month]) if this_month in gastos_pivot.columns else 0.0
        prev = float(gastos_pivot.at[clasif, last_month]) if last_month in gastos_pivot.columns else 0.0
        if prev > 0:
            pct = (curr - prev) / prev * 100
            if abs(pct) > 50:
                alertas.append({"clasificacion": clasif, "mes_actual": curr, "mes_anterior": prev, "cambio_pct": round(pct, 1)})
    alertas.sort(key=lambda x: abs(x["cambio_pct"]), reverse=True)

    recurrentes = (
        df_gastos.groupby("clasificacion")
        .agg(
            n_transacciones=("amount", "count"),
            total=("amount", lambda x: float(x.abs().sum())),
            promedio=("amount", lambda x: float(x.abs().mean())),
        )
        .reset_index()
        .sort_values("n_transacciones", ascending=False)
        .head(15)
    )
    recurrentes_list = recurrentes.to_dict("records")

    return {
        "ingresos_tiktok": ingresos_tiktok,
        "burn_rate": burn_rate,
        "runway_months": runway_months,
        "margen_pct": margen_pct,
        "cxc_cobrado": cxc_cobrado,
        "gap_tiktok_banco": gap_tiktok_banco,
        "alertas": alertas,
        "recurrentes": recurrentes_list,
    }


# ---------------------------------------------------------------------------
# Classifications catalog (stored in store.settings)
# ---------------------------------------------------------------------------
def get_classifications(db: Session, store_id: str) -> dict:
    store = db.query(Store).filter(Store.id == store_id).first()
    if store and store.settings and "finance_classifications" in store.settings:
        return store.settings["finance_classifications"]
    return {k: list(v) for k, v in DEFAULT_CLASSIFICATIONS.items()}


def save_classifications(db: Session, store_id: str, data: dict) -> dict:
    store = db.query(Store).filter(Store.id == store_id).first()
    if not store:
        return data
    settings = dict(store.settings or {})
    settings["finance_classifications"] = data
    store.settings = settings
    db.commit()
    return data
