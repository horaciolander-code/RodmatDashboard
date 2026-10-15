"""
Scheduler / cron / post-import orchestration with freshness gates.

This module wraps the report and agent business logic that lives in
  - app.services.daily_report_service
  - app.services.agents.<name>_agent
and decides *when* / *whether* to fire them, by consulting
`check_data_freshness` and the run-audit tables (`report_logs`, `agent_runs`).

Three entry points:
  - run_scheduled_reports(db)  - called by main.py scheduler at 12:00 UTC
                                  and by POST /api/reports/run-all
  - run_scheduled_agents(db)   - called by main.py scheduler at 11:00 UTC
                                  and by POST /api/agents/run-all
  - trigger_pending_jobs(sid)  - called as a background task by
                                  POST /api/import/orders and /amazon
                                  after a successful import, to fire
                                  whatever was skipped earlier today.

All three log structured run records and (where applicable) send a single
consolidated freshness alert per store to OPERATIONS_EMAIL.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy.orm import Session

from app.models.store import Store
from app.models.report_log import ReportLog
from app.models.agent_run import AgentRun
from app.services.freshness import check_data_freshness
from app.services.alert_service import send_freshness_alert

logger = logging.getLogger("rodmat.scheduled")


AGENT_MODULES = {
    "prism":     "app.services.agents.prism_agent",
    "haiku":     "app.services.agents.haiku_agent",
    "faraway":   "app.services.agents.faraway_agent",
    "mesmerize": "app.services.agents.mesmerize_agent",
    "timeless":  "app.services.agents.timeless_agent",
    "khamrah":   "app.services.agents.khamrah_agent",
}


# ── audit helpers ────────────────────────────────────────────────────────────

def _today_start_utc() -> datetime:
    return datetime.combine(
        datetime.now(timezone.utc).date(),
        datetime.min.time(),
        tzinfo=timezone.utc,
    )


def _today_has_sent_report(db: Session, store_id: str,
                           brand_id: str | None = None) -> bool:
    """Idempotencia POR MARCA (report_logs.brand_id creada 2026-09-20).

    El envio global (brand_id NULL) y el de cada marca se cuentan por separado.
    Sin esta separacion el diario global marcaba el dia como 'ya enviado' y el de
    Atralia no salia nunca.
    """
    start_naive = _today_start_utc().replace(tzinfo=None)  # ReportLog.sent_at is naive
    q = db.query(ReportLog).filter(
        ReportLog.store_id == store_id,
        ReportLog.status == "sent",
        ReportLog.sent_at >= start_naive,
    )
    q = q.filter(ReportLog.brand_id.is_(None) if brand_id is None
                 else ReportLog.brand_id == brand_id)
    return q.first() is not None


def _today_has_agent_run(db: Session, store_id: str, agent_name: str,
                         status: str = "sent", brand_id: str | None = None) -> bool:
    q = db.query(AgentRun).filter(
        AgentRun.store_id == store_id,
        AgentRun.agent_name == agent_name,
        AgentRun.status == status,
        AgentRun.run_at >= _today_start_utc(),
    )
    # Idempotencia por marca: el envio global (brand_id NULL) y el de cada marca
    # son envios distintos y no deben bloquearse entre si.
    q = q.filter(AgentRun.brand_id.is_(None) if brand_id is None
                 else AgentRun.brand_id == brand_id)
    return q.first() is not None


def _brands_with_own_recipients(db: Session, store_id: str) -> list:
    """Marcas con destinatarios propios configurados (brands.email_sender).

    SOLO estas reciben envio brand-scoped. Una marca sin lista propia no recibe
    nada por separado; sus datos siguen saliendo en el envio global de la store
    igual que hasta ahora. El cambio es puramente aditivo.
    """
    from sqlalchemy import text as _text
    rows = db.execute(_text(
        "SELECT id, slug FROM brands "
        "WHERE store_id = :sid AND is_active = true "
        "AND email_sender IS NOT NULL AND btrim(email_sender) <> ''"
    ), {"sid": store_id}).fetchall()
    return [(r[0], r[1]) for r in rows]


def _scheduler_already_attempted_report_today(db: Session, store_id: str) -> bool:
    """True if the scheduler has logged ANY report attempt today (sent,
    failed, or skipped_stale). Used by trigger_pending_jobs to decide
    whether to act as catch-up vs wait for the scheduler."""
    start_naive = _today_start_utc().replace(tzinfo=None)
    return db.query(ReportLog).filter(
        ReportLog.store_id == store_id,
        ReportLog.sent_at >= start_naive,
    ).first() is not None


def _scheduler_already_attempted_agent_today(db: Session, store_id: str,
                                              agent_name: str) -> bool:
    """True if the scheduler has logged ANY attempt today for this agent."""
    return db.query(AgentRun).filter(
        AgentRun.store_id == store_id,
        AgentRun.agent_name == agent_name,
        AgentRun.run_at >= _today_start_utc(),
    ).first() is not None


def _log_report(db: Session, store_id: str, status: str,
                recipients: str | None = None,
                brand_id: str | None = None) -> None:
    log = ReportLog(store_id=store_id, status=status, recipients=recipients,
                    brand_id=brand_id)
    db.add(log)
    db.commit()


def _log_agent_run(db: Session, store_id: str, agent_name: str, status: str,
                   reason: str | None = None,
                   latest_import_at: Optional[datetime] = None,
                   brand_id: str | None = None) -> None:
    rec = AgentRun(store_id=store_id, agent_name=agent_name, status=status,
                   reason=reason, latest_import_at=latest_import_at,
                   brand_id=brand_id)
    db.add(rec)
    db.commit()


def _is_agent_day(agent_name: str, today: Optional[datetime] = None) -> bool:
    """Mirror of each agent's internal day check, used to decide whether a
    skip should be recorded as 'skipped_stale' (= it WAS its day) vs
    'skipped_not_its_day' (= not due today)."""
    if today is None:
        today = datetime.now()
    wd = today.weekday()
    if agent_name == "prism":     return wd == 0                      # Mon
    if agent_name == "haiku":     return wd == 2                      # Wed
    if agent_name == "faraway":   return wd == 4                      # Fri
    if agent_name == "mesmerize": return wd == 0 and today.day <= 7   # 1st Mon
    if agent_name == "timeless":  return today.day == 1               # Día 1 de cada mes
    if agent_name == "khamrah":   return wd == 0                      # Lunes cierre semanal
    return False


# ── scheduled report (12:00 UTC) ─────────────────────────────────────────────

def run_scheduled_reports(db: Session) -> dict:
    """Run daily reports for every report-enabled store, gated by freshness.

    Behavior per store:
      - if not report_enabled            -> skipped_disabled
      - elif not fresh                   -> log skipped_stale + queue alert
      - else                             -> send report + log sent

    Returns dict mapping store_name -> status string.
    Sends one consolidated stale-data alert per store at the end.
    """
    from app.services.daily_report_service import run_store_report

    stores = db.query(Store).all()
    results: dict[str, str] = {}
    skipped_by_store: dict[str, list[dict]] = {}
    latest_by_store: dict[str, Optional[datetime]] = {}

    for store in stores:
        settings = store.settings or {}
        if not settings.get("report_enabled", False):
            results[store.name] = "skipped_disabled"
            continue

        # Idempotency: if the daily report was already sent today (e.g. via
        # post-import catch-up earlier in the day), don't fire again.
        if _today_has_sent_report(db, store.id):
            results[store.name] = "skipped_already_sent_today"
            logger.info("[scheduled_reports] %s skipped (already sent today)",
                        store.name)
            continue

        f = check_data_freshness(db, store.id, store=store)
        if not f.is_fresh:
            _log_report(db, store.id, status="skipped_stale")
            results[store.name] = "skipped_stale"
            skipped_by_store.setdefault(store.name, []).append({
                "kind": "daily_report",
                "name": "Daily Report",
                "reason": f.detail,
            })
            latest_by_store[store.name] = f.latest_import_at
            logger.info("[scheduled_reports] %s skipped (stale): %s",
                        store.name, f.reason)
            continue

        try:
            ok = run_store_report(db, store.id)
        except Exception as exc:
            logger.exception("[scheduled_reports] %s failed: %s", store.name, exc)
            _log_report(db, store.id, status="failed")
            results[store.name] = "failed"
            continue

        if ok:
            recips = ", ".join(settings.get("report_recipients") or [])
            _log_report(db, store.id, status="sent", recipients=recips)
            results[store.name] = "sent"
        else:
            _log_report(db, store.id, status="failed")
            results[store.name] = "failed"

        # ── 2026-09-20 · informe diario POR MARCA ────────────────────────────
        # Aditivo: el envio global de arriba no cambia. Cada marca con
        # destinatarios propios en brands.email_sender recibe ademas su propio
        # informe, calculado solo con sus ordenes. Marca sin destinatarios -> nada.
        for _bid, _bslug in _brands_with_own_recipients(db, store.id):
            if _today_has_sent_report(db, store.id, brand_id=_bid):
                results[f"{store.name}:{_bslug}"] = "skipped_already_sent_today"
                continue
            try:
                _bok = run_store_report(db, store.id, brand_slug=_bslug)
            except Exception as _bexc:
                logger.exception("[scheduled_reports] %s/%s failed: %s",
                                 store.name, _bslug, _bexc)
                _log_report(db, store.id, status="failed", brand_id=_bid)
                results[f"{store.name}:{_bslug}"] = "failed"
                continue
            _log_report(db, store.id, status="sent" if _bok else "failed",
                        brand_id=_bid)
            results[f"{store.name}:{_bslug}"] = "sent" if _bok else "failed"

    for store_name, items in skipped_by_store.items():
        send_freshness_alert(
            store_name=store_name,
            skipped=items,
            latest_import_at=latest_by_store.get(store_name),
        )
    return results


# ── scheduled agents (11:00 UTC) ─────────────────────────────────────────────

def run_scheduled_agents(db: Session) -> dict:
    """Run all four agents for every store, gated by freshness.

    Per (store, agent):
      - if stale AND today is the agent's day -> log skipped_stale + alert
      - if stale AND today is NOT its day     -> log skipped_not_its_day (no alert)
      - if fresh                              -> call agent.run(), log result

    Sends one consolidated stale-data alert per store at the end.
    """
    import importlib

    stores = db.query(Store).all()
    results: dict[str, dict] = {}
    skipped_by_store: dict[str, list[dict]] = {}
    latest_by_store: dict[str, Optional[datetime]] = {}

    for store in stores:
        store_results: dict[str, str] = {}
        f = check_data_freshness(db, store.id, store=store)

        for agent_name, mod_path in AGENT_MODULES.items():
            try:
                # Idempotency: already sent today via earlier catch-up? Skip.
                if _today_has_agent_run(db, store.id, agent_name, status="sent"):
                    store_results[agent_name] = "skipped_already_sent_today"
                    continue

                if not f.is_fresh:
                    if _is_agent_day(agent_name):
                        _log_agent_run(
                            db, store.id, agent_name,
                            status="skipped_stale",
                            reason=f.detail,
                            latest_import_at=f.latest_import_at,
                        )
                        skipped_by_store.setdefault(store.name, []).append({
                            "kind": "agent",
                            "name": agent_name.upper(),
                            "reason": f.detail,
                        })
                        latest_by_store[store.name] = f.latest_import_at
                        store_results[agent_name] = "skipped_stale"
                    else:
                        # don't pollute the audit table with daily noise
                        store_results[agent_name] = "skipped_not_its_day"
                    continue

                agent = importlib.import_module(mod_path)
                ran = agent.run(db, store.id)

                # ── Envio adicional por marca ────────────────────────────────
                # Solo marcas con destinatarios propios. El envio global de
                # arriba NO se toca: esto suma, no sustituye.
                for _bid, _bslug in _brands_with_own_recipients(db, store.id):
                    if _today_has_agent_run(db, store.id, agent_name,
                                            status="sent", brand_id=_bid):
                        continue
                    try:
                        if agent.run(db, store.id, brand_slug=_bslug):
                            _log_agent_run(db, store.id, agent_name, status="sent",
                                           latest_import_at=f.latest_import_at,
                                           brand_id=_bid)
                            store_results[f"{agent_name}:{_bslug}"] = "sent"
                    except Exception as _be:
                        logger.exception("Agent %s brand %s failed: %s",
                                         agent_name, _bslug, _be)
                        _log_agent_run(db, store.id, agent_name, status="error",
                                       reason=str(_be)[:500], brand_id=_bid)
                        store_results[f"{agent_name}:{_bslug}"] = "error"

                if ran:
                    _log_agent_run(db, store.id, agent_name, status="sent",
                                   latest_import_at=f.latest_import_at)
                    store_results[agent_name] = "sent"
                else:
                    # agent.run() returned False -- either not its day, disabled,
                    # or no recipients. Don't audit every daily no-op.
                    if _is_agent_day(agent_name):
                        _log_agent_run(
                            db, store.id, agent_name,
                            status="skipped_other",
                            reason=("agent.run() returned False despite fresh data "
                                    "(disabled / no recipients / agent-internal gate)"),
                            latest_import_at=f.latest_import_at,
                        )
                    store_results[agent_name] = "skipped_other"
            except Exception as exc:
                logger.exception("[scheduled_agents] %s %s failed: %s",
                                 store.name, agent_name, exc)
                _log_agent_run(db, store.id, agent_name, status="error",
                               reason=str(exc)[:500],
                               latest_import_at=f.latest_import_at)
                store_results[agent_name] = f"error: {type(exc).__name__}"

        results[store.id] = store_results

    for store_name, items in skipped_by_store.items():
        send_freshness_alert(
            store_name=store_name,
            skipped=items,
            latest_import_at=latest_by_store.get(store_name),
        )
    return results


# ── post-import trigger ──────────────────────────────────────────────────────

def trigger_pending_jobs(store_id: str) -> dict:
    """Catch-up trigger: called as a FastAPI background task after a
    successful import. Only fires reports/agents that the scheduler
    already ATTEMPTED earlier today and SKIPPED due to stale data
    (skipped_stale). Does NOT pre-fire — if the scheduler hasn't run
    yet today (upload arrived BEFORE the scheduled hour), this function
    waits for the scheduler to do its job at the normal time.

    Behavior matrix:
      - already_sent_today      -> skip
      - scheduler not yet run   -> skip ('waiting_for_scheduler')
      - scheduler skipped stale + data now fresh -> CATCH UP, send
      - scheduler skipped stale + data still stale -> skip ('still_stale')
      - agent not its day       -> skip ('not_its_day')
    """
    import importlib
    from app.database import SessionLocal
    from app.services.daily_report_service import run_store_report

    out: dict[str, str] = {}
    db = SessionLocal()
    try:
        store = db.query(Store).filter(Store.id == store_id).first()
        if not store:
            return {"error": "store_not_found"}

        settings = store.settings or {}
        f = check_data_freshness(db, store_id, store=store)

        # ── 1) Daily report ───────────────────────────────────────────
        if not settings.get("report_enabled", False):
            out["daily_report"] = "disabled"
        elif _today_has_sent_report(db, store_id):
            out["daily_report"] = "already_sent_today"
        elif not _scheduler_already_attempted_report_today(db, store_id):
            # Scheduler hasn't run yet today (upload arrived BEFORE its
            # scheduled hour). Let the scheduler handle it at the normal
            # time — do NOT pre-fire. User's rule: upload only triggers
            # when the upload arrives AFTER the scheduled hour.
            out["daily_report"] = "waiting_for_scheduler"
        elif not f.is_fresh:
            out["daily_report"] = "still_stale"
        else:
            try:
                ok = run_store_report(db, store_id)
                if ok:
                    recips = ", ".join(settings.get("report_recipients") or [])
                    _log_report(db, store_id, status="sent", recipients=recips)
                    out["daily_report"] = "sent_catchup"
                else:
                    _log_report(db, store_id, status="failed")
                    out["daily_report"] = "failed"
            except Exception as exc:
                logger.exception("[trigger] daily report %s failed: %s",
                                 store_id[:8], exc)
                _log_report(db, store_id, status="failed")
                out["daily_report"] = f"error: {type(exc).__name__}"

            # 2026-09-20 · catch-up del diario POR MARCA (mismas reglas que el global)
            for _bid, _bslug in _brands_with_own_recipients(db, store_id):
                _k = f"daily_report:{_bslug}"
                if _today_has_sent_report(db, store_id, brand_id=_bid):
                    out[_k] = "already_sent_today"
                    continue
                try:
                    _bok = run_store_report(db, store_id, brand_slug=_bslug)
                    _log_report(db, store_id, status="sent" if _bok else "failed",
                                brand_id=_bid)
                    out[_k] = "sent_catchup" if _bok else "failed"
                except Exception as _bexc:
                    logger.exception("[trigger] daily %s/%s failed: %s",
                                     store_id[:8], _bslug, _bexc)
                    _log_report(db, store_id, status="failed", brand_id=_bid)
                    out[_k] = f"error: {type(_bexc).__name__}"

        # ── 2) Agents — same catch-up-only semantics, per agent ───────
        for agent_name, mod_path in AGENT_MODULES.items():
            if not _is_agent_day(agent_name):
                out[agent_name] = "not_its_day"
                continue
            if _today_has_agent_run(db, store_id, agent_name, status="sent"):
                out[agent_name] = "already_sent_today"
                continue
            if not _scheduler_already_attempted_agent_today(db, store_id, agent_name):
                out[agent_name] = "waiting_for_scheduler"
                continue
            if not f.is_fresh:
                out[agent_name] = "still_stale"
                continue
            try:
                agent = importlib.import_module(mod_path)
                ran = agent.run(db, store_id)
                if ran:
                    _log_agent_run(db, store_id, agent_name, status="sent",
                                   latest_import_at=f.latest_import_at)
                    out[agent_name] = "sent_catchup"
                else:
                    _log_agent_run(
                        db, store_id, agent_name,
                        status="skipped_other",
                        reason="agent.run() returned False (disabled or internal gate)",
                        latest_import_at=f.latest_import_at,
                    )
                    out[agent_name] = "skipped_other"
            except Exception as exc:
                logger.exception("[trigger] agent %s %s failed: %s",
                                 store_id[:8], agent_name, exc)
                _log_agent_run(db, store_id, agent_name, status="error",
                               reason=str(exc)[:500],
                               latest_import_at=f.latest_import_at)
                out[agent_name] = f"error: {type(exc).__name__}"

        # ── 3) SKU vendiendo sin descontar ────────────────────────────
        # Va al final y entre try: un fallo avisando NO puede tumbar el
        # informe diario ni los agentes, que es lo que de verdad importa.
        try:
            out["aviso_skus"] = avisar_skus_sin_asignar(db, store_id, store=store)
        except Exception as exc:
            logger.exception("[trigger] aviso SKU %s failed: %s", store_id[:8], exc)
            out["aviso_skus"] = f"error: {type(exc).__name__}"
    finally:
        db.close()
    logger.info("[trigger] store=%s result=%s", store_id[:8], out)
    return out


# ═════════════════════════════════════════════════════════════════════════
#  AVISO · SKU que aparecen en ventas y NO descuentan inventario
#
#  El banner de Gestion ya los enseña, pero hay que entrar a mirarlo. Esto
#  los manda por correo en cuanto aparecen, que es cuando se pueden
#  arreglar antes de que se acumulen unidades mal contadas.
#
#  Solo avisa de los NUEVOS. Las cargas diarias son cinco (tres trozos de
#  TikTok, el statement y TEMU): avisando en cada una serian cinco correos
#  identicos al dia, y un aviso que se repite se deja de leer.
# ═════════════════════════════════════════════════════════════════════════

def _destinatarios_aviso(store) -> list:
    """A quien se avisa. SOLO `settings.alert_recipients`, sin respaldo.

    A proposito no cae a `report_recipients`: este aviso es operativo y va a
    quien tenga que asignar los SKU, no a toda la lista del informe diario.
    Sin configurar, no se manda nada — mejor callado que mandando a quien no
    lo pidio.
    """
    dest = (store.settings or {}).get("alert_recipients") or []
    if isinstance(dest, str):
        dest = [dest]
    return [d.strip() for d in dest if d and d.strip()]


def _html_aviso_skus(sin_asignar: list, nuevos: list, tienda: str) -> str:
    filas = []
    for r in sorted(sin_asignar, key=lambda x: -(x.get("total_qty") or 0)):
        sku = str(r.get("seller_sku") or "")
        es_nuevo = sku in nuevos
        marca = ('<span style="background:#FCEBEB;color:#A32D2D;font-size:11px;'
                 'padding:2px 6px;border-radius:4px;margin-left:6px;">NUEVO</span>'
                 if es_nuevo else "")
        filas.append(
            f'<tr style="border-bottom:1px solid #e1e0d9;">'
            f'<td style="padding:8px 10px;font-family:monospace;font-size:13px;">{sku}{marca}</td>'
            f'<td style="padding:8px 10px;font-size:13px;color:#52514e;">'
            f'{str(r.get("product_name") or "")[:70]}</td>'
            f'<td style="padding:8px 10px;text-align:right;font-size:13px;">{r.get("order_count") or 0}</td>'
            f'<td style="padding:8px 10px;text-align:right;font-size:13px;font-weight:600;">'
            f'{int(r.get("total_qty") or 0)}</td></tr>')

    unidades = sum(int(r.get("total_qty") or 0) for r in sin_asignar)
    return f"""
    <div style="font-family:-apple-system,Segoe UI,Arial,sans-serif;max-width:720px;margin:0 auto;">
      <div style="background:#0b1020;color:#fff;padding:18px 22px;border-radius:10px 10px 0 0;">
        <div style="font-size:19px;font-weight:600;">{len(sin_asignar)} SKU sin asignar en {tienda}</div>
        <div style="font-size:13px;color:#9aa4bf;margin-top:4px;">
          No estan descontando inventario. Son {unidades:,} unidades vendidas que el
          sistema no ha restado del stock.
        </div>
      </div>
      <div style="border:1px solid #e1e0d9;border-top:0;border-radius:0 0 10px 10px;padding:18px 22px;">
        <p style="font-size:14px;color:#52514e;margin:0 0 14px;">
          {len(nuevos)} de ellos han aparecido hoy. Mientras no tengan combo asignado,
          su stock queda mas alto de lo real y el margen sale mal.
        </p>
        <table style="width:100%;border-collapse:collapse;">
          <tr style="background:#f1efe8;">
            <th style="text-align:left;padding:8px 10px;font-size:12px;">SKU</th>
            <th style="text-align:left;padding:8px 10px;font-size:12px;">Producto segun la plataforma</th>
            <th style="text-align:right;padding:8px 10px;font-size:12px;">Pedidos</th>
            <th style="text-align:right;padding:8px 10px;font-size:12px;">Unidades</th>
          </tr>
          {''.join(filas)}
        </table>
        <p style="margin:18px 0 0;">
          <a href="https://dashboard.rodmatcenter.com"
             style="background:#2a78d6;color:#fff;text-decoration:none;padding:10px 18px;
                    border-radius:8px;font-size:14px;display:inline-block;">
            Asignarlos en Gestion &gt; Gestion Combos
          </a>
        </p>
      </div>
    </div>"""


def avisar_skus_sin_asignar(db: Session, store_id: str, store=None) -> str:
    """Manda un correo si hay SKU vendiendo sin descontar. Devuelve que hizo.

    Guarda los ya avisados en `settings.skus_avisados` para no repetirse. Si
    uno se asigna y vuelve a aparecer mas adelante, se avisa otra vez: sale
    de la lista en cuanto deja de estar sin asignar.
    """
    from app.services.agents._base import send_email
    from app.services.stock_calculator import get_unknown_combo_skus

    store = store or db.query(Store).filter(Store.id == store_id).first()
    if not store:
        return "store_not_found"

    try:
        sin_asignar = get_unknown_combo_skus(db, store_id) or []
    except Exception:
        logger.exception("aviso SKU: no se pudo calcular la lista")
        return "error_calculando"

    actuales = {str(r.get("seller_sku") or "").strip() for r in sin_asignar}
    actuales.discard("")

    settings = dict(store.settings or {})
    avisados = {str(s).strip() for s in (settings.get("skus_avisados") or [])}

    # Los que ya se arreglaron salen de la lista: si reaparecen, vuelven a avisar.
    avisados &= actuales
    nuevos = sorted(actuales - avisados)

    if not nuevos:
        if avisados != {str(s).strip() for s in (settings.get("skus_avisados") or [])}:
            settings["skus_avisados"] = sorted(avisados)
            store.settings = settings
            db.commit()
        return "sin_novedades"

    destinatarios = _destinatarios_aviso(store)
    if not destinatarios:
        # Sin configurar NO se marcan como avisados: el dia que se configure,
        # el primer correo tiene que traer todo lo que hay pendiente.
        return "sin_destinatarios"

    asunto = (f"{len(nuevos)} SKU nuevo{'s' if len(nuevos) != 1 else ''} sin asignar "
              f"— no estan descontando stock")
    html = _html_aviso_skus(sin_asignar, nuevos, store.name or "la tienda")
    enviado = send_email(html, asunto, destinatarios)

    # La lista se actualiza igual aunque el correo falle: si no, cada import
    # reintentaria y acabariamos mandando el mismo aviso en bucle.
    settings["skus_avisados"] = sorted(actuales)
    store.settings = settings
    db.commit()

    logger.info("aviso SKU sin asignar: %d nuevos, enviado=%s", len(nuevos), enviado)
    return f"avisados_{len(nuevos)}" if enviado else "email_fallo"
