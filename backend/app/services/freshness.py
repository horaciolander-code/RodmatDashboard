"""
Data-freshness gate.

Dos condiciones, y la segunda se anadio el 21-oct-2026:

  1. Hay al menos una fila de `import_history` de HOY (hora local del store).
  2. Han llegado los ficheros de TODOS los canales que venden estos dias.

La segunda existe porque con un solo canal la primera bastaba, y con TikTok
y TEMU ya no: el informe salia con el primer fichero que llegaba, sin el
otro canal, y el dia quedaba marcado como enviado — asi que la cifra
completa no se reenviaba nunca. Los numeros de los emails no cuadraban ni
con TikTok ni con la suma de los dos.

The scheduled jobs (daily report + agents) use this gate to decide whether
to fire or skip+alert. When an upload eventually lands,
the imports endpoint calls `trigger_pending_jobs` (in scheduled_jobs.py)
which uses this same gate to decide whether to fire what was previously
skipped.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional, Sequence

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover — Python <3.9 not supported on Railway
    from backports.zoneinfo import ZoneInfo  # type: ignore

from sqlalchemy.orm import Session

from app.models.import_history import ImportHistory


# Todo lo que trae PEDIDOS. Si falta un tipo aqui, una carga de ese canal
# no cuenta como dato fresco: el informe se queda en skipped_stale y el
# disparador de recuperacion tampoco lo rescata.
#
# Paso de verdad: el 17-oct-2026 solo se subio TEMU. Como "temu_orders"
# no estaba en esta lista, ese dia NO salio el informe diario.
#
# El statement de TikTok NO va aqui a proposito: es dinero, no pedidos.
# Que llegue el extracto no significa que las ventas esten cargadas.
DEFAULT_TYPES: tuple[str, ...] = (
    "tiktok", "amazon", "walmart", "temu", "temu_orders",
)


# Cuantos dias de ventas se miran para decidir que un canal "esta activo" y
# por tanto su fichero se espera cada dia. Walmart lleva desde junio sin una
# venta: exigir su fichero seria bloquear el informe para siempre. Es la
# misma ventana que usa el aviso por canal del propio informe.
DIAS_ACTIVIDAD = 14


def _canales_pendientes_hoy(db: Session, store_id: str,
                            utc_day_start: datetime,
                            now: datetime) -> list:
    """Que canales activos no han subido su fichero de pedidos hoy.

    Derivado de los datos, no de una lista fija: un canal nuevo empieza a
    exigirse en cuanto vende, y uno que se apaga deja de exigirse solo.
    """
    from sqlalchemy import text as _t

    from app.services.frescura_canales import canales_pendientes

    desde = (now - timedelta(days=DIAS_ACTIVIDAD)).replace(tzinfo=None)
    activos = [r[0] for r in db.execute(_t(
        "SELECT DISTINCT lower(platform) FROM sales_orders "
        "WHERE store_id = :sid AND order_date >= :desde"
    ), {"sid": store_id, "desde": desde}).fetchall()]

    # `imported_at` es timestamp SIN zona y guarda hora UTC, asi que el
    # parametro va naive: comparar naive con naive es exacto. Con un
    # datetime aware el resultado dependeria del TimeZone de la sesion.
    tipos_hoy = [r[0] for r in db.execute(_t(
        "SELECT DISTINCT import_type FROM import_history "
        "WHERE store_id = :sid AND imported_at >= :desde"
    ), {"sid": store_id, "desde": utc_day_start.replace(tzinfo=None)}).fetchall()]

    return canales_pendientes(activos, tipos_hoy)


@dataclass(frozen=True)
class FreshnessResult:
    is_fresh: bool
    latest_import_at: Optional[datetime]
    latest_import_type: Optional[str]
    reason: str   # 'fresh' | 'incomplete' | 'stale' | 'no_imports_ever'
    detail: str   # human-readable detail (safe to put in an email body)
    # Canales que venden estos dias y cuyo fichero no ha llegado hoy.
    # Con 'incomplete' va lleno; con el resto va vacio.
    canales_pendientes: tuple = ()

    def __bool__(self) -> bool:
        return self.is_fresh


def _store_tz(store) -> ZoneInfo:
    """Resolve the store's timezone; fallback to America/New_York (EDT/EST)."""
    if store and getattr(store, "timezone", None):
        try:
            return ZoneInfo(store.timezone)
        except Exception:
            pass
    return ZoneInfo("America/New_York")


def canales_sin_cargar_hoy(db: Session, store_id: str, *, store=None,
                           now: Optional[datetime] = None) -> list:
    """Canales activos cuyo fichero no ha llegado hoy, para la cabecera del
    informe.

    Misma cuenta que hace el gate, expuesta aparte: si un informe llega a
    enviarse faltando un canal (pasada de rescate, o /send-now?force=true),
    tiene que decirlo en la cara. Un informe que calla lo que le falta es
    peor que no tener informe.
    """
    if now is None:
        now = datetime.now(timezone.utc)
    elif now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    if store is None:
        from app.models.store import Store
        store = db.query(Store).filter(Store.id == store_id).first()
    tz = _store_tz(store)
    inicio = datetime.combine(
        now.astimezone(tz).date(), datetime.min.time(), tzinfo=tz
    ).astimezone(timezone.utc)
    return _canales_pendientes_hoy(db, store_id, inicio, now)


def check_data_freshness(
    db: Session,
    store_id: str,
    *,
    store=None,
    import_types: Sequence[str] = DEFAULT_TYPES,
    now: Optional[datetime] = None,
    exigir_todos_los_canales: bool = True,
) -> FreshnessResult:
    """Return a FreshnessResult for this store.

    Fresh <=> at least one import_history row exists with imported_at on
    "today" in the store's local timezone.
    """
    if now is None:
        now = datetime.now(timezone.utc)
    elif now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)

    if store is None:
        from app.models.store import Store
        store = db.query(Store).filter(Store.id == store_id).first()

    tz = _store_tz(store)
    local_today = now.astimezone(tz).date()
    utc_day_start = datetime.combine(
        local_today, datetime.min.time(), tzinfo=tz
    ).astimezone(timezone.utc)

    q = (db.query(ImportHistory)
         .filter(ImportHistory.store_id == store_id)
         .filter(ImportHistory.imported_at >= utc_day_start)
         .order_by(ImportHistory.imported_at.desc()))
    if import_types:
        q = q.filter(ImportHistory.import_type.in_(list(import_types)))
    latest_today = q.first()

    if latest_today is not None:
        # Que haya llegado UN fichero no significa que esten todos. Con
        # TikTok y TEMU, enviar al primero que llega saca el email con la
        # mitad de las ventas y marca el dia como enviado: la cifra buena
        # ya no se reenvia nunca.
        pendientes = (_canales_pendientes_hoy(db, store_id, utc_day_start, now)
                      if exigir_todos_los_canales else [])
        if pendientes:
            from app.services.frescura_canales import texto_canales
            return FreshnessResult(
                is_fresh=False,
                latest_import_at=latest_today.imported_at,
                latest_import_type=latest_today.import_type,
                reason="incomplete",
                detail=(
                    f"Falta la carga de hoy de {texto_canales(pendientes)}. "
                    f"Ya ha llegado '{latest_today.import_type}' a las "
                    f"{latest_today.imported_at.isoformat()}, pero el informe "
                    f"saldria sin ese canal. En cuanto subas su fichero se "
                    f"envia solo."
                ),
                canales_pendientes=tuple(pendientes),
            )
        return FreshnessResult(
            is_fresh=True,
            latest_import_at=latest_today.imported_at,
            latest_import_type=latest_today.import_type,
            reason="fresh",
            detail=(
                f"Import '{latest_today.import_type}' at "
                f"{latest_today.imported_at.isoformat()} "
                f"(today {local_today.isoformat()} in {tz.key})."
            ),
        )

    # Not fresh -- surface the most recent import overall for the alert.
    q_any = (db.query(ImportHistory)
             .filter(ImportHistory.store_id == store_id)
             .order_by(ImportHistory.imported_at.desc()))
    if import_types:
        q_any = q_any.filter(ImportHistory.import_type.in_(list(import_types)))
    last_overall = q_any.first()

    if last_overall is None:
        return FreshnessResult(
            is_fresh=False,
            latest_import_at=None,
            latest_import_type=None,
            reason="no_imports_ever",
            detail=(f"No imports ever recorded for store "
                    f"(types={list(import_types)})."),
        )

    age_h = (now - last_overall.imported_at).total_seconds() / 3600.0
    return FreshnessResult(
        is_fresh=False,
        latest_import_at=last_overall.imported_at,
        latest_import_type=last_overall.import_type,
        reason="stale",
        detail=(
            f"Latest import is '{last_overall.import_type}' at "
            f"{last_overall.imported_at.isoformat()} ({age_h:.1f}h ago). "
            f"Today in {tz.key} is {local_today.isoformat()}, "
            f"no import yet."
        ),
    )
