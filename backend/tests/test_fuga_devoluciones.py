"""Smoke test de la fuga de inventario por devoluciones post-envío.

Una orden Completed/Shipped con Cancelation/Return Type salió físicamente del
almacén: TIENE que descontar stock. El filtro anterior la descartaba y eso
fugaba 620 u en el catálogo 2026 (Far Away Original 101, Imari Skin Softener 85...).

Sin BD: se stubean sqlalchemy y app.models para poder importar el módulo suelto.
    python backend/tests/test_fuga_devoluciones.py
"""
import sys, types, pathlib

_BACKEND = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_BACKEND))

for _name in ("sqlalchemy", "sqlalchemy.orm", "app.models.sales", "app.models.combo",
              "app.models.product", "app.models.inventory"):
    if _name not in sys.modules:
        _m = types.ModuleType(_name)
        for _attr in ("Session", "SalesOrder", "Combo", "ComboItem", "Product",
                      "InitialInventory", "IncomingStock", "text"):
            setattr(_m, _attr, object)
        sys.modules[_name] = _m

import pandas as pd
from app.services.stock_calculator import build_shipped_components

INITIAL = pd.Timestamp("2026-01-01")


def _fila(order_date, status, shipped_time, cancel_type, qty=1, key="Far Away Original"):
    return {
        "Order_Date": pd.Timestamp(order_date), "Order Status": status,
        "Shipped Time": pd.NaT if shipped_time is None else pd.Timestamp(shipped_time),
        "Cancelation/Return Type": cancel_type,
        "ComponentKey": key, "ComponentQty": qty,
    }


def _qty(df, key="Far Away Original"):
    out = build_shipped_components(df, INITIAL)
    hit = out[out["ComponentKey"] == key]["QtyShipped"]
    return int(hit.iloc[0]) if len(hit) else 0


def test_enviada_con_devolucion_si_descuenta():
    """EL BUG: Completed + shipped_time + Return/Refund. Salió → descuenta."""
    df = pd.DataFrame([_fila("2026-08-25", "Completed", "2026-08-25", "Return/Refund", qty=3)])
    assert _qty(df) == 3, "orden enviada con devolución debe descontar"


def test_cancelada_antes_de_enviar_no_descuenta():
    """Cancelada sin salir del almacén: no descuenta."""
    df = pd.DataFrame([_fila("2026-08-25", "Canceled", None, "Cancel", qty=3)])
    assert _qty(df) == 0, "cancelación previa al envío no debe descontar"


def test_venta_normal_descuenta():
    df = pd.DataFrame([_fila("2026-08-25", "Delivered", "2026-08-26", None, qty=2)])
    assert _qty(df) == 2


def test_anterior_al_inventario_inicial_se_ignora():
    df = pd.DataFrame([_fila("2025-12-31", "Delivered", "2025-12-31", None, qty=9)])
    assert _qty(df) == 0


def test_caso_real_far_away_agregado():
    """Mezcla: 2 devoluciones ya enviadas (5 u) + 1 venta (2 u) + 1 cancelada (4 u)."""
    df = pd.DataFrame([
        _fila("2026-08-25", "Completed", "2026-08-25", "Return/Refund", qty=3),
        _fila("2026-08-30", "Completed", "2026-08-30", "Return/Refund", qty=2),
        _fila("2026-09-01", "Delivered", "2026-09-02", None, qty=2),
        _fila("2026-09-03", "Canceled", None, "Cancel", qty=4),
    ])
    assert _qty(df) == 7, "3+2 devueltas tras envío + 2 vendidas = 7; la cancelada no cuenta"


if __name__ == "__main__":
    fallos = 0
    for _n, _f in sorted(globals().items()):
        if _n.startswith("test_") and callable(_f):
            try:
                _f(); print(f"  OK   {_n}")
            except AssertionError as e:
                fallos += 1; print(f"  FALLA {_n}: {e}")
    print("TODO OK" if not fallos else f"{fallos} fallos")
    sys.exit(1 if fallos else 0)
