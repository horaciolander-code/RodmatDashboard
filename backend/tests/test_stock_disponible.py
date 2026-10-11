"""Lo vendido que aun no ha salido del almacen.

Un pendiente NO baja el stock real: puede cancelarse, y por eso se dejo
fuera desde el principio. Pero con varios canales vendiendo del mismo almacen
hay que verlo, o se compromete dos veces la misma unidad sin enterarse. El
11-oct habia 126 lineas asi: 91 en TikTok, 33 en TEMU, 2 en Amazon.

Los estados de aqui son los REALES de produccion, leidos de la BD:
  TikTok   Completed · Shipped · To ship · Pending · Canceled
  TEMU     Delivered · Shipped · Unshipped · Pending · Canceled
  Amazon   Shipped · Awaiting Shipment · Cancelled
  Walmart  Delivered · Shipped

Sin BD: se stubean sqlalchemy y app.models para importar el modulo suelto.
    python backend/tests/test_stock_disponible.py
"""
import pathlib
import sys

_BACKEND = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_BACKEND))

import _stubs

_stubs.instalar()

import pandas as pd
from app.services.stock_calculator import build_pending_components

INICIO = pd.Timestamp("2026-01-01")
DESPUES = pd.Timestamp("2026-10-10")


def _lineas(filas):
    base = {"Order_Date": DESPUES, "ComponentKey": "far away", "ComponentQty": 1,
            "Order Status": "To ship", "Shipped Time": pd.NaT,
            "Cancelation/Return Type": None}
    return pd.DataFrame([{**base, **f} for f in filas])


def _pendiente(df, clave="far away"):
    r = build_pending_components(df, INICIO)
    if r.empty:
        return 0
    fila = r[r["ComponentKey"] == clave]
    return int(fila["PendienteDeEnviar"].iloc[0]) if not fila.empty else 0


def test_cuenta_los_estados_de_espera_de_las_cuatro_plataformas():
    for estado in ("To ship", "Pending", "Unshipped", "Awaiting Shipment"):
        assert _pendiente(_lineas([{"Order Status": estado, "ComponentQty": 5}])) == 5, estado


def test_no_cuenta_lo_que_ya_salio():
    for estado in ("Shipped", "Delivered", "Completed"):
        assert _pendiente(_lineas([{"Order Status": estado, "ComponentQty": 5}])) == 0, estado


def test_no_cuenta_lo_cancelado():
    """Canceled (TikTok/TEMU) y Cancelled (Amazon) se escriben distinto."""
    for estado in ("Canceled", "Cancelled", "canceled"):
        assert _pendiente(_lineas([{"Order Status": estado, "ComponentQty": 5}])) == 0, estado


def test_un_estado_nuevo_cuenta_como_pendiente():
    """Definido por exclusion a proposito.

    Si manana un canal inventa 'Preparing' y la lista fuera cerrada, esas
    unidades desaparecerian de la alerta en silencio. Contarlas de mas avisa
    de algo que quiza no pasa; contarlas de menos deja vender algo que no hay.
    """
    assert _pendiente(_lineas([{"Order Status": "Preparing to ship", "ComponentQty": 7}])) == 7


def test_con_fecha_de_salida_ya_no_esta_pendiente():
    """Manda la marca de tiempo sobre el estado: si salio, salio."""
    df = _lineas([{"Order Status": "To ship", "Shipped Time": pd.Timestamp("2026-10-11"),
                   "ComponentQty": 4}])
    assert _pendiente(df) == 0


def test_una_devolucion_registrada_no_cuenta_como_pendiente():
    df = _lineas([{"Order Status": "To ship", "Cancelation/Return Type": "Refund",
                   "ComponentQty": 4}])
    assert _pendiente(df) == 0


def test_suma_por_producto_entre_canales():
    """El almacen es uno: lo de TikTok y lo de TEMU se acumulan sobre el mismo
    producto. Es justo el caso que hace falta ver."""
    df = _lineas([
        {"Order Status": "To ship",   "ComponentQty": 6},    # tiktok
        {"Order Status": "Unshipped", "ComponentQty": 4},    # temu
        {"Order Status": "Shipped",   "ComponentQty": 99},   # ya salio, no cuenta
        {"Order Status": "To ship",   "ComponentQty": 3, "ComponentKey": "imari"},
    ])
    assert _pendiente(df) == 10
    assert _pendiente(df, "imari") == 3


def test_lo_anterior_a_la_fecha_inicial_no_cuenta():
    df = _lineas([{"Order_Date": pd.Timestamp("2025-06-01"), "ComponentQty": 9}])
    assert _pendiente(df) == 0


def test_un_frame_vacio_devuelve_vacio_sin_reventar():
    vacio = pd.DataFrame(columns=["Order_Date", "ComponentKey", "ComponentQty", "Order Status"])
    assert build_pending_components(vacio, INICIO).empty


if __name__ == "__main__":
    fallos = 0
    for _n, _f in sorted(globals().items()):
        if _n.startswith("test_") and callable(_f):
            try:
                _f()
                print(f"  OK    {_n}")
            except AssertionError as e:
                fallos += 1
                print(f"  FALLA {_n}: {e}")
    print("TODO OK" if not fallos else f"{fallos} fallos")
    sys.exit(1 if fallos else 0)
