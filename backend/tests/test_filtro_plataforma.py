"""El filtro de canal se aplica SOLO a las ventas, nunca al stock.

El almacén es uno solo: repartirlo por canal sería inventarse un número. Lo
que cambia al filtrar son las unidades vendidas y lo que se deriva de ellas.

El riesgo real que vigilan estos tests es que el filtro se coma ventas sin
avisar — sobre todo las de combos, que se construyen con `order.copy()` en
`decompose_orders` y por eso conservan su `Platform`. Si algún día dejaran de
conservarla, filtrar por canal haría desaparecer todos los packs y el número
saldría más bajo sin que nada fallara.

Sin BD: se stubean sqlalchemy y app.models para importar el módulo suelto.
    python backend/tests/test_filtro_plataforma.py
"""
import pathlib
import sys

_BACKEND = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_BACKEND))

import _stubs

_stubs.instalar()

import pandas as pd
from app.services.stock_calculator import PLATAFORMAS, _solo_plataforma


def _ventas(filas):
    return pd.DataFrame(filas)


VENTAS = _ventas([
    {"Platform": "tiktok",  "ComponentKey": "far away", "ComponentQty": 3},
    {"Platform": "temu",    "ComponentKey": "far away", "ComponentQty": 5},
    {"Platform": "amazon",  "ComponentKey": "imari",    "ComponentQty": 2},
    {"Platform": "TEMU",    "ComponentKey": "imari",    "ComponentQty": 7},
])


def test_sin_canal_no_filtra_nada():
    """«Todas las plataformas» tiene que devolver el frame entero."""
    assert len(_solo_plataforma(VENTAS, None)) == 4
    assert len(_solo_plataforma(VENTAS, "")) == 4


def test_deja_solo_el_canal_pedido():
    r = _solo_plataforma(VENTAS, "temu")
    assert r["ComponentQty"].sum() == 12, "deberian ser las 5 + 7 unidades de TEMU"
    assert set(r["Platform"].str.lower()) == {"temu"}


def test_no_distingue_mayusculas_ni_espacios():
    """El CSV de TEMU escribe el canal de varias formas segun el export."""
    for escrito in ("TEMU", " temu ", "Temu"):
        assert _solo_plataforma(VENTAS, escrito)["ComponentQty"].sum() == 12


def test_las_filas_de_combo_conservan_su_canal():
    """Un pack vendido en TEMU se descompone en componentes que siguen siendo de TEMU.

    Es el fallo que mas caro saldria: si al descomponer se perdiera `Platform`,
    filtrar por canal borraria las ventas de packs y nadie veria un error.
    """
    con_combos = _ventas([
        {"Platform": "temu",   "ComponentKey": "odyssey cologne",  "ComponentQty": 2, "Is_Combo": True},
        {"Platform": "temu",   "ComponentKey": "odyssey deodorant", "ComponentQty": 2, "Is_Combo": True},
        {"Platform": "tiktok", "ComponentKey": "odyssey cologne",  "ComponentQty": 9, "Is_Combo": True},
    ])
    r = _solo_plataforma(con_combos, "temu")
    assert len(r) == 2
    assert r["ComponentQty"].sum() == 4


def test_un_canal_sin_ventas_devuelve_vacio_no_todo():
    """Mejor cero que el total: devolver el frame entero seria mentir."""
    r = _solo_plataforma(VENTAS, "walmart")
    assert r.empty


def test_sin_columna_platform_el_historico_es_tiktok():
    """Lo anterior a la multiplataforma es TikTok, igual que el COALESCE del SQL."""
    antiguo = _ventas([
        {"ComponentKey": "far away", "ComponentQty": 4},
        {"ComponentKey": "imari",    "ComponentQty": 6},
    ])
    assert _solo_plataforma(antiguo, "tiktok")["ComponentQty"].sum() == 10
    assert _solo_plataforma(antiguo, "temu").empty


def test_un_frame_vacio_no_revienta():
    vacio = pd.DataFrame(columns=["Platform", "ComponentKey", "ComponentQty"])
    assert _solo_plataforma(vacio, "temu").empty


def test_la_suma_de_los_canales_es_el_total():
    """Ninguna venta se queda fuera de todos los canales ni se cuenta dos veces."""
    total = VENTAS["ComponentQty"].sum()
    por_canal = sum(_solo_plataforma(VENTAS, p)["ComponentQty"].sum() for p in PLATAFORMAS)
    assert por_canal == total


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
