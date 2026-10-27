"""El almacen tiene que empaquetar lo mismo que el inventario descuenta.

Si `empaquetado.componentes_de` y `stock_calculator.decompose_orders` dejan
de decir lo mismo, el almacen mete tres frascos donde el sistema resta dos y
el descuadre aparece semanas despues sin rastro de donde salio. Por eso aqui
no se comprueba solo que la funcion nueva haga algo razonable: se compara
caso a caso contra la que mueve el stock de verdad.

La comparacion va con `db=None`, que es como decirle a decompose_orders que
ningun SKU esta en los mapas de Amazon/Walmart. El atajo de ese guardia se
prueba aparte, sobre la funcion pura, porque recrearlo pide base de datos.

    python backend/tests/test_empaquetado.py
"""
import pathlib
import sys

_BACKEND = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_BACKEND))

import _stubs

_stubs.instalar()

import pandas as pd
from app.services.empaquetado import (componentes_de, sku_limpio,
                                      total_piezas, ya_venia_expandida)
from app.services.stock_calculator import decompose_orders

COMBOS = {
    "AV-FOBM/3": [{"product": "Far Away Original", "product_id": "p1", "units": 1},
                  {"product": "Far Away Bali", "product_id": "p2", "units": 1},
                  {"product": "Far Away Moroccan", "product_id": "p3", "units": 1}],
    "AV-FP/3":   [{"product": "Far Away Original", "product_id": "p1", "units": 3}],
}


def _como_lo_ve_el_stock(clave, nombre, cantidad, plataforma="tiktok"):
    """Lo que decompose_orders saca para una sola linea."""
    df = pd.DataFrame([{
        "SKU_ID_Clean": clave, "Product Name": nombre,
        "Quantity": cantidad, "Platform": plataforma,
    }])
    salida = decompose_orders(df, COMBOS)
    return sorted((r["ComponentKey"], int(r["ComponentQty"]))
                  for _, r in salida.iterrows())


def _como_lo_ve_el_almacen(clave, nombre, cantidad, plataforma="tiktok"):
    piezas = componentes_de(clave, nombre, cantidad, COMBOS, plataforma=plataforma)
    return sorted((p["producto"], int(p["cantidad"])) for p in piezas)


CASOS = [
    ("AV-FOBM/3", "Far Away EDP Set of 3", 1),
    ("AV-FOBM/3", "Far Away EDP Set of 3", 4),
    ("AV-FP/3",   "Far Away Original x3",  2),
    ("AV-IP",     "Avon Imari EDT",        3),
    ("AV-NUEVO",  "Producto sin combo",    1),
    ("1729592630949483413", "", 2),
]


def test_el_almacen_y_el_stock_ven_lo_mismo():
    for clave, nombre, cantidad in CASOS:
        almacen = _como_lo_ve_el_almacen(clave, nombre, cantidad)
        stock = _como_lo_ve_el_stock(clave, nombre, cantidad)
        assert almacen == stock, (
            f"{clave} x{cantidad}: almacen={almacen} stock={stock}")


def test_un_pack_se_multiplica_por_las_unidades_pedidas():
    piezas = componentes_de("AV-FOBM/3", "pack", 4, COMBOS)
    assert sorted(p["cantidad"] for p in piezas) == [4, 4, 4]
    assert all(p["de_combo"] for p in piezas)


def test_tres_iguales_son_una_linea_de_tres_no_tres_lineas():
    """combo_items tiene UNIQUE(combo_id, product_id): N iguales van juntos."""
    piezas = componentes_de("AV-FP/3", "pack de 3", 2, COMBOS)
    assert piezas == [{"producto": "Far Away Original", "cantidad": 6,
                       "de_combo": True}]


def test_lo_que_no_es_combo_sale_tal_cual():
    piezas = componentes_de("AV-IP", "Avon Imari EDT", 2, COMBOS)
    assert piezas == [{"producto": "Avon Imari EDT", "cantidad": 2,
                       "de_combo": False}]


def test_sin_nombre_se_ensena_el_sku_antes_que_un_hueco():
    piezas = componentes_de("AV-X", "", 1, COMBOS)
    assert piezas[0]["producto"] == "AV-X"
    piezas = componentes_de("AV-X", "nan", 1, COMBOS)
    assert piezas[0]["producto"] == "AV-X"


def test_el_seller_sku_manda_y_el_numerico_es_el_respaldo():
    assert sku_limpio("AV-IP", "17295926") == "AV-IP"
    assert sku_limpio("", "17295926") == "17295926"
    assert sku_limpio(None, "17295926") == "17295926"
    assert sku_limpio("nan", "17295926") == "17295926"
    assert sku_limpio(None, None) == ""


def test_en_amazon_un_sku_del_mapa_no_se_vuelve_a_expandir():
    """El doble descuento de AV-BSS/2: el importador ya multiplico."""
    assert ya_venia_expandida("amazon", "AV-BSS/2", {"AV-BSS/2"}) is True
    assert ya_venia_expandida("walmart", "AV-BSS/2", {"AV-BSS/2"}) is True
    piezas = componentes_de("AV-FOBM/3", "pack", 1, COMBOS,
                            plataforma="amazon", skus_de_mapa={"AV-FOBM/3"})
    assert piezas == [{"producto": "pack", "cantidad": 1, "de_combo": False}]


def test_en_tiktok_y_temu_el_combo_se_aplica_siempre():
    """El guardia es solo de Amazon y Walmart; sus parsers son los que expanden."""
    for canal in ("tiktok", "temu", None):
        assert ya_venia_expandida(canal, "AV-FOBM/3", {"AV-FOBM/3"}) is False
        piezas = componentes_de("AV-FOBM/3", "pack", 1, COMBOS, plataforma=canal,
                                skus_de_mapa={"AV-FOBM/3"})
        assert len(piezas) == 3, f"{canal} no desmonto el pack"


def test_una_cantidad_ilegible_no_tumba_la_pagina():
    piezas = componentes_de("AV-IP", "Imari", None, COMBOS)
    assert piezas[0]["cantidad"] == 0


def test_el_total_de_piezas_suma_el_pedido_entero():
    lineas = [componentes_de("AV-FOBM/3", "pack", 2, COMBOS),
              componentes_de("AV-IP", "Imari", 3, COMBOS)]
    assert total_piezas(lineas) == 9


if __name__ == "__main__":
    fallos = 0
    for _n, _f in sorted(globals().items()):
        if _n.startswith("test_") and callable(_f):
            try:
                _f()
                print(f"  OK    {_n}")
            except AssertionError as e:
                fallos += 1
                print(f"  FALLA {_n}: {str(e)[:200]}")
    print("TODO OK" if not fallos else f"{fallos} fallos")
    sys.exit(1 if fallos else 0)
