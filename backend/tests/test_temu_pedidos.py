"""El export de PEDIDOS de TEMU es lo que descuenta inventario.

Hasta el 11-oct se cargaba el extracto financiero en su lugar. El extracto
solo da el SKU interno de TEMU, asi que septiembre entro con combos creados
sobre ese id en vez del SKU de la casa. El export de pedidos trae
`contribution sku`, que ES el SKU de combo, y ademas estados y cantidades
explicitas.

Los casos de aqui salen del fichero real, no son inventados.

Sin BD: se stubean sqlalchemy y app.models para importar el modulo suelto.
    python backend/tests/test_temu_pedidos.py
"""
import pathlib
import sys

_BACKEND = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_BACKEND))

import _stubs

_stubs.instalar()

import pandas as pd
from app.services.import_service import (
    TEMU_ESTADOS_QUE_DESCUENTAN,
    _fecha_temu,
    construir_filas_temu_pedidos,
)

TIENDA = "store-1"
MARCAS = {"av-mbb/2": "marca-avon", "av-ss/3": "marca-avon", "av-sss/8": "marca-avon"}


def _linea(pedido, sku, numerico, estado, cantidad, fecha="Oct 11, 2026, 4:43 pm CEST(UTC+2)"):
    # Los tabuladores pegados son reales: asi los escribe TEMU en el CSV.
    return {
        "Order ID": pedido,
        "order item status": estado,
        "contribution sku": sku,
        "SKU ID": numerico + "\t",
        "quantity purchased": str(cantidad) + "\t",
        "quantity shipped": str(cantidad) + "\t",
        "quantity canceled": "0\t",
        "purchase date": fecha,
        "product name": "Mesmerize Blue EDC & Mesmerize Black EDT",
        "goods base price": "$34.90",
        "variation": "2\t",
        "ship city": "Miami",
        "ship state": "FL",
        "Fulfillment mode": "Seller fulfillment",
    }


def _construir(lineas, marcas=None):
    df = pd.DataFrame(lineas)
    return construir_filas_temu_pedidos(df, TIENDA, "lote-1", MARCAS if marcas is None else marcas)


def test_solo_descuentan_los_estados_que_salieron_del_almacen():
    """Es la MISMA regla que TikTok. Unshipped y Pending no han salido."""
    filas, res = _construir([
        _linea("P1", "AV-MBB/2", "111", "Shipped", 3),
        _linea("P2", "AV-MBB/2", "111", "Delivered", 2),
        _linea("P3", "AV-MBB/2", "111", "Unshipped", 10),
        _linea("P4", "AV-MBB/2", "111", "Pending", 10),
        _linea("P5", "AV-MBB/2", "111", "Canceled", 10),
    ])
    assert len(filas) == 5, "las 5 lineas se guardan: lo que cambia es cual descuenta"
    assert res["unidades_que_descuentan"] == 5, "solo las 3 enviadas + 2 entregadas"


def test_el_mismo_pedido_y_sku_dos_veces_se_SUMA_no_se_pisa():
    """Caso real: PO-211-18704138184313299 trae AV-MBB/2 en dos lineas.

    El UPSERT va por (store, pedido, sku). Si no se suman aqui, la segunda
    pisa a la primera y se pierde una unidad de combo — dos unidades fisicas.
    """
    filas, res = _construir([
        _linea("PO-211-18704138184313299", "AV-MBB/2", "172056389900572", "Shipped", 1),
        _linea("PO-211-18704138184313299", "AV-MBB/2", "172056389900572", "Shipped", 1),
    ])
    assert len(filas) == 1, "una sola fila para esa clave"
    assert filas[0]["quantity"] == 2, "las dos unidades tienen que sobrevivir"
    assert res["lineas_agrupadas"] == 1
    assert res["unidades_que_descuentan"] == 2


def test_el_sku_de_combo_manda_y_el_numerico_se_guarda_aparte():
    """`seller_sku` es lo que usa el calculador para encontrar el combo.

    El numerico se guarda en `sku` para que el extracto financiero sepa a que
    fila engancharse cuando lo suban despues.
    """
    filas, _ = _construir([_linea("P1", "AV-MBB/2", "172056389900572", "Shipped", 1)])
    assert filas[0]["seller_sku"] == "AV-MBB/2"
    assert filas[0]["sku"] == "172056389900572", "sin el tabulador"


def test_la_marca_sale_del_combo_no_se_adivina():
    filas, _ = _construir([_linea("P1", "AV-SS/3", "222", "Delivered", 1)])
    assert filas[0]["brand_id"] == "marca-avon"


def test_un_sku_que_no_esta_dado_de_alta_se_informa():
    """Si no se avisa, sus ventas no descuentan y nadie se entera."""
    filas, res = _construir([_linea("P1", "AV-NUEVO/9", "333", "Shipped", 4)])
    assert res["sin_combo"] == ["AV-NUEVO/9"]
    assert filas[0]["brand_id"] is None


def test_la_plataforma_siempre_es_temu():
    filas, _ = _construir([_linea("P1", "AV-MBB/2", "111", "Shipped", 1)])
    assert filas[0]["platform"] == "temu"


def test_cantidad_cero_o_vacia_no_crea_fila():
    filas, _ = _construir([
        _linea("P1", "AV-MBB/2", "111", "Shipped", 0),
        _linea("P2", "AV-MBB/2", "111", "Shipped", 2),
    ])
    assert len(filas) == 1 and filas[0]["tiktok_order_id"] == "P2"


def test_la_fecha_de_temu_se_entiende_y_queda_sin_zona():
    """El resto del sistema guarda fechas sin zona: mezclarlas rompe las
    comparaciones del calculador de stock."""
    f = _fecha_temu("Oct 11, 2026, 4:43 pm CEST(UTC+2)")
    assert (f.year, f.month, f.day, f.hour, f.minute) == (2026, 10, 11, 16, 43)
    assert f.tzinfo is None
    assert _fecha_temu("Sep 1, 2026, 12:18 am CEST(UTC+2)").month == 9
    assert _fecha_temu("") is None
    assert _fecha_temu("no es una fecha") is None


def test_los_estados_que_descuentan_son_los_mismos_que_en_tiktok():
    """Si alguien anade uno aqui sin mirar build_shipped_components, TEMU y
    TikTok empezarian a contar distinto."""
    assert TEMU_ESTADOS_QUE_DESCUENTAN == {"shipped", "delivered", "completed"}



def test_la_linea_lleva_el_subtotal_facturado():
    """De aqui sale el "Facturado" del dashboard y del email diario.

    Sin esta columna TEMU sumaba ordenes y unidades pero facturaba CERO: los
    dias 10, 11 y 12 de octubre salian con 22, 15 y 16 pedidos y 0 $.
    """
    filas, _ = _construir([_linea("P1", "AV-MBB/2", "111", "Shipped", 3)])
    assert filas[0]["sku_subtotal_after_discount"] == 104.70, "34.90 x 3"


def test_el_precio_base_es_por_unidad_no_por_linea():
    """Comprobado en los ficheros reales: AV-SSS/8 vale 34.79 con cantidad
    1, 2 y 7. Si se tratara como total de linea, el facturado saldria bajo."""
    una, _ = _construir([_linea("P1", "AV-MBB/2", "111", "Shipped", 1)])
    dos, _ = _construir([_linea("P2", "AV-MBB/2", "111", "Shipped", 2)])
    assert dos[0]["sku_subtotal_after_discount"] == 2 * una[0]["sku_subtotal_after_discount"]


def test_al_agrupar_dos_lineas_el_subtotal_tambien_se_suma():
    """Si se suman las unidades pero no el importe, el facturado sale corto."""
    filas, _ = _construir([
        _linea("PO-211-18704138184313299", "AV-MBB/2", "172056389900572", "Shipped", 1),
        _linea("PO-211-18704138184313299", "AV-MBB/2", "172056389900572", "Shipped", 1),
    ])
    assert filas[0]["quantity"] == 2
    assert filas[0]["sku_subtotal_after_discount"] == 69.80


def test_sin_precio_el_subtotal_queda_vacio_no_a_cero():
    """Vacio deja que el extracto lo rellene despues. Un 0 se quedaria ahi
    para siempre, porque el UPSERT solo rellena lo que esta a NULL."""
    l = _linea("P1", "AV-MBB/2", "111", "Shipped", 2)
    l["goods base price"] = ""
    filas, _ = _construir([l])
    assert filas[0]["sku_subtotal_after_discount"] is None


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
