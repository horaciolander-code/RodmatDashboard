"""El extracto de TEMU tiene seis tipos de transacción y cada uno se trata
distinto. Este test fija ese comportamiento con un fichero de juguete.

Sin base de datos: se prueba `construir_filas_temu`, que es la parte pura.
    python backend/tests/test_temu_import.py
"""
import importlib
import io
import pathlib
import sys
import types

_BACKEND = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_BACKEND))


def _stub(nombre):
    """Módulo falso SOLO si el real no se puede importar, para no contaminar
    la sesión de pytest en entornos que sí tienen las dependencias."""
    if nombre in sys.modules:
        return
    try:
        importlib.import_module(nombre)
        return
    except Exception:
        pass
    m = types.ModuleType(nombre)
    for attr in ("Session", "SalesOrder", "AffiliateSale", "Product", "Combo",
                 "ComboItem", "InitialInventory", "IncomingStock", "and_", "text"):
        setattr(m, attr, object)
    sys.modules[nombre] = m


for _n in ("sqlalchemy", "sqlalchemy.orm", "app.models.sales", "app.models.product",
           "app.models.combo", "app.models.inventory"):
    _stub(_n)

import pandas as pd

from app.services.import_service import construir_filas_temu

CABECERA = ('"Fecha/hora","Tipo de transacción","ID relacionado","ID del pedido",'
            '"ID del artículo del pedido","SKU","SKU ID","Cantidad","Ciudad de envío",'
            '"Estado del envío","Precio minorista","Descuento en la plataforma",'
            '"Descuento del vendedor","Tarifa de servicio","Impuesto sobre la tarifa del servicio",'
            '"Incentivo de la plataforma","Subtotal","Envío","Incentivo de plataforma - Envío",'
            '"Impuesto sobre el producto","Impuesto de envío","Firma en la entrega",'
            '"Impuesto de firma en la entrega","Impuesto retenido en el marketplace","Otros",'
            '"Total","Moneda"')

# Un pedido simple, uno con dos líneas, una etiqueta huérfana, una devolución,
# una transferencia al banco y una penalización.
FILAS = [
    # venta simple
    '"2026-09-01 16:45:36","Order Payment","PO-1","PO-1","it-1","Set of 3","111","1","PITTSBURGH","PA","68.77","-3.00","0.00","-6.92","0.00","0.00","61.85","2.99","0.00","4.81","0.00","0.00","0.00","-4.81","0.00","64.84","USD"',
    '"2026-09-02 20:58:28","Shipping label purchase","G1","PO-1",,,,,,,"0.00","0.00","0.00","0.00","0.00","0.00","0.00","0.00","0.00","0.00","0.00","0.00","0.00","0.00","-4.25","-4.25","USD"',
    # pedido con DOS líneas: el coste de etiqueta se reparte entre ambas
    '"2026-09-05 10:00:00","Order Payment","PO-2","PO-2","it-2","Set of 5","222","1","MEMPHIS","TN","30.00","0.00","0.00","0.00","0.00","0.00","30.00","0.00","0.00","0.00","0.00","0.00","0.00","0.00","0.00","30.00","USD"',
    '"2026-09-05 10:00:00","Order Payment","PO-2","PO-2","it-3","Unidad","333","2","MEMPHIS","TN","10.00","0.00","0.00","0.00","0.00","0.00","10.00","0.00","0.00","0.00","0.00","0.00","0.00","0.00","0.00","10.00","USD"',
    '"2026-09-06 09:00:00","Shipping label purchase","G2","PO-2",,,,,,,"0.00","0.00","0.00","0.00","0.00","0.00","0.00","0.00","0.00","0.00","0.00","0.00","0.00","0.00","-8.00","-8.00","USD"',
    # venta devuelta, con su etiqueta: salió del almacén y luego se reembolsó
    '"2026-09-10 12:00:00","Order Payment","PO-3","PO-3","it-4","Set of 3","111","1","LYNN","MA","68.77","0.00","0.00","0.00","0.00","0.00","61.85","0.00","0.00","0.00","0.00","0.00","0.00","0.00","0.00","61.85","USD"',
    '"2026-09-09 08:00:00","Shipping label purchase","G3","PO-3",,,,,,,"0.00","0.00","0.00","0.00","0.00","0.00","0.00","0.00","0.00","0.00","0.00","0.00","0.00","0.00","-5.00","-5.00","USD"',
    '"2026-09-11 12:00:00","Refund","PO-3","PO-3","it-4","Set of 3","111","1","LYNN","MA","-68.77","0.00","0.00","0.00","0.00","0.00","-61.85","0.00","0.00","0.00","0.00","0.00","0.00","0.00","0.00","-61.85","USD"',
    # abono de una etiqueta: viene en POSITIVO y baja el coste, no lo sube
    '"2026-09-03 07:30:35","Shipping label purchase adjustment","G1b","PO-1",,,,,,,"0.00","0.00","0.00","0.00","0.00","0.00","0.00","0.00","0.00","0.00","0.00","0.00","0.00","0.00","0.25","0.25","USD"',
    # etiqueta de un pedido que NO viene en este fichero
    '"2026-09-20 10:00:00","Shipping label purchase","G9","PO-VIEJO",,,,,,,"0.00","0.00","0.00","0.00","0.00","0.00","0.00","0.00","0.00","0.00","0.00","0.00","0.00","0.00","-3.33","-3.33","USD"',
    # dinero saliendo al banco: NO es un coste
    '"2026-09-22 14:44:32","Transfer","LWO1","",,,,,,,"0.00","0.00","0.00","0.00","0.00","0.00","0.00","0.00","0.00","0.00","0.00","0.00","0.00","0.00","-449.03","-449.03","USD"',
    # penalización por envío tardío
    '"2026-09-20 03:49:15","Delayed fulfillment deduction","DO1","PO-1",,,,,,,"0.00","0.00","0.00","0.00","0.00","0.00","0.00","0.00","0.00","0.00","0.00","0.00","0.00","0.00","-5.00","-5.00","USD"',
]

MARCAS = {"111": "marca-avon", "222": "marca-avon"}   # el 333 no tiene combo


def _construir():
    csv = CABECERA + "\n" + "\n".join(FILAS) + "\n"
    df = pd.read_csv(io.StringIO(csv), dtype=str, keep_default_na=False)
    df.columns = df.columns.str.strip()
    return construir_filas_temu(df, "tienda-1", "lote-1", MARCAS)


def test_solo_las_ventas_se_convierten_en_filas():
    """4 Order Payment. Ni las etiquetas, ni la transferencia, ni la
    penalización, ni la devolución generan fila de venta."""
    filas, _ = _construir()
    assert len(filas) == 4, [f["tiktok_order_id"] for f in filas]
    assert all(f["platform"] == "temu" for f in filas)


def test_la_transferencia_al_banco_no_es_un_coste():
    """Es el error más caro del fichero: contarla duplica el gasto en el P&L."""
    _, r = _construir()
    assert r["transferencias"] == 1
    assert r["penalizaciones"] == 5.00


def test_el_coste_de_la_etiqueta_llega_desde_su_fila_aparte():
    """PO-1 tiene etiqueta de 4,25 y un abono de 0,25 → 4,00 de coste real.
    El abono viene en positivo: hay que restarlo, no sumar su valor absoluto."""
    filas, _ = _construir()
    po1 = next(f for f in filas if f["tiktok_order_id"] == "PO-1")
    assert po1["original_shipping_fee"] == 4.00   # lo que nos cuesta, neto del abono
    assert po1["shipping_fee_after_discount"] == 2.99   # lo que pagó el comprador


def test_el_envio_se_prorratea_entre_las_lineas_del_pedido():
    """PO-2: 8,00 de etiqueta repartidos entre 30,00 y 10,00 → 6,00 y 2,00."""
    filas, _ = _construir()
    po2 = {f["sku"]: f for f in filas if f["tiktok_order_id"] == "PO-2"}
    assert po2["222"]["original_shipping_fee"] == 6.00
    assert po2["333"]["original_shipping_fee"] == 2.00
    assert round(sum(f["original_shipping_fee"] for f in po2.values()), 2) == 8.00


def test_la_etiqueta_fija_la_fecha_de_salida():
    """TEMU no informa fecha de envío. Sin shipped_time, una orden devuelta no
    descontaría stock aunque la mercancía hubiese salido."""
    filas, _ = _construir()
    po3 = next(f for f in filas if f["tiktok_order_id"] == "PO-3")
    assert po3["shipped_time"] is not None
    assert str(po3["shipped_time"])[:10] == "2026-09-09"


def test_la_devolucion_marca_la_venta_sin_borrarla():
    filas, _ = _construir()
    po3 = next(f for f in filas if f["tiktok_order_id"] == "PO-3")
    assert po3["cancelation_return_type"] == "Refund"
    assert po3["order_refund_amount"] == 61.85
    assert po3["sku_subtotal_after_discount"] == 61.85   # la venta no se borra


def test_etiqueta_de_pedido_ausente_no_se_pierde_en_silencio():
    _, r = _construir()
    assert "PO-VIEJO" in r["envios_huerfanos"]
    assert r["envios_huerfanos"]["PO-VIEJO"] == 3.33


def test_la_marca_sale_del_combo_y_lo_no_mapeado_se_avisa():
    """Sin brand_id la fila es invisible en la vista de marca; y un anuncio sin
    combo no descuenta stock, así que hay que avisar de los dos casos."""
    filas, r = _construir()
    assert all(f["brand_id"] == "marca-avon" for f in filas if f["sku"] in ("111", "222"))
    assert next(f for f in filas if f["sku"] == "333")["brand_id"] is None
    assert r["sin_combo"] == ["333"]


def test_el_sku_id_va_en_las_dos_columnas_que_mira_el_stock():
    """El stock empareja por `Seller SKU ?? SKU ID`. La columna 'SKU' del
    fichero es el título del anuncio, no un SKU."""
    filas, _ = _construir()
    po1 = next(f for f in filas if f["tiktok_order_id"] == "PO-1")
    assert po1["sku"] == "111" and po1["seller_sku"] == "111"
    assert po1["product_name"] == "Set of 3"


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
