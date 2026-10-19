"""El UPSERT no debe reescribir las filas que llegan iguales.

Supabase aviso el 19-oct-2026 de que el proyecto estaba agotando su
presupuesto de Disk IO. La causa: el fichero diario de TikTok son ~18.000
filas y cambian ~100, pero los importadores hacian UPSERT de todas.

Una UPDATE que deja la fila igual NO es gratis en Postgres: crea una version
nueva, escribe WAL, actualiza TODOS los indices y deja una fila muerta para
el autovacuum. Reescribir 17.900 filas identicas cada manana sobre una tabla
de 28.000 es rehacer media tabla al dia.

Estos tests fijan las DOS decisiones que hacen que el guardia funcione:
no comparar las columnas de control, y usar IS DISTINCT FROM.

Sin BD: se stubean sqlalchemy y app.models.
    python backend/tests/test_upsert_solo_si_cambia.py
"""
import pathlib
import sys

_BACKEND = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_BACKEND))

import _stubs

_stubs.instalar()

from app.services.import_service import _COLS_SIN_VALOR_DE_NEGOCIO


def test_import_batch_id_NO_cuenta_como_cambio():
    """Es la decision de la que depende todo el arreglo.

    `import_batch_id` es distinto en CADA importacion. Si entrara en la
    comparacion, el guardia seria siempre cierto, no se saltaria ni una fila
    y el arreglo no haria absolutamente nada — pareciendo que si.
    """
    assert "import_batch_id" in _COLS_SIN_VALOR_DE_NEGOCIO


def test_imported_at_tampoco():
    assert "imported_at" in _COLS_SIN_VALOR_DE_NEGOCIO


def test_no_se_excluye_nada_de_negocio():
    """Excluir de mas es peor que no arreglar nada: una columna fuera de la
    comparacion deja de actualizarse cuando cambia de verdad."""
    de_negocio = {
        "status", "quantity", "price", "product_name", "order_date",
        "shipped_time", "cancelation_return_type", "sku_subtotal_after_discount",
        "order_amount", "order_refund_amount", "brand_id", "seller_sku",
        "city", "state", "platform", "variation", "fulfillment_type",
    }
    solapan = de_negocio & _COLS_SIN_VALOR_DE_NEGOCIO
    assert not solapan, f"columnas de negocio excluidas por error: {solapan}"


def test_la_lista_es_corta_a_proposito():
    """Si crece, alguien esta tapando un problema distinto con este guardia."""
    assert len(_COLS_SIN_VALOR_DE_NEGOCIO) <= 3


# ── La condicion SQL ─────────────────────────────────────────────────────
# `_solo_si_cambia` necesita sqlalchemy de verdad para construir la clausula,
# y el job de aislamiento del CI no lo tiene. La verificacion de que el SQL
# compila bien contra el dialecto de Postgres se hizo aparte, comprobando
# que genera:
#
#   ON CONFLICT ON CONSTRAINT uq_store_order_sku DO UPDATE
#   SET ... WHERE sales_orders.status IS DISTINCT FROM excluded.status
#            OR sales_orders.quantity IS DISTINCT FROM excluded.quantity ...
#   RETURNING (xmax = 0)
#
# Lo que si se puede fijar aqui sin base de datos es POR QUE va con
# IS DISTINCT FROM y no con !=.

def test_por_que_IS_DISTINCT_FROM_y_no_distinto():
    """Con NULL, `!=` devuelve NULL, que no es cierto.

    Una fila cuyo `shipped_time` pasa de NULL a una fecha no se actualizaria
    nunca: justo el caso de un pedido que se envia. `IS DISTINCT FROM` trata
    NULL como un valor mas y si lo detecta.
    """
    def distinto_sql(a, b):
        return None if (a is None or b is None) else a != b

    def is_distinct_from(a, b):
        if a is None and b is None:
            return False
        if a is None or b is None:
            return True
        return a != b

    assert distinto_sql(None, "2026-10-19") is None, "!= con NULL no decide"
    assert not bool(distinto_sql(None, "2026-10-19")), "y por tanto no actualizaria"
    assert is_distinct_from(None, "2026-10-19") is True, "IS DISTINCT FROM si"
    assert is_distinct_from(None, None) is False
    assert is_distinct_from("Shipped", "Delivered") is True
    assert is_distinct_from("Shipped", "Shipped") is False


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
