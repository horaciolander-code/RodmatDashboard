"""La marca de un combo se deriva de sus productos, en servidor.

Hasta el 15-oct `POST /api/combos` no guardaba `brand_id` — ni el schema lo
admitia. Todo combo creado por el panel nacia sin marca, y eso rompia dos
cosas en silencio:

  - el importador de TEMU saca la marca del combo para etiquetar la venta,
    asi que esas ventas entraban sin marca
  - `GET /api/combos` filtra por marca (combos.py:81), asi que un combo sin
    marca es invisible para un usuario con marca asignada

`app.services.marcas` no importa fastapi ni sqlalchemy a proposito: el job de
aislamiento del CI solo tiene pytest y pandas.
    python backend/tests/test_marca_combo.py
"""
import pathlib
import sys

_BACKEND = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_BACKEND))

from app.services.marcas import marca_de_los_componentes

AVON = "b1000001-0000-0000-0000-000000000001"
LUX = "b1000002-0000-0000-0000-000000000002"


class _Prod:
    def __init__(self, brand_id):
        self.brand_id = brand_id


def test_todos_de_la_misma_marca_la_hereda():
    assert marca_de_los_componentes([_Prod(AVON), _Prod(AVON), _Prod(AVON)]) == AVON


def test_un_solo_componente_tambien():
    assert marca_de_los_componentes([_Prod(LUX)]) == LUX


def test_componentes_de_marcas_DISTINTAS_no_heredan_ninguna():
    """Un combo mixto no pertenece a ninguna marca.

    Marcarlo con una de las dos falsearia el filtro de la otra: el P&L de
    Avon se comeria un pack que lleva Lattafa dentro. Mejor sin marca y que
    se vea, que con la marca equivocada y que no se vea.
    """
    assert marca_de_los_componentes([_Prod(AVON), _Prod(LUX)]) is None


def test_si_algun_componente_no_tiene_marca_mandan_los_que_si():
    """Un producto sin marca no debe impedir que el combo herede la suya."""
    assert marca_de_los_componentes([_Prod(AVON), _Prod(None), _Prod(AVON)]) == AVON


def test_ningun_componente_con_marca_devuelve_None():
    assert marca_de_los_componentes([_Prod(None), _Prod(None)]) is None
    assert marca_de_los_componentes([_Prod("")]) is None


def test_lista_vacia_o_None_no_revienta():
    assert marca_de_los_componentes([]) is None
    assert marca_de_los_componentes(None) is None


def test_objetos_sin_el_atributo_no_revientan():
    """Por si algun dia se le pasa otra cosa que no sea un Product."""
    assert marca_de_los_componentes([object()]) is None


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
