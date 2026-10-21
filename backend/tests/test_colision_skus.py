"""Un SKU nuevo entra. Un SKU que YA descuenta se confirma antes.

La regla de Horacio, 21-oct-2026:

    "lo que no podemos permitir es duplicados SKU con distintos componentes
     [...] un sku que en tiktok descuente algo que en temu o bien no lo
     sabes o bien esta mal. Pero si es un sku nuevo tu lo dejas entrar."

Son dos cosas opuestas que se parecen:

  SKU NUEVO     no tiene componentes -> no descuenta -> no puede
                equivocarse. Entra, y se asigna en Gestion Combos.

  SKU EN USO    un anuncio de TEMU que no habiamos visto reclama un seller
                SKU que ya descuenta. Si no es ese producto, cada venta
                resta del stock de otro y nadie lo ve.

Se mide por ID de anuncio, nunca por el texto del seller SKU: la relacion
nuestra es el ID de producto de la plataforma.
    python backend/tests/test_colision_skus.py
"""
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from app.services.colision_skus import (anuncios_a_confirmar,
                                        sellers_con_varios_anuncios,
                                        texto_a_confirmar)

# (id_de_anuncio, seller_sku, nombre)
CONOCIDOS = ["601099512345678"]
CON_COMPONENTES = ["av-ms/2", "av-mbb/2"]


# ── Lo que debe dejar pasar ──────────────────────────────────────────────

def test_un_sku_nuevo_entra_sin_avisar():
    """AV-MS/2 sin componentes todavia: no descuenta, no puede fallar."""
    lineas = [("601000000000999", "AV-MS/2", "Mesmerize 2 pack")]
    assert anuncios_a_confirmar(lineas, CONOCIDOS, []) == []


def test_un_anuncio_ya_conocido_no_vuelve_a_avisar():
    """Se confirmo el dia que aparecio. Avisar cada carga seria ruido."""
    lineas = [("601099512345678", "AV-MS/2", "Mesmerize 2 pack")]
    assert anuncios_a_confirmar(lineas, CONOCIDOS, CON_COMPONENTES) == []


# ── Lo que NO puede pasar en silencio ───────────────────────────────────

def test_anuncio_nuevo_sobre_un_sku_que_ya_descuenta():
    """El caso de Horacio: TEMU reusa un seller SKU de TikTok."""
    lineas = [("601000000000111", "AV-MS/2", "Otra cosa distinta")]
    assert anuncios_a_confirmar(lineas, CONOCIDOS, CON_COMPONENTES) == [
        ("AV-MS/2", "601000000000111", "Otra cosa distinta")]


def test_el_mismo_sku_con_dos_anuncios_en_el_fichero():
    """Esto no es ambiguo, es un error: uno de los dos descuenta mal."""
    lineas = [("601000000000111", "AV-MS/2", "a"),
              ("601000000000222", "AV-MS/2", "b")]
    assert sellers_con_varios_anuncios(lineas) == [
        ("av-ms/2", ["601000000000111", "601000000000222"])]


def test_un_sku_con_un_solo_anuncio_no_es_duplicado():
    lineas = [("601000000000111", "AV-MS/2", "a"),
              ("601000000000111", "AV-MS/2", "a")]
    assert sellers_con_varios_anuncios(lineas) == []


def test_no_repite_el_mismo_aviso():
    lineas = [("601000000000111", "AV-MS/2", "x")] * 4
    assert len(anuncios_a_confirmar(lineas, CONOCIDOS, CON_COMPONENTES)) == 1


# ── Robustez ────────────────────────────────────────────────────────────

def test_mayusculas_tabuladores_y_espacios():
    """Los campos de TEMU vienen con tabuladores pegados detras."""
    lineas = [("\t601000000000111 ", " av-MS/2\t", "x")]
    assert len(anuncios_a_confirmar(lineas, [], ["AV-MS/2"])) == 1
    assert anuncios_a_confirmar(lineas, [" 601000000000111 "], ["AV-MS/2"]) == []


def test_lineas_incompletas_se_ignoran():
    lineas = [("", "AV-MS/2", "x"), ("601000000000111", "", "x"),
              (None, None, None)]
    assert anuncios_a_confirmar(lineas, [], CON_COMPONENTES) == []
    assert sellers_con_varios_anuncios(lineas) == []


def test_orden_estable():
    """El aviso no puede cambiar de orden entre cargas iguales."""
    lineas = [("601000000000222", "AV-MBB/2", "b"),
              ("601000000000111", "AV-MS/2", "a")]
    r = anuncios_a_confirmar(lineas, [], CON_COMPONENTES)
    assert [x[0] for x in r] == ["AV-MBB/2", "AV-MS/2"]


def test_texto_lleva_el_id_del_anuncio():
    """El seller SKU solo no identifica nada: el ID es la relacion."""
    t = texto_a_confirmar([("AV-MS/2", "601000000000111", "Mesmerize")])
    assert "AV-MS/2" in t and "601000000000111" in t


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


# ── Anadido tras la simulacion: el aviso no puede salir todos los dias ───

def test_dos_anuncios_ya_aceptados_dejan_de_avisar():
    """Un producto puede tener dos anuncios legitimos en TEMU.

    Sin esto el aviso saltaria en CADA carga para siempre, y un aviso que
    sale todos los dias deja de leerse — que es como se cuela el que importa.
    """
    lineas = [("601000000000111", "AV-MS/2", "a"),
              ("601000000000222", "AV-MS/2", "b")]
    ambos = ["601000000000111", "601000000000222"]
    assert sellers_con_varios_anuncios(lineas, ambos) == []


def test_si_aparece_un_anuncio_nuevo_vuelve_a_avisar():
    """Algo ha cambiado: hay que mirarlo."""
    lineas = [("601000000000111", "AV-MS/2", "a"),
              ("601000000000333", "AV-MS/2", "c")]
    r = sellers_con_varios_anuncios(lineas, ["601000000000111"])
    assert r == [("av-ms/2", ["601000000000111", "601000000000333"])]
