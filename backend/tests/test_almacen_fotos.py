"""Una foto equivocada en el almacen es un paquete equivocado.

El catalogo tiene cinco seller SKU colgando de DOS anuncios con contenidos
distintos, y uno mas (AV-FM/2) que ademas vendio con dos titulos. Si el
resolutor eligiera por SKU, esas ventas saldrian con la foto del otro pack.

Medido sobre 18.403 lineas de pedido reales de TikTok: cero fotos
equivocadas, con y sin el ID de anuncio. Lo que no sabe seguro lo marca como
dudoso para que lo mire una persona.

    python backend/tests/test_almacen_fotos.py
"""
import pathlib
import sys

_RAIZ = pathlib.Path(__file__).resolve().parents[2]
# `dashboard` va al FINAL de sys.path, no al principio. Al principio,
# `import app` resuelve a `dashboard/app.py` en vez del paquete
# `backend/app`, y como sys.path dura toda la sesion de pytest eso
# tumba los tests que corren despues. Paso el 27-oct-2026: el PR #38
# anadio `backend/app/__init__.py` y 13 tests ajenos se cayeron con
# `ModuleNotFoundError: No module named 'plotly'`, importando la pagina
# del dashboard sin querer.
sys.path.append(str(_RAIZ / "dashboard"))

import catalogo_fotos as cf

# Las dos caras reales de AV-FGS/3: el mismo seller SKU, un pack de tres
# perfumes y un pack de perfume + desodorante + crema.
TRES_PERFUMES = {
    "product_id": "1732519334887854997", "sku": "AV-FGS/3",
    "nombre": "Far Away Gold Eau de Parfum 1.7 fl. oz SET of 3 High Quality Fragrance"}
PERFUME_Y_CREMAS = {
    "product_id": "1732503238876959637", "sku": "AV-FGS/3",
    "nombre": "Avon Far Away Gold Eau De Parfum Set, Perfume 1.7, Roll-on Deodorant, Skin Softener"}
MOROCCAN = {
    "product_id": "1731871688121111111", "sku": "AV-FM/2",
    "nombre": "AVON Far Away Moroccan Nights EDP 1.7 fl. oz Set of 2 Regular Edition"}

CATALOGO = cf.Catalogo([TRES_PERFUMES, PERFUME_Y_CREMAS, MOROCCAN])


def test_el_id_del_anuncio_manda_sobre_el_sku():
    """Es el unico dato que distingue los dos packs que comparten SKU."""
    r = CATALOGO.resolver(product_id=PERFUME_Y_CREMAS["product_id"],
                          nombre="lo que sea", seller_sku="AV-FGS/3")
    assert r["certeza"] == cf.SEGURA
    assert r["foto"] == "1732503238876959637.jpg"


def test_el_titulo_exacto_tambien_resuelve_el_choque():
    """Sin ID de anuncio, el titulo del anuncio sigue siendo unico."""
    r = CATALOGO.resolver(nombre=TRES_PERFUMES["nombre"], seller_sku="AV-FGS/3")
    assert r["certeza"] == cf.SEGURA
    assert r["foto"] == "1732519334887854997.jpg"


def test_un_sku_con_dos_anuncios_nunca_devuelve_foto():
    """Lo peligroso no es no saber: es enseñar una de las dos al azar."""
    r = CATALOGO.resolver(nombre="titulo que no esta", seller_sku="AV-FGS/3")
    assert r["certeza"] == cf.DUDOSA
    assert r["foto"] is None
    assert len(r["alternativas"]) == 2


def test_el_sku_de_un_anuncio_distinto_se_marca_en_vez_de_mentir():
    """El caso AV-FM/2 de produccion: 10 ventas lo pedian mal.

    El SKU dice Moroccan Nights y el titulo dice Far Away Original +
    Mesmerize Blue. No son el mismo producto, asi que no hay foto.
    """
    r = CATALOGO.resolver(
        nombre="AVON Far Away Original for her & Mesmerize Blue for him SET of 2",
        seller_sku="AV-FM/2")
    assert r["certeza"] == cf.DUDOSA
    assert r["foto"] is None


def test_una_errata_en_el_titulo_no_tira_la_foto():
    """"permume" por "perfume", una coma de mas, el titulo recortado.

    Son 5.843 lineas reales. Si el parecido fuera exacto, el almacen se
    quedaria sin foto en un tercio de los pedidos por una errata.
    """
    r = CATALOGO.resolver(
        nombre="AVON Far Away Moroccan Nights EDP 1.7 fl. oz Set of 2",
        seller_sku="AV-FM/2")
    assert r["certeza"] == cf.SEGURA
    assert r["foto"] == "1731871688121111111.jpg"


def test_lo_que_no_esta_en_el_catalogo_se_dice_claro():
    r = CATALOGO.resolver(nombre="Producto nuevo", seller_sku="AV-NUEVO")
    assert r["certeza"] == cf.NINGUNA
    assert r["foto"] is None


def test_sin_sku_ni_nombre_no_revienta():
    r = CATALOGO.resolver()
    assert r["certeza"] == cf.NINGUNA


def test_el_parecido_separa_los_dos_casos():
    erratas = cf.parecido(
        "Avon Far Away original & Rare Gold perfume Set of 2",
        "Avon Far Away original & Rare Gold permume Set of 2")
    otro_producto = cf.parecido(
        "AVON Far Away Moroccan Nights EDP 1.7 fl. oz Set of 2 Regular Edition",
        "AVON Far Away Original for her & Mesmerize Blue for him SET of 2")
    assert erratas >= cf.PARECIDO_MINIMO
    assert otro_producto < cf.PARECIDO_MINIMO


def test_el_titulo_recortado_sigue_valiendo():
    """TikTok manda el titulo cortado en muchas lineas; es el mismo anuncio."""
    assert cf.parecido(
        "Rare Pearls Eau de Parfum SET of 2",
        "AVON Rare Pearls Eau de Parfum SET of 2 - Floral Fragrance with Magnolia",
    ) >= cf.PARECIDO_MINIMO


def test_normalizar_quita_acentos_y_puntuacion():
    assert cf.normalizar("Prismé  Vert, EDP!") == "prisme vert edp"


# ── el catalogo que se publica de verdad ────────────────────────────────────

CATALOGO_REAL = cf.cargar()


def test_el_catalogo_publicado_no_esta_vacio():
    assert len(CATALOGO_REAL) > 100, "dashboard/fotos/_indice.csv no se genero"


def test_cada_anuncio_del_indice_tiene_su_foto_en_disco():
    """Un indice que apunta a una foto que no esta es un hueco silencioso."""
    faltan = [e["foto"] for e in CATALOGO_REAL.por_id.values()
              if not (cf.CARPETA / e["foto"]).exists()]
    assert not faltan, f"{len(faltan)} fotos del indice no estan: {faltan[:5]}"


def test_ningun_anuncio_comparte_id_con_otro():
    """Si dos compartieran id, uno pisaria la foto del otro al generarlas."""
    ids = [e["product_id"] for e in CATALOGO_REAL.por_id.values()]
    assert len(ids) == len(set(ids))


def test_los_sku_que_chocan_en_el_catalogo_real_no_dan_foto():
    """Los cinco de verdad, leidos del indice publicado."""
    chocan = [s for s, v in CATALOGO_REAL.por_sku.items() if len(v) > 1]
    assert chocan, "el indice ya no tiene choques: revisa si es cierto"
    for sku in chocan:
        r = CATALOGO_REAL.resolver(nombre="titulo que no existe", seller_sku=sku)
        assert r["foto"] is None, f"{sku} devuelve foto pese a tener varios anuncios"


if __name__ == "__main__":
    fallos = 0
    for _n, _f in sorted(globals().items()):
        if _n.startswith("test_") and callable(_f):
            try:
                _f()
                print(f"  OK    {_n}")
            except AssertionError as e:
                fallos += 1
                print(f"  FALLA {_n}: {str(e)[:160]}")
    print("TODO OK" if not fallos else f"{fallos} fallos")
    sys.exit(1 if fallos else 0)
