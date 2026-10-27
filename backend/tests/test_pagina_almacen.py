"""La pagina del almacen se pinta entera, con Streamlit y la API falseados.

Una pagina de Streamlit no se rompe al importarla: se rompe al pintarla, con
un f-string mal cerrado, una clave que no existe o una division por cero
cuando el pedido viene vacio. Y eso no se ve hasta que alguien abre la
pestana en produccion.

Aqui se sustituyen `streamlit` y `api_client` por dobles que guardan lo que
se les pide, y se recorre la pagina con pedidos de verdad: uno normal, uno
con un pack, uno vacio, uno que no existe. Si algo revienta, revienta aqui.

    python backend/tests/test_pagina_almacen.py
"""
import pathlib
import sys
import types

_RAIZ = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_RAIZ / "dashboard"))


# ── dobles de streamlit y api_client ────────────────────────────────────────

class _Hueco:
    """Vale como columna, como pestana y como contenedor."""

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _Estado(dict):
    pass


class _StreamlitFalso(types.ModuleType):
    def __init__(self):
        super().__init__("streamlit")
        self.session_state = _Estado()
        self.pintado = []
        self.respuestas = {}
        self.recargo = False

    # lo que escribe
    def markdown(self, texto="", **k):
        self.pintado.append(str(texto))

    caption = write = markdown

    def info(self, t="", **k):
        self.pintado.append(f"INFO:{t}")

    def warning(self, t="", **k):
        self.pintado.append(f"AVISO:{t}")

    def error(self, t="", **k):
        self.pintado.append(f"ERROR:{t}")

    def progress(self, v, text=None, **k):
        self.pintado.append(f"PROGRESO:{v}:{text}")

    def dataframe(self, df, **k):
        self.pintado.append(f"TABLA:{len(df)}")

    # maquetacion
    def tabs(self, nombres):
        return [_Hueco() for _ in nombres]

    def columns(self, spec, **k):
        n = spec if isinstance(spec, int) else len(spec)
        return [_Hueco() for _ in range(n)]

    def container(self, **k):
        return _Hueco()

    # entradas: devuelven lo que diga el test
    def text_input(self, label, key=None, **k):
        return self.respuestas.get(key, "")

    def selectbox(self, label, opciones, key=None, **k):
        return (list(opciones) or [None])[0]

    def checkbox(self, label, key=None, **k):
        return bool(self.session_state.get(key))

    def button(self, label, key=None, **k):
        return False

    def rerun(self):
        self.recargo = True

    def __getattr__(self, nombre):
        raise AssertionError(
            f"la pagina usa st.{nombre}, que este doble no imita todavia")


_st = _StreamlitFalso()
sys.modules["streamlit"] = _st

_api = types.ModuleType("api_client")
_api.respuestas = {}


def _api_get(ruta, **k):
    return _api.respuestas.get(ruta)


_api.api_get = _api_get
sys.modules["api_client"] = _api

import catalogo_fotos as cat  # noqa: E402  (despues de instalar los dobles)
import pagina_almacen as pag  # noqa: E402


# ── pedidos de prueba ───────────────────────────────────────────────────────

ANUNCIO = next(iter(cat.cargar().por_id.values()))

PEDIDO_CON_PACK = {
    "orden": "577639922332832484", "encontrada": True, "plataforma": "tiktok",
    "fecha": "2026-10-26T14:03:00", "estado": "Awaiting shipment",
    "total_piezas": 5,
    "lineas": [
        {"seller_sku": "AV-FOBM/3", "sku_plataforma": "1729592630949483413",
         "product_id": None, "producto": ANUNCIO["nombre"], "cantidad": 1,
         "plataforma": "tiktok", "estado": "Awaiting shipment",
         "es_combo": True,
         "componentes": [
             {"producto": "Avon Far Away Eau de Parfume", "sku": "AV-FA",
              "cantidad": 1},
             {"producto": "Far Away Bali EDP", "sku": "AV-FB", "cantidad": 1},
             {"producto": "Producto que no esta en el catalogo",
              "sku": None, "cantidad": 1},
         ]},
        {"seller_sku": "AV-IP", "sku_plataforma": "1729592630949483499",
         "product_id": None, "producto": "Avon Imari Pefume EDT Spray for her",
         "cantidad": 2, "plataforma": "tiktok", "estado": "Awaiting shipment",
         "es_combo": False,
         "componentes": [{"producto": "Avon Imari EDT", "sku": "AV-IP",
                          "cantidad": 2}]},
    ],
}

PEDIDO_VACIO = {"orden": "111111111", "encontrada": True, "plataforma": "temu",
                "fecha": None, "estado": None, "total_piezas": 0, "lineas": []}

NO_ENCONTRADO = {"orden": "999999", "encontrada": False, "lineas": [],
                 "total_piezas": 0,
                 "parecidas": ["577639922332832484", "577639922332832485"]}

SIN_PARECIDAS = {"orden": "999999", "encontrada": False, "lineas": [],
                 "total_piezas": 0, "parecidas": []}

EXISTENCIAS = [
    {"producto": "Avon Far Away Eau de Parfume", "en_almacen": 120,
     "en_fbt": 30, "total": 150, "pendiente_de_enviar": 4, "disponible": 146},
    {"producto": "Avon Imari Pefume EDT", "en_almacen": -73, "en_fbt": 0,
     "total": -73, "pendiente_de_enviar": 2, "disponible": -75},
]


def _pintar(orden_escrita="", datos=None, existencias=None):
    _st.pintado.clear()
    _st.respuestas = {"_alm_orden": orden_escrita, "_alm_busca_stock": "",
                      "_alm_busca_prod": ""}
    _api.respuestas = {}
    if datos is not None:
        _api.respuestas[f"/almacen/orden/{orden_escrita}"] = datos
    if existencias is not None:
        _api.respuestas["/almacen/resumen"] = existencias
    pag.page_almacen()
    return "\n".join(_st.pintado)


# ── que la pagina no se cae ─────────────────────────────────────────────────

def test_un_pedido_con_pack_se_pinta_entero():
    salida = _pintar("577639922332832484", PEDIDO_CON_PACK, EXISTENCIAS)
    assert "577639922332832484" in salida
    assert "AV-FOBM/3" in salida
    assert "pack" in salida
    for pieza in ("Avon Far Away Eau de Parfume", "Far Away Bali EDP"):
        assert pieza in salida, f"falta la pieza {pieza}"


def test_el_pack_se_ve_desmontado_en_piezas():
    """Es el punto de la pagina: el almacen no empaqueta un SKU, empaqueta
    frascos. Un pack de tres tiene que salir como tres lineas."""
    salida = _pintar("577639922332832484", PEDIDO_CON_PACK)
    assert salida.count('class="alm-pieza"') == 4, (
        "tres piezas del pack mas la linea suelta")


def test_la_cuenta_de_piezas_sale_en_la_cinta():
    salida = _pintar("577639922332832484", PEDIDO_CON_PACK)
    assert "Piezas a meter" in salida
    assert "PROGRESO:0.0:0 de 5 piezas" in salida


def test_marcar_una_pieza_mueve_la_barra():
    _st.session_state.clear()
    _st.session_state["_alm_ok_577639922332832484_1_0"] = True
    salida = _pintar("577639922332832484", PEDIDO_CON_PACK)
    assert "2 de 5 piezas" in salida, "la pieza marcada vale 2 unidades"
    _st.session_state.clear()


def test_un_pedido_sin_lineas_no_divide_por_cero():
    salida = _pintar("111111111", PEDIDO_VACIO)
    assert "PROGRESO" not in salida


def test_una_orden_que_no_existe_ofrece_las_parecidas():
    salida = _pintar("999999", NO_ENCONTRADO)
    assert "2 que se parecen" in salida


def test_sin_parecidas_se_dice_claro_y_no_se_ofrece_nada():
    salida = _pintar("999999", SIN_PARECIDAS)
    assert "ERROR:" in salida
    assert "se parecen" not in salida


def test_sin_escribir_nada_no_se_llama_a_la_api():
    salida = _pintar("")
    assert "INFO:" in salida


def test_con_menos_de_seis_digitos_avisa_antes_de_preguntar():
    salida = _pintar("123")
    assert "AVISO:" in salida
    assert "6 digitos" in salida


def test_si_la_api_falla_la_pagina_no_revienta():
    """api_get devuelve None cuando hay un 500 o se cae la red."""
    salida = _pintar("577639922332832484", None)
    assert isinstance(salida, str)


# ── fotos: ninguna dudosa, y los huecos explicados ─────────────────────────

def test_la_pieza_que_no_esta_en_el_catalogo_sale_como_hueco():
    salida = _pintar("577639922332832484", PEDIDO_CON_PACK)
    assert "Sin foto" in salida
    assert "alm-sinfoto" in salida


def test_un_sku_de_dos_anuncios_pinta_aviso_y_no_foto():
    """El caso que hay que proteger: la foto equivocada en vez del aviso."""
    real = cat.cargar()
    chocan = [s for s, v in real.por_sku.items() if len(v) > 1]
    assert chocan, "el catalogo ya no tiene SKU con dos anuncios"
    pedido = {
        "orden": "123456789", "encontrada": True, "plataforma": "tiktok",
        "fecha": None, "estado": None, "total_piezas": 1,
        "lineas": [{"seller_sku": chocan[0], "sku_plataforma": "1",
                    "product_id": None, "producto": "titulo que no existe",
                    "cantidad": 1, "plataforma": "tiktok", "estado": None,
                    "es_combo": False,
                    "componentes": [{"producto": "titulo que no existe",
                                     "sku": chocan[0], "cantidad": 1}]}],
    }
    salida = _pintar("123456789", pedido)
    assert "Comprueba" in salida
    assert "data:image/jpeg;base64" not in salida, (
        "esta pintando una foto que no sabe si es la correcta")


def test_cuando_la_identificacion_es_segura_si_hay_foto():
    pedido = {
        "orden": "123456789", "encontrada": True, "plataforma": "tiktok",
        "fecha": None, "estado": None, "total_piezas": 1,
        "lineas": [{"seller_sku": ANUNCIO["sku"], "sku_plataforma": "1",
                    "product_id": ANUNCIO["product_id"],
                    "producto": ANUNCIO["nombre"], "cantidad": 1,
                    "plataforma": "tiktok", "estado": None, "es_combo": False,
                    "componentes": [{"producto": ANUNCIO["nombre"],
                                     "sku": ANUNCIO["sku"], "cantidad": 1}]}],
    }
    salida = _pintar("123456789", pedido)
    assert "data:image/jpeg;base64" in salida


# ── existencias ────────────────────────────────────────────────────────────

def test_las_existencias_avisan_de_los_negativos():
    salida = _pintar("", existencias=EXISTENCIAS)
    assert "en negativo" in salida
    assert "TABLA:2" in salida


def test_sin_existencias_no_se_pinta_una_tabla_vacia():
    salida = _pintar("", existencias=[])
    assert "TABLA" not in salida


def test_las_existencias_no_traen_ninguna_columna_de_dinero():
    """Doble cinturon: el backend no lo manda, y aqui tampoco se pinta."""
    salida = _pintar("", existencias=EXISTENCIAS).lower()
    for palabra in ("coste", "precio", "valor", "importe", "margen"):
        assert palabra not in salida, f"la pagina del almacen pinta «{palabra}»"


if __name__ == "__main__":
    fallos = 0
    for _n, _f in sorted(globals().items()):
        if _n.startswith("test_") and callable(_f):
            try:
                _f()
                print(f"  OK    {_n}")
            except AssertionError as e:
                fallos += 1
                print(f"  FALLA {_n}: {str(e)[:220]}")
    print("TODO OK" if not fallos else f"{fallos} fallos")
    sys.exit(1 if fallos else 0)
