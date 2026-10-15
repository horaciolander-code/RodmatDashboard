"""El aviso de los SKU que venden sin descontar inventario.

El banner de Gestion ya los enseña, pero hay que entrar a mirarlo. Esto los
manda por correo en cuanto aparecen. El 15-oct habia 8 vendiendo sin
descontar — 13 unidades que el sistema no habia restado del stock.

Lo delicado no es detectarlos: es NO repetirse. Las cargas diarias son cinco
(tres trozos de TikTok, el statement y TEMU). Si avisara en cada una serian
cinco correos identicos al dia, y un aviso que se repite se deja de leer.

`app.services.aviso_skus` no importa sqlalchemy ni modelos a proposito, asi
que estos tests ejercitan el codigo REAL sin stubear nada.
    python backend/tests/test_aviso_skus.py
"""
import pathlib
import sys

_BACKEND = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_BACKEND))

from app.services.aviso_skus import (calcular_nuevos, destinatarios_aviso,
                                     html_aviso_skus)


class _Tienda:
    def __init__(self, settings):
        self.settings = settings
        self.name = "Rodmat"


# ── a quien se avisa ──────────────────────────────────────────────────────

def test_manda_a_quien_diga_alert_recipients():
    t = _Tienda({"alert_recipients": ["jefe@rodmatcenter.com"],
                 "report_recipients": ["todos@rodmatcenter.com"]})
    assert destinatarios_aviso(t) == ["jefe@rodmatcenter.com"]


def test_NO_cae_a_los_destinatarios_del_informe_diario():
    """Este aviso es operativo: va a quien asigna los SKU, no a toda la lista.

    Sin `alert_recipients` no se manda nada. Mejor callado que mandando un
    correo tecnico a cuatro personas que no lo pidieron.
    """
    t = _Tienda({"report_recipients": ["a@x.com", "b@x.com"]})
    assert destinatarios_aviso(t) == []


def test_admite_un_solo_correo_escrito_como_texto():
    """En settings es facil escribir una cadena en vez de una lista."""
    assert destinatarios_aviso(_Tienda({"alert_recipients": "jefe@x.com"})) == ["jefe@x.com"]


def test_sin_settings_no_revienta():
    assert destinatarios_aviso(_Tienda(None)) == []
    assert destinatarios_aviso(_Tienda({})) == []


def test_ignora_entradas_vacias():
    t = _Tienda({"alert_recipients": ["  ", "", "jefe@x.com", None]})
    assert destinatarios_aviso(t) == ["jefe@x.com"]


# ── de que se avisa, y una sola vez ───────────────────────────────────────

def test_la_primera_vez_avisa_de_todos():
    nuevos, _ = calcular_nuevos({"AV-X/2", "AV-Y"}, set())
    assert nuevos == ["AV-X/2", "AV-Y"]


def test_la_segunda_carga_del_dia_no_vuelve_a_avisar():
    """Cinco importaciones al dia NO pueden ser cinco correos iguales."""
    nuevos, _ = calcular_nuevos({"AV-X/2", "AV-Y"}, {"AV-X/2", "AV-Y"})
    assert nuevos == []


def test_solo_avisa_del_que_acaba_de_aparecer():
    nuevos, _ = calcular_nuevos({"AV-X/2", "AV-Y", "AV-NUEVO/3"}, {"AV-X/2", "AV-Y"})
    assert nuevos == ["AV-NUEVO/3"]


def test_un_sku_arreglado_sale_de_la_lista_y_si_vuelve_avisa_otra_vez():
    """Si no se limpiara, un SKU que se asigna y luego reaparece quedaria
    marcado como 'ya avisado' para siempre y pasaria desapercibido."""
    _, quedan = calcular_nuevos({"AV-Y"}, {"AV-X/2", "AV-Y"})
    assert quedan == {"AV-Y"}, "AV-X/2 ya no esta sin asignar: fuera de la lista"
    nuevos, _ = calcular_nuevos({"AV-X/2", "AV-Y"}, quedan)
    assert nuevos == ["AV-X/2"], "al reaparecer tiene que volver a avisar"


def test_los_espacios_no_cuelan_un_duplicado():
    """Un SKU con espacios alrededor no puede contarse como nuevo."""
    nuevos, _ = calcular_nuevos({" AV-X/2 "}, {"AV-X/2"})
    assert nuevos == []


# ── el correo ─────────────────────────────────────────────────────────────

def test_el_correo_dice_cuantas_unidades_no_se_han_descontado():
    """El numero que mueve a actuar no es cuantos SKU, es cuanto stock."""
    html = html_aviso_skus(
        [{"seller_sku": "AV-X/2", "product_name": "Pack de 2", "order_count": 3, "total_qty": 7},
         {"seller_sku": "AV-Y", "product_name": "Suelto", "order_count": 1, "total_qty": 2}],
        ["AV-X/2"], "Rodmat")
    assert "9 unidades" in html, "7 + 2"
    assert "2 SKU sin asignar en Rodmat" in html
    assert html.count("NUEVO") == 1, "solo AV-X/2 es nuevo"


def test_el_correo_ordena_por_unidades_el_que_mas_duele_arriba():
    html = html_aviso_skus(
        [{"seller_sku": "POCO", "total_qty": 1}, {"seller_sku": "MUCHO", "total_qty": 50}],
        [], "Rodmat")
    assert html.index("MUCHO") < html.index("POCO")


def test_el_correo_aguanta_campos_vacios():
    """Los SKU sin Seller SKU llegan con el nombre a None."""
    html = html_aviso_skus([{"seller_sku": "1732755437778604949"}], [], "Rodmat")
    assert "1732755437778604949" in html
    assert "0 unidades" in html


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
