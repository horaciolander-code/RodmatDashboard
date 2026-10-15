"""El aviso de SKU que venden sin descontar.

El banner de Gestion ya los enseña, pero hay que entrar a mirarlo. Esto los
manda por correo en cuanto aparecen.

Lo delicado no es detectarlos: es NO repetirse. Las cargas diarias son cinco
(tres trozos de TikTok, el statement y TEMU). Si avisara en cada una serian
cinco correos identicos al dia, y un aviso que se repite se deja de leer.

Sin BD: se stubean sqlalchemy y app.models para importar el modulo suelto.
    python backend/tests/test_aviso_skus.py
"""
import pathlib
import sys

_BACKEND = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_BACKEND))

import _stubs

_stubs.instalar()

from app.services.scheduled_jobs import _destinatarios_aviso, _html_aviso_skus


class _Tienda:
    def __init__(self, settings):
        self.settings = settings
        self.name = "Rodmat"


def test_manda_a_quien_diga_alert_recipients():
    t = _Tienda({"alert_recipients": ["jefe@rodmatcenter.com"],
                 "report_recipients": ["todos@rodmatcenter.com"]})
    assert _destinatarios_aviso(t) == ["jefe@rodmatcenter.com"]


def test_NO_cae_a_los_destinatarios_del_informe_diario():
    """Este aviso es operativo: va a quien asigna los SKU, no a toda la lista.

    Sin `alert_recipients` no se manda nada. Mejor callado que mandando un
    correo tecnico a cuatro personas que no lo pidieron.
    """
    t = _Tienda({"report_recipients": ["a@x.com", "b@x.com"]})
    assert _destinatarios_aviso(t) == []


def test_admite_un_solo_correo_escrito_como_texto():
    """En settings es facil escribir una cadena en vez de una lista."""
    assert _destinatarios_aviso(_Tienda({"alert_recipients": "jefe@x.com"})) == ["jefe@x.com"]


def test_sin_destinatarios_devuelve_lista_vacia_no_revienta():
    assert _destinatarios_aviso(_Tienda({})) == []
    assert _destinatarios_aviso(_Tienda({"alert_recipients": []})) == []


def test_ignora_entradas_vacias():
    t = _Tienda({"alert_recipients": ["  ", "", "jefe@x.com", None]})
    assert _destinatarios_aviso(t) == ["jefe@x.com"]


def _dedup(actuales, avisados):
    """Replica la regla del aviso: los arreglados salen, los nuevos entran.

    Es la logica de avisar_skus_sin_asignar, aislada para poder probarla sin
    base de datos ni correo.
    """
    avisados = set(avisados) & set(actuales)
    return sorted(set(actuales) - avisados), sorted(avisados)


def test_la_primera_vez_avisa_de_todos():
    nuevos, _ = _dedup({"AV-X/2", "AV-Y"}, set())
    assert nuevos == ["AV-X/2", "AV-Y"]


def test_la_segunda_carga_del_dia_no_vuelve_a_avisar():
    """Cinco importaciones al dia NO pueden ser cinco correos iguales."""
    nuevos, _ = _dedup({"AV-X/2", "AV-Y"}, {"AV-X/2", "AV-Y"})
    assert nuevos == []


def test_solo_avisa_del_que_acaba_de_aparecer():
    nuevos, _ = _dedup({"AV-X/2", "AV-Y", "AV-NUEVO/3"}, {"AV-X/2", "AV-Y"})
    assert nuevos == ["AV-NUEVO/3"]


def test_un_sku_arreglado_sale_de_la_lista_y_si_vuelve_avisa_otra_vez():
    """Si no se limpiara, un SKU que se asigna y luego reaparece quedaria
    marcado como 'ya avisado' para siempre y pasaria desapercibido."""
    _, quedan = _dedup({"AV-Y"}, {"AV-X/2", "AV-Y"})
    assert quedan == ["AV-Y"], "AV-X/2 ya no esta sin asignar: fuera"
    nuevos, _ = _dedup({"AV-X/2", "AV-Y"}, set(quedan))
    assert nuevos == ["AV-X/2"], "al reaparecer tiene que volver a avisar"


def test_el_correo_dice_cuantas_unidades_no_se_han_descontado():
    """El numero que mueve a actuar no es cuantos SKU, es cuanto stock."""
    html = _html_aviso_skus(
        [{"seller_sku": "AV-X/2", "product_name": "Pack de 2", "order_count": 3, "total_qty": 7},
         {"seller_sku": "AV-Y", "product_name": "Suelto", "order_count": 1, "total_qty": 2}],
        ["AV-X/2"], "Rodmat")
    assert "9 unidades" in html, "7 + 2"
    assert "2 SKU sin asignar en Rodmat" in html
    assert html.count("NUEVO") == 1, "solo AV-X/2 es nuevo"


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
