"""Que canales llevan datos viejos, uno por uno.

El 17-oct-2026 no salio el informe diario. La puerta de frescura solo miraba
importaciones de tipo tiktok y amazon, y ese dia lo unico que se subio fue
TEMU: la tienda quedo marcada como stale y ni el envio programado ni el
disparador de recuperacion mandaron nada.

El otro lado del mismo problema: el aviso de "datos desactualizados" del
email miraba la fecha mas reciente de TODAS las plataformas juntas. Con TEMU
subido hoy, el conjunto parece fresco aunque TikTok lleve dos dias sin
cargar, y el informe sale con pinta de completo faltandole el canal que mas
vende. Eso es peor que no mandarlo.

`app.services.frescura_canales` no importa pandas ni sqlalchemy a proposito.
    python backend/tests/test_frescura_canales.py
"""
import pathlib
import sys
from datetime import date

_BACKEND = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_BACKEND))

from app.services.frescura_canales import canales_desactualizados, texto_aviso

HOY = date(2026, 10, 18)


def _v(*pares):
    return [(c, date(2026, 10, d)) for c, d in pares]


def test_todo_al_dia_no_avisa():
    assert canales_desactualizados(_v(("tiktok", 18), ("temu", 18)), HOY) == []


def test_un_dia_de_retraso_todavia_no_avisa():
    """El umbral es 2 dias, el mismo que ya usaba el informe. Un dia de
    retraso es normal: el fichero del dia aun no se ha subido."""
    assert canales_desactualizados(_v(("tiktok", 17)), HOY) == []


def test_avisa_del_canal_concreto_que_falta():
    assert canales_desactualizados(_v(("tiktok", 16), ("temu", 18)), HOY) == [("tiktok", 2)]


def test_el_canal_fresco_NO_tapa_al_atrasado():
    """Es el fallo que hacia salir informes incompletos con pinta de completos.

    Mirando solo la fecha maxima del conjunto, TEMU de hoy dejaba el informe
    sin aviso aunque TikTok llevara cuatro dias sin cargarse.
    """
    ventas = _v(("temu", 18), ("tiktok", 14), ("tiktok", 13))
    assert canales_desactualizados(ventas, HOY) == [("tiktok", 4)]


def test_un_canal_sin_ventas_recientes_no_genera_ruido():
    """Walmart no vende desde junio. Avisar de el todos los dias acabaria
    tapando el aviso que si importa."""
    ventas = [("walmart", date(2026, 6, 26)), ("tiktok", date(2026, 10, 18))]
    assert canales_desactualizados(ventas, HOY) == []


def test_avisa_de_varios_a_la_vez():
    ventas = _v(("tiktok", 15), ("temu", 16), ("amazon", 18))
    assert canales_desactualizados(ventas, HOY) == [("temu", 2), ("tiktok", 3)]


def test_sin_ventas_no_revienta():
    assert canales_desactualizados([], HOY) == []


def test_ignora_filas_sin_canal_o_sin_fecha():
    ventas = [("", date(2026, 10, 14)), (None, date(2026, 10, 14)),
              ("tiktok", None), ("tiktok", date(2026, 10, 15))]
    assert canales_desactualizados(ventas, HOY) == [("tiktok", 3)]


def test_el_canal_se_normaliza():
    assert canales_desactualizados([(" TikTok ", date(2026, 10, 15))], HOY) == [("tiktok", 3)]


def test_el_texto_se_lee_como_una_frase():
    assert texto_aviso([("tiktok", 2)]) == "TikTok (2 días)"
    assert texto_aviso([("tiktok", 1)]) == "TikTok (1 día)"
    assert texto_aviso([("temu", 2), ("tiktok", 3)]) == "TEMU (2 días) y TikTok (3 días)"
    assert texto_aviso([]) == ""


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
