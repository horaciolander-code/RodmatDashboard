"""El asunto del informe no puede usar variables que ya no existen.

El 19-oct-2026 el cambio del aviso por canal quito `data_lag_days` y dejo su
uso 360 lineas mas abajo, al construir el asunto. Compilaba perfectamente y
moria con NameError en CADA ejecucion: el informe diario estuvo SIETE DIAS
sin salir, y en report_logs solo se veia "failed", sin decir por que.

La red de seguridad general es pyflakes en el CI, que caza cualquier nombre
indefinido. Aqui solo se fija este caso concreto, porque construir el
informe de verdad necesita base de datos.

    python backend/tests/test_asunto_informe.py
"""
import pathlib
import sys

_BACKEND = pathlib.Path(__file__).resolve().parents[1]
_SRC = (_BACKEND / "app" / "services" / "daily_report_service.py").read_text(
    encoding="utf-8")


def _codigo_sin_comentarios():
    """El comentario que documenta el fallo nombra la variable a proposito."""
    return "\n".join(linea.split("#")[0] for linea in _SRC.splitlines())


def test_data_lag_days_no_vuelve():
    assert "data_lag_days" not in _codigo_sin_comentarios(), (
        "vuelve a usarse la variable que dejo el informe siete dias sin salir")


def test_el_asunto_avisa_cuando_faltan_datos():
    """Si falta un canal tiene que verse en el ASUNTO.

    Un aviso solo en el cuerpo se pierde entre los correos de la manana, que
    es justo lo que hay que evitar: un informe incompleto con pinta normal.
    """
    i = _SRC.index("Build subject")
    bloque = _SRC[i:i + 900]
    assert "_sin_cargar" in bloque, "el asunto no mira si faltan cargas de hoy"
    assert "_viejos" in bloque, "el asunto no mira si hay canales atrasados"


def test_el_asunto_normal_sigue_llevando_las_cifras():
    """El caso bueno no puede perder lo que ya daba: dinero y pedidos."""
    i = _SRC.index("Build subject")
    bloque = _SRC[i:i + 900]
    assert "rev_y" in bloque and "orders_y" in bloque


if __name__ == "__main__":
    fallos = 0
    for _n, _f in sorted(globals().items()):
        if _n.startswith("test_") and callable(_f):
            try:
                _f()
                print(f"  OK    {_n}")
            except AssertionError as e:
                fallos += 1
                print(f"  FALLA {_n}: {str(e)[:180]}")
    print("TODO OK" if not fallos else f"{fallos} fallos")
    sys.exit(1 if fallos else 0)
