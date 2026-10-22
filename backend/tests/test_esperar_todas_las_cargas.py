"""El informe diario no debe salir con un canal sin cargar.

Hasta el 21-oct-2026 el gate de frescura decia "fresco" en cuanto llegaba
UNA carga del dia, cualquiera. Con un solo canal eso bastaba. Con TikTok y
TEMU significa esto:

    07:02  se sube TikTok  (tarea programada)
    08:00  sale el email   -> solo TikTok, y el dia queda "ya enviado"
    10:00  se sube TEMU    -> el disparador ve "already_sent_today" y calla

El email no cuadraba ni con TikTok ni con la suma de los dos, y no habia
forma de verlo desde dentro del email.

Modulo puro a proposito: el job de aislamiento del CI solo tiene pytest y
pandas, asi que `frescura_canales` no importa sqlalchemy ni los modelos.
    python backend/tests/test_esperar_todas_las_cargas.py
"""
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from app.services.frescura_canales import (TIPO_POR_CANAL, canales_pendientes,
                                           texto_canales)


# ── El caso que motivo todo ──────────────────────────────────────────────

def test_tiktok_cargado_y_temu_no_bloquea():
    assert canales_pendientes(["tiktok", "temu"], ["tiktok"]) == ["temu"]


def test_los_dos_cargados_no_bloquea():
    assert canales_pendientes(["tiktok", "temu"], ["tiktok", "temu_orders"]) == []


def test_al_reves_temu_cargado_y_tiktok_no():
    """El 17-oct-2026 paso esto: solo se subio TEMU."""
    assert canales_pendientes(["tiktok", "temu"], ["temu_orders"]) == ["tiktok"]


def test_faltan_los_dos():
    assert canales_pendientes(["tiktok", "temu"], []) == ["temu", "tiktok"]


# ── La trampa: "temu" es el extracto, no los pedidos ─────────────────────

def test_el_extracto_de_temu_NO_cuenta_como_los_pedidos():
    """`temu` es el mirror del merchant statement: dinero, no pedidos.

    Si el mapa apuntara a "temu", subir solo el extracto convenceria al
    sistema de que los pedidos del dia ya estan — y saldria el informe sin
    las ventas de TEMU, que es justo el fallo que esto arregla.
    """
    assert TIPO_POR_CANAL["temu"] == "temu_orders"
    assert canales_pendientes(["temu"], ["temu"]) == ["temu"]
    assert canales_pendientes(["temu"], ["temu_orders"]) == []


def test_los_demas_canales_van_a_su_propio_fichero():
    assert TIPO_POR_CANAL["tiktok"] == "tiktok"
    assert TIPO_POR_CANAL["amazon"] == "amazon"
    assert TIPO_POR_CANAL["walmart"] == "walmart"


# ── Lo que NO debe bloquear ──────────────────────────────────────────────

def test_un_canal_inactivo_no_se_exige():
    """Walmart lleva desde junio sin vender.

    Quien llama pasa solo canales con ventas recientes, asi que Walmart no
    llega hasta aqui. Pero si llegara un canal que no sabemos de que fichero
    sale, tampoco puede bloquear: un informe de mas es mejor que quedarse
    sin informe por un canal que nadie ha mapeado.
    """
    assert canales_pendientes(["tiktok", "canal_nuevo"], ["tiktok"]) == []


def test_sin_canales_activos_no_hay_nada_que_esperar():
    assert canales_pendientes([], []) == []


def test_mayusculas_y_espacios():
    assert canales_pendientes([" TikTok ", "TEMU"], [" TikTok "]) == ["temu"]
    assert canales_pendientes(["tiktok"], ["TEMU_ORDERS", "TikTok"]) == []


def test_nones_no_rompen():
    assert canales_pendientes(["tiktok", None, ""], ["tiktok", None]) == []


# ── El texto del aviso ───────────────────────────────────────────────────

def test_texto():
    assert texto_canales(["temu"]) == "TEMU"
    assert texto_canales(["temu", "tiktok"]) == "TEMU y TikTok"
    assert texto_canales(["amazon", "temu", "tiktok"]) == "Amazon, TEMU y TikTok"
    assert texto_canales([]) == ""


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
