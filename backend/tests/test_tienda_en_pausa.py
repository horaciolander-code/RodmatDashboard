"""Una tienda en pausa no gasta, no avisa y no deja entrar.

Nokal tiene la cuenta bloqueada y no sube nada desde el 25-ago-2026. El
sistema seguia tratandola como viva: 30 avisos de "informe NO enviado" en 30
dias, y sus 7 usuarios admin (5 de fuera) entrando al panel con normalidad.

Poner `agents_enabled` a false NO servia: el planificador registra el salto y
manda el aviso ANTES de mirar esa opcion. De ahi este interruptor.

    python backend/tests/test_tienda_en_pausa.py
"""
import pathlib
import sys

_BACKEND = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_BACKEND))

from app.services.pausa import CLAVE, en_pausa, motivo

_JOBS = (_BACKEND / "app" / "services" / "scheduled_jobs.py").read_text(encoding="utf-8")
_AUTH = (_BACKEND / "app" / "api" / "auth.py").read_text(encoding="utf-8")


# ── la lectura del interruptor ───────────────────────────────────────────

def test_sin_la_clave_la_tienda_esta_viva():
    """Lo que no esta en pausa, sigue como siempre. Es lo mas importante:
    este cambio no puede apagarle los informes a Rodmat."""
    assert en_pausa({}) is False
    assert en_pausa(None) is False
    assert en_pausa({"report_enabled": True}) is False


def test_en_pausa():
    assert en_pausa({CLAVE: True}) is True


def test_false_explicito_no_pausa():
    assert en_pausa({CLAVE: False}) is False


def test_texto_porque_el_json_se_edita_a_mano():
    """`settings` es un JSON que alguien puede tocar desde Supabase.

    Si escribe la cadena "true" en vez del booleano, bool("false") daria
    True y apagaria la tienda al reves de como se pretendia.
    """
    assert en_pausa({CLAVE: "true"}) is True
    assert en_pausa({CLAVE: "True"}) is True
    assert en_pausa({CLAVE: "si"}) is True
    assert en_pausa({CLAVE: "false"}) is False, "bool('false') seria True: el fallo clasico"
    assert en_pausa({CLAVE: ""}) is False


def test_el_motivo_se_puede_personalizar():
    assert motivo({}) == ""
    assert "pausa" in motivo({CLAVE: True}).lower()
    assert "cuenta bloqueada" in motivo({CLAVE: True, "on_hold_reason": "cuenta bloqueada"})


# ── donde tiene que estar mirado ─────────────────────────────────────────

def test_los_dos_planificadores_lo_miran():
    """Informes Y agentes. Solo uno de los dos deja la mitad del ruido."""
    assert _JOBS.count("en_pausa(") >= 3, "faltan sitios donde comprobarlo"
    assert "skipped_on_hold" in _JOBS


def test_los_agentes_lo_miran_ANTES_de_la_frescura():
    """El aviso se manda en la rama de datos viejos.

    Si la comprobacion fuera despues, la tienda en pausa seguiria generando
    exactamente el aviso diario que se quiere quitar.
    """
    # Acotado a run_scheduled_agents: la llamada a check_data_freshness
    # aparece antes en el fichero, dentro de run_scheduled_reports.
    cuerpo = _JOBS[_JOBS.index("def run_scheduled_agents("):]
    i_pausa = cuerpo.index("if en_pausa(store.settings or {}):")
    i_fresco = cuerpo.index("f = check_data_freshness(")
    assert i_pausa < i_fresco, "la pausa debe comprobarse antes de medir frescura"


def test_el_login_lo_rechaza():
    assert "en_pausa" in _AUTH and "HTTP_403_FORBIDDEN" in _AUTH


def test_el_login_comprueba_la_pausa_DESPUES_de_la_contrasena():
    """Si no, cualquiera podria averiguar que correos existen probando."""
    i_pass = _AUTH.index("verify_password")
    i_pausa = _AUTH.index("en_pausa(_store.settings")
    assert i_pass < i_pausa


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
