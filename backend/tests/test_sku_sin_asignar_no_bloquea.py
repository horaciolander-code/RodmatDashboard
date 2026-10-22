"""Un SKU sin asignar NO para una carga de pedidos.

El 21-oct-2026 la carga de TEMU "fallaba" por AV-MS/2. No fallaba: el
importador de pedidos de TEMU mete las lineas igual y solo INFORMA de los
SKU sin combo. Era el panel, que pintaba cualquier `unknown_skus` como
"Bloqueado" y ademas escondia el resultado real — asi que una carga que
habia entrado entera parecia haber fallado, y eso rompia el proceso
automatico de Horacio.

Los SKU sin asignar ya salen donde tienen que salir: Gestion > Gestion
Combos, bloque "Procedencia de SKUs", filtro SIN ASIGNAR.

Lo que SI bloquea, y debe seguir bloqueando, es el inventario inicial de
Amazon/Walmart: esas filas van a un `product_id` y no hay donde poner la
fila de un producto que no existe. Dejarlas pasar escribiria inventario
huerfano, que es peor que no cargar.

Tests de fuente: la decision vive en el TSX, que este CI no ejecuta. Lo que
se puede fijar aqui es el contrato del backend, que es lo que lee el panel.
    python backend/tests/test_sku_sin_asignar_no_bloquea.py
"""
import pathlib
import re
import sys

_BACKEND = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_BACKEND))

_SRC = (_BACKEND / "app" / "services" / "import_service.py").read_text(encoding="utf-8")
_TSX = (_BACKEND.parent / "frontend" / "setup-panel" / "src" / "pages"
        / "DataImport.tsx").read_text(encoding="utf-8")


def _bloques_return_con_unknown():
    """Cada `return {...}` del importador que lleva unknown_skus."""
    bloques = []
    for m in re.finditer(r"return \{[^{}]*?unknown_skus[^{}]*?\}", _SRC, re.S):
        bloques.append(m.group(0))
    return bloques


# ── El contrato del backend ──────────────────────────────────────────────

def test_solo_dos_sitios_declaran_bloqueo():
    """Inventario inicial de Amazon y de Walmart. Ni uno mas.

    Si este numero sube, alguien ha puesto a bloquear un importador que
    antes dejaba pasar — y el proceso automatico se vuelve a romper.
    """
    assert _SRC.count('"bloqueado": True') == 2


def test_los_que_bloquean_no_escriben_nada():
    """Bloquear e insertar a la vez seria lo peor de los dos mundos."""
    for b in _bloques_return_con_unknown():
        if '"bloqueado": True' in b:
            assert '"inserted": 0' in b, b


def test_temu_informa_pero_no_bloquea():
    """Las salidas de TEMU llevan unknown_skus y NUNCA bloquean."""
    de_temu = [b for b in _bloques_return_con_unknown()
               if "sin_combo" in b or "resumen[" in b]
    assert de_temu, "no se encontro ninguna salida de TEMU con unknown_skus"
    for b in de_temu:
        assert "bloqueado" not in b, b


def test_el_schema_no_bloquea_por_defecto():
    """Por fuente, no importando el schema: `import_schemas` trae pydantic y
    el job de aislamiento del CI solo tiene pytest y pandas."""
    esquema = (_BACKEND / "app" / "schemas" / "import_schemas.py").read_text(
        encoding="utf-8")
    assert "bloqueado: bool = False" in esquema, (
        "un SKU sin asignar no es un bloqueo: el campo tiene que nacer en False")


def test_los_avisos_nuevos_llegan_al_panel():
    """Sin estar en el schema, el response_model los descarta en silencio."""
    esquema = (_BACKEND / "app" / "schemas" / "import_schemas.py").read_text(
        encoding="utf-8")
    for campo in ("a_confirmar", "sellers_duplicados"):
        assert f"{campo}: List[dict] = []" in esquema, campo


# ── Lo que lee el panel ──────────────────────────────────────────────────

def test_el_panel_decide_por_bloqueado_no_por_unknown_skus():
    """La regresion concreta que se arregla.

    `if (result.unknown_skus && ...length > 0)` para pintar "Bloqueado" es
    justo lo que hacia parecer fallida una carga buena.
    """
    assert "if (result.bloqueado)" in _TSX
    assert "hasUnknownSkus = !!result?.bloqueado" in _TSX


def test_el_panel_manda_a_gestion_combos():
    assert "Gesti" in _TSX and "Combos" in _TSX


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
