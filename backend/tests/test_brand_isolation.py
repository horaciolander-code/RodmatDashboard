"""Tests de aislamiento multi-tenant / multi-marca.

Escritos el 2026-09-20 despues de un dia entero arreglando fugas de marca.
Cada test de aqui corresponde a un bug REAL que llego a produccion. No hacen
falta ni base de datos ni red: son analisis estatico del codigo, para que
corran en el CI en segundos y rompan el build antes de desplegar.

Los 4 fallos que cubren:
  1. Un endpoint nuevo sin parametro brand_slug.
  2. Una funcion de servicio que ACEPTA brand_slug y no lo usa
     (paso con get_top_combos y get_finances: aceptaban la marca y la tiraban).
  3. Un guard que comprueba una columna que no existe
     (paso con `if "brand_id" in df.columns` cuando se llama "Brand ID":
      la condicion era siempre falsa y el filtro no se aplicaba NUNCA).
  4. Una pagina del dashboard que llama a un fetch_* sin pasarle la marca
     (paso con 18 llamadas: Top Creadores, Ordenes Check, todo Afiliados...).
"""
from __future__ import annotations
import ast
import pathlib

ROOT = pathlib.Path(__file__).resolve().parent.parent      # backend/
REPO = ROOT.parent                                          # repo/
API = ROOT / "app" / "api"
DASHBOARD = REPO / "dashboard" / "app.py"

# Rutas que por diseno NO llevan marca, con el motivo. Anadir aqui obliga a
# justificarlo: si no esta en la lista y no acepta brand_slug, el test falla.
SIN_MARCA_JUSTIFICADO = {
    "/beta-check":        "solo dice si el usuario ve la pagina de Chat IA",
    "/brands":            "es la lista de marcas que alimenta el propio selector",
    "/brands/product-map": "mapa producto->marca, necesario para construir el filtro",
    "/auth/me":           "identidad del usuario",
    "/health":            "healthcheck",
    "/stores/me":         "configuracion del tenant, no datos de negocio",
}


def _routers():
    for f in sorted(API.glob("*.py")):
        if f.name != "__init__.py":
            yield f, ast.parse(f.read_text(encoding="utf-8"))


def _get_routes(tree):
    """(nombre_funcion, path, params) de cada @router.get."""
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef):
            continue
        for dec in node.decorator_list:
            if (isinstance(dec, ast.Call) and isinstance(dec.func, ast.Attribute)
                    and dec.func.attr == "get"
                    and isinstance(dec.func.value, ast.Name)
                    and dec.func.value.id == "router"):
                path = dec.args[0].value if dec.args and isinstance(dec.args[0], ast.Constant) else ""
                params = {a.arg for a in node.args.args} | {a.arg for a in node.args.kwonlyargs}
                yield node.name, path, params


def test_endpoints_de_lectura_aceptan_brand_slug():
    """Bug 1 — un GET que devuelve datos del tenant debe poder acotarse por marca."""
    fallos = []
    for f, tree in _routers():
        # routers que no exponen datos de negocio
        if f.stem in {"auth", "admin", "health", "stores", "chat", "agents", "brands", "demo"}:
            continue
        for fn, path, params in _get_routes(tree):
            if path in SIN_MARCA_JUSTIFICADO:
                continue
            # los detalles por id ya estan acotados por el propio id + store
            if "{" in path:
                continue
            if "brand_slug" not in params:
                fallos.append(f"{f.name}::{fn}  (GET {path})")
    assert not fallos, (
        "Estos endpoints de lectura no aceptan brand_slug. Anade el parametro y "
        "resuelvelo con _resolve_brand_slug/get_user_brand_id, o justificalo en "
        "SIN_MARCA_JUSTIFICADO:\n  - " + "\n  - ".join(fallos))


def test_brand_slug_que_se_acepta_se_usa():
    """Bug 2 — aceptar brand_slug y no usarlo es peor que no aceptarlo:
    la llamada parece filtrada y no lo esta."""
    fallos = []
    for py in sorted((ROOT / "app" / "services").rglob("*.py")):
        tree = ast.parse(py.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.FunctionDef):
                continue
            params = {a.arg for a in node.args.args} | {a.arg for a in node.args.kwonlyargs}
            if "brand_slug" not in params:
                continue
            cuerpo = ast.dump(ast.Module(body=node.body, type_ignores=[]))
            # el parametro debe aparecer en el cuerpo, no solo en la firma
            if "'brand_slug'" not in cuerpo and '"brand_slug"' not in cuerpo:
                fallos.append(f"{py.name}::{node.name}")
    assert not fallos, (
        "Estas funciones aceptan brand_slug y NO lo usan en el cuerpo:\n  - "
        + "\n  - ".join(fallos))


def test_no_se_comprueba_una_columna_de_marca_inexistente():
    """Bug 3 — el guard `if "brand_id" in df.columns` era siempre falso porque la
    columna se llama "Brand ID". Un filtro que no falla: simplemente no filtra."""
    CANONICAS = {'"Brand ID"', "'Brand ID'", '"Brand_ID"', "'Brand_ID'"}
    fallos = []
    for py in sorted((ROOT / "app").rglob("*.py")):
        for i, linea in enumerate(py.read_text(encoding="utf-8").splitlines(), 1):
            if ".columns" not in linea:
                continue
            if linea.lstrip().startswith("#"):      # comentarios que citan el bug viejo
                continue
            if ('"brand_id"' in linea or "'brand_id'" in linea) and not any(c in linea for c in CANONICAS):
                fallos.append(f"{py.name}:{i}  {linea.strip()[:90]}")
    assert not fallos, (
        "Guard sobre una columna de marca que no existe con ese nombre. Las columnas "
        'reales son "Brand ID" (ordenes) y "Brand_ID" (stock):\n  - ' + "\n  - ".join(fallos))


def test_el_dashboard_pasa_la_marca_en_cada_llamada():
    """Bug 4 — el fallo mas caro del dia: el backend filtraba bien pero el
    Streamlit no mandaba la marca, asi que el filtro no se ejecutaba nunca."""
    if not DASHBOARD.exists():
        return
    import re
    texto = DASHBOARD.read_text(encoding="utf-8")
    lineas = texto.splitlines()

    # que funciones fetch_* aceptan marca
    acepta = {}
    for i, l in enumerate(lineas):
        m = re.match(r"^def (fetch_\w+)\(", l)
        if m:
            firma, j = l, i
            while firma.count("(") > firma.count(")") and j + 1 < len(lineas):
                j += 1
                firma += lineas[j]
            acepta[m.group(1)] = "brand_slug" in firma

    NEUTRALES = {"fetch_brands"}   # alimenta el propio selector de marca
    fallos = []
    paginas = [(i, re.match(r"^def (page_\w+)", l).group(1))
               for i, l in enumerate(lineas) if re.match(r"^def page_\w+", l)]
    paginas.append((len(lineas), "FIN"))
    for k in range(len(paginas) - 1):
        ini, nombre = paginas[k]
        cuerpo = lineas[ini:paginas[k + 1][0]]
        for i, l in enumerate(cuerpo):
            for m in re.finditer(r"\b(fetch_\w+)\(", l):
                fn = m.group(1)
                if fn in NEUTRALES or fn not in acepta:
                    continue
                llamada, j = l, i
                while llamada.count("(") > llamada.count(")") and j + 1 < len(cuerpo):
                    j += 1
                    llamada += cuerpo[j]
                if not acepta[fn]:
                    fallos.append(f"{nombre} -> {fn}()  [la funcion no acepta marca]")
                elif "_bs" not in llamada and "brand_slug" not in llamada:
                    fallos.append(f"{nombre} -> {fn}()  [no se le pasa la marca]")
    assert not fallos, (
        "Llamadas del dashboard que no propagan la marca seleccionada:\n  - "
        + "\n  - ".join(sorted(set(fallos))))


def test_los_agentes_respetan_la_marca():
    """Cada agente debe filtrar su snapshot y sus destinatarios por marca.
    TIMELESS importaba get_brand_recipients y no lo usaba: mandaba el cierre
    mensual de una marca a la lista entera del store."""
    agentes = ROOT / "app" / "services" / "agents"
    fallos = []
    for f in sorted(agentes.glob("*_agent.py")):
        s = f.read_text(encoding="utf-8")
        if "def run(" not in s or "brand_slug" not in s.split("def run(")[1][:400]:
            fallos.append(f"{f.name}: run() no acepta brand_slug")
            continue
        usa_marca = ("get_brand_recipients(db, store_id, brand_slug" in s
                     or "get_brand_recipients_legacy(store, brand)" in s)
        if not usa_marca:
            fallos.append(f"{f.name}: elige destinatarios sin tener en cuenta la marca")
    assert not fallos, "Agentes con el scoping de marca incompleto:\n  - " + "\n  - ".join(fallos)
