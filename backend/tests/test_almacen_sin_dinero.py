"""Por el router del almacen no sale dinero. Comprobado sobre el codigo.

La peticion de Horacio, 27-oct-2026:

    "hay que tener un usuario que no pueda ver nada del reporte ni finance,
     ni numeros ni nada. Solo puede ver una tabla con el resumen de los
     productos que tenemos y la cantidad."

Esconderlo en el dashboard no vale: quien tiene el usuario tiene el token, y
con el token se llama a la API a mano. La unica garantia es que el backend
no lo devuelva, y eso es lo que se mide aqui.

No se lee el fichero como texto, porque entonces las propias explicaciones
(que nombran el coste para decir que NO sale) darian falso positivo. Se
recorre el arbol de sintaxis y se miran solo los nombres y las cadenas que
son codigo de verdad.

    python backend/tests/test_almacen_sin_dinero.py
"""
import ast
import pathlib
import sys

_BACKEND = pathlib.Path(__file__).resolve().parents[1]
_ROUTER = _BACKEND / "app" / "api" / "almacen.py"
_DEPS = _BACKEND / "app" / "dependencies.py"
_ARBOL = ast.parse(_ROUTER.read_text(encoding="utf-8"))


def constante(fichero, nombre):
    """Lee una constante del fichero sin importarlo.

    En el CI de aislamiento solo hay pytest y pandas: importar el router
    arrastraria fastapi y sqlalchemy, que no estan.
    """
    arbol = ast.parse(pathlib.Path(fichero).read_text(encoding="utf-8"))
    for nodo in arbol.body:
        destinos = (nodo.targets if isinstance(nodo, ast.Assign)
                    else [nodo.target] if isinstance(nodo, ast.AnnAssign) else [])
        for d in destinos:
            if isinstance(d, ast.Name) and d.id == nombre:
                return ast.literal_eval(nodo.value)
    raise AssertionError(f"{nombre} ya no esta en {pathlib.Path(fichero).name}")

PALABRAS_DE_DINERO = (
    "price", "cost", "coste", "precio", "revenue", "margin", "margen",
    "importe", "valor", "amount", "commission", "comision", "fee",
    "subtotal", "discount", "descuento", "payout", "profit", "pnl",
)

CAMPOS_PERMITIDOS_EN_RESPUESTA = {
    "orden", "encontrada", "parecidas", "lineas", "total_piezas", "plataforma",
    "fecha", "estado", "seller_sku", "sku_plataforma", "product_id", "producto",
    "cantidad", "es_combo", "componentes", "sku", "anuncios", "unidades",
    "en_almacen", "en_fbt", "total", "pendiente_de_enviar", "disponible",
    "dia", "canales",
}


def _cadenas_de_codigo(arbol):
    """Todas las cadenas del modulo menos las que son docstring."""
    docstrings = set()
    for nodo in ast.walk(arbol):
        if isinstance(nodo, (ast.Module, ast.ClassDef,
                             ast.FunctionDef, ast.AsyncFunctionDef)):
            doc = ast.get_docstring(nodo, clean=False)
            if doc is not None:
                docstrings.add(doc)
    return [n.value for n in ast.walk(arbol)
            if isinstance(n, ast.Constant) and isinstance(n.value, str)
            and n.value not in docstrings]


def _atributos(arbol):
    return [n.attr for n in ast.walk(arbol) if isinstance(n, ast.Attribute)]


def test_el_router_no_nombra_ningun_campo_de_dinero():
    sospechosos = []
    for texto in _cadenas_de_codigo(_ARBOL) + _atributos(_ARBOL):
        bajo = str(texto).lower()
        for palabra in PALABRAS_DE_DINERO:
            if palabra in bajo:
                sospechosos.append(f"{texto}  (por '{palabra}')")
    assert not sospechosos, (
        "el router del almacen toca campos de dinero: " + "; ".join(sospechosos))


def test_la_lista_blanca_de_stock_solo_lleva_unidades():
    for origen, destino in constante(_ROUTER, "COLUMNAS_DE_STOCK").items():
        for palabra in PALABRAS_DE_DINERO:
            assert palabra not in origen.lower(), f"{origen} huele a dinero"
            assert palabra not in destino.lower(), f"{destino} huele a dinero"


def test_el_resumen_filtra_columnas_en_vez_de_devolver_el_frame():
    """Una lista blanca: si el calculo de stock gana una columna de dinero
    manana, no se cuela sola en la respuesta del almacen."""
    fuente = _ROUTER.read_text(encoding="utf-8")
    assert "COLUMNAS_DE_STOCK" in fuente
    assert "to_dict(orient=" not in fuente, (
        "devolver el DataFrame entero sacaria tambien Coste, PRECIO y "
        "ValorInventario")


def test_todos_los_campos_que_se_devuelven_estan_declarados():
    """Si alguien anade una clave nueva, este test obliga a mirarla.

    Solo se miran los diccionarios que se construyen DENTRO de las funciones,
    que son los que acaban en la respuesta. Las constantes de modulo, como la
    lista blanca de columnas, las vigila el test de mas arriba.
    """
    nuevos = set()
    cuerpos = [n for n in ast.walk(_ARBOL)
               if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]
    for nodo in (x for c in cuerpos for x in ast.walk(c)):
        if isinstance(nodo, ast.Dict):
            for clave in nodo.keys:
                if isinstance(clave, ast.Constant) and isinstance(clave.value, str):
                    if clave.value not in CAMPOS_PERMITIDOS_EN_RESPUESTA:
                        nuevos.add(clave.value)
    nuevos -= {"sid"}
    assert not nuevos, (
        f"campos nuevos sin revisar: {sorted(nuevos)}. Si no son dinero, "
        "anadelos a CAMPOS_PERMITIDOS_EN_RESPUESTA.")


def test_todas_las_rutas_piden_permiso_de_almacen():
    """Sin la dependencia, cualquier usuario con token entraria.

    Se cuentan las rutas en vez de fijar un numero: una ruta nueva que se
    olvide el permiso hace fallar esto, y anadir rutas no obliga a venir a
    tocar el test con un numero a mano.
    """
    fuente = _ROUTER.read_text(encoding="utf-8")
    rutas = fuente.count("@router.get(") + fuente.count("@router.post(")
    assert rutas > 0, "el router se ha quedado sin rutas"
    assert fuente.count("Depends(require_almacen)") == rutas, (
        f"{rutas} rutas pero "
        f"{fuente.count('Depends(require_almacen)')} con require_almacen")


def test_el_rol_de_almacen_no_es_admin_ni_viewer():
    roles = constante(_DEPS, "ROLES_DE_ALMACEN")
    assert "warehouse" in roles
    assert "viewer" not in roles, (
        "un viewer no tiene por que ver el almacen, y al reves tampoco")


def test_la_orden_se_busca_siempre_dentro_de_su_tienda():
    """Regla 2 del proyecto: Nokal y Rodmat no se mezclan nunca."""
    fuente = _ROUTER.read_text(encoding="utf-8")
    assert fuente.count("SalesOrder.store_id == user.store_id") == 2, (
        "alguna consulta de pedidos no filtra por tienda")


def test_apuntar_la_consulta_no_puede_romper_la_busqueda():
    """El contador escribe; la busqueda es lo que da de comer.

    Si la insercion del contador propagara su error, un fallo tonto (la
    tabla sin crear, el indice unico a medias) dejaria al almacen sin poder
    buscar un pedido. Tiene que estar envuelta y deshacer la transaccion,
    para no dejar la sesion envenenada.
    """
    arbol = ast.parse(_ROUTER.read_text(encoding="utf-8"))
    funcion = next((n for n in ast.walk(arbol)
                    if isinstance(n, ast.FunctionDef)
                    and n.name == "_apuntar_consulta"), None)
    assert funcion is not None, "_apuntar_consulta ya no esta en el router"
    intentos = [n for n in ast.walk(funcion) if isinstance(n, ast.Try)]
    assert intentos, "la insercion del contador no esta dentro de un try"
    manejadores = [h for i in intentos for h in i.handlers]
    assert manejadores, "el try no captura nada"
    texto = ast.dump(funcion)
    assert "rollback" in texto, "no deshace la transaccion al fallar"
    assert "on_conflict_do_nothing" in texto, (
        "sin esto, recargar la pagina sumaria otra vez")


def test_el_contador_solo_escribe_del_store_de_quien_pregunta():
    """Regla 2: ni leyendo ni escribiendo se cruzan dos tiendas."""
    fuente = _ROUTER.read_text(encoding="utf-8")
    assert "store_id=user.store_id" in fuente, (
        "la fila del contador no se ata al store del usuario")
    assert "AlmacenConsulta.store_id == user.store_id" in fuente, (
        "el contador no filtra por store al leer")

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
