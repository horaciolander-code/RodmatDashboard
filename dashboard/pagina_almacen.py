"""La pagina del almacen: empaquetar un pedido mirando las fotos.

Vive aparte de app.py por dos razones. app.py ya pasa de 3.300 lineas, y
sobre todo: aqui se puede importar y probar sin arrancar Streamlit ni la API,
que es lo que hace `backend/tests/test_pagina_almacen.py`.
"""
import base64 as _b64
import datetime as _dt

import pandas as pd
import streamlit as st
from api_client import api_get, api_post

import catalogo_fotos as _cat

# ============================================================================
#  ALMACEN — empaquetado por numero de orden
#  Quien entra aqui prepara cajas. Ni dinero, ni ventas, ni informes: el
#  backend (api/almacen.py) ya no los devuelve, asi que no hay nada que
#  esconder aqui. Esta pagina solo tiene que ser rapida de leer de pie, con
#  un movil o una pantalla a un metro.
# ============================================================================


_CATALOGO = _cat.cargar()

_CSS_ALMACEN = """
<style>
.alm-h1{font-size:26px;font-weight:800;letter-spacing:-.4px;
  background:linear-gradient(90deg,#00D4FF,#7B61FF);-webkit-background-clip:text;
  -webkit-text-fill-color:transparent;margin:4px 0 2px;}
.alm-sub{color:#8892b0;font-size:13px;margin-bottom:14px;}
.alm-cinta{display:flex;gap:10px;flex-wrap:wrap;margin:10px 0 18px;}
.alm-dato{background:linear-gradient(135deg,#0f142f,#141a3d);
  border:1px solid rgba(123,97,255,.18);border-radius:10px;padding:10px 16px;}
.alm-dato-l{color:#8892b0;font-size:10px;text-transform:uppercase;letter-spacing:.8px;}
.alm-dato-v{color:#e4e9ff;font-size:19px;font-weight:700;line-height:1.2;margin-top:2px;}
.alm-sku{font-family:Menlo,Consolas,monospace;font-size:19px;font-weight:700;color:#00D4FF;}
.alm-nombre{color:#e4e9ff;font-size:15px;line-height:1.45;margin:4px 0 2px;}
.alm-cant{display:inline-block;background:rgba(0,255,136,.12);color:#00FF88;
  border:1px solid rgba(0,255,136,.35);border-radius:8px;padding:3px 12px;
  font-size:17px;font-weight:800;font-family:Menlo,Consolas,monospace;}
.alm-pieza{color:#e4e9ff;font-size:15px;line-height:1.3;}
.alm-pieza-sku{color:#8892b0;font-size:11px;font-family:Menlo,Consolas,monospace;}
.alm-x{color:#00FF88;font-size:17px;font-weight:800;font-family:Menlo,Consolas,monospace;}
.alm-aviso{background:rgba(255,159,69,.1);border:1px solid rgba(255,159,69,.4);
  border-left:4px solid #FF9F45;border-radius:8px;padding:10px 14px;color:#FF9F45;
  font-size:13px;margin:6px 0;}
.alm-sinfoto{background:rgba(136,146,176,.07);border:1px dashed rgba(136,146,176,.35);
  border-radius:10px;display:flex;align-items:center;justify-content:center;
  color:#8892b0;font-size:12px;text-align:center;padding:8px;}
.alm-etq{display:inline-block;font-size:10px;font-weight:700;letter-spacing:.6px;
  text-transform:uppercase;border-radius:6px;padding:2px 8px;margin-left:8px;
  background:rgba(123,97,255,.15);color:#7B61FF;border:1px solid rgba(123,97,255,.4);}

/* La caja del numero de orden, grande. Se teclea de pie, con el movil en una
   mano, y antes quedaba debajo del contador y de dos parrafos: habia que
   bajar la pagina para escribir. Ahora va arriba del todo y se ve desde
   lejos. 30px es lo que se lee a un metro de la pantalla. */
div[data-testid="stTextInput"] input{font-size:30px !important;height:64px;
  font-weight:700;letter-spacing:1px;font-family:Menlo,Consolas,monospace;
  color:#e4e9ff;}
div[data-testid="stTextInput"] label p{font-size:15px;font-weight:700;}
div[data-testid="stExpander"] summary p{font-size:17px;font-weight:700;}
</style>
"""


# ── la API, con cache ───────────────────────────────────────────────────────
# Streamlit repinta la pagina entera en cada interaccion. Sin cache, marcar
# una casilla costaba tres viajes a la API (contador, pedido y existencias) y
# ademas una escritura en la base, porque /almacen/orden apunta la consulta
# del dia. Con la ficha dentro de un fragmento (ver _ficha_del_pedido) las
# casillas ya no repintan la pagina; la cache cubre el resto de pasadas.


def _inquilino():
    """Tienda y usuario, para que una cache global no cruce clientes.

    `st.cache_data` se comparte entre sesiones de la misma instancia de
    Streamlit. app.py ya la limpia cuando cambia el usuario, pero eso es
    higiene de otro fichero y de otra pasada: aqui el inquilino va DENTRO de
    la clave, asi que dos tiendas no pueden compartir entrada aunque nadie
    limpie nada. Regla 2: Nokal y Rodmat no se mezclan.
    """
    usuario = st.session_state.get("cached_user") or {}
    return f"{usuario.get('store_id') or '?'}:{usuario.get('email') or '?'}"


@st.cache_data(ttl=120, show_spinner=False)
def _pedido_desde_la_api(inquilino, numero):
    return api_get(f"/almacen/orden/{numero}")


@st.cache_data(ttl=30, show_spinner=False)
def _contador_desde_la_api(inquilino):
    return api_get("/almacen/contador")


@st.cache_data(ttl=300, show_spinner=False)
def _existencias_desde_la_api(inquilino):
    return api_get("/almacen/resumen")


def _sin_pegar_el_fallo(valor, cacheada):
    """Devuelve el valor, pero no deja un fallo metido en la cache.

    `api_get` devuelve None cuando la API da error o se cae la red. Cachear
    ese None dejaria la pagina rota los dos minutos siguientes aunque la API
    ya hubiera vuelto.
    """
    if valor is None:
        cacheada.clear()
    return valor


def _pedido(numero):
    return _sin_pegar_el_fallo(_pedido_desde_la_api(_inquilino(), numero),
                               _pedido_desde_la_api)


def _contador():
    return _sin_pegar_el_fallo(_contador_desde_la_api(_inquilino()),
                               _contador_desde_la_api)


def _existencias():
    return _sin_pegar_el_fallo(_existencias_desde_la_api(_inquilino()),
                               _existencias_desde_la_api)


@st.cache_data(ttl=3600, show_spinner=False)
def _foto_en_base64(nombre_fichero):
    """El base64 de una foto. En cache: el fichero en disco no cambia.

    Sin esto se leia de disco y se recodificaba en cada pasada, y son cuatro
    o cinco fotos por pedido.
    """
    ruta = _cat.CARPETA / nombre_fichero
    if not ruta.exists():
        return None
    return _b64.b64encode(ruta.read_bytes()).decode()


def _foto_incrustada(nombre_fichero, alto):
    """La foto como etiqueta img. Se incrusta para maquetar la ficha entera.

    Son cuatro o cinco fotos por pedido, de 30 KB cada una: incrustarlas sale
    mas barato que una peticion por imagen y deja fijar el alto exacto, que es
    lo que hace que la lista se lea de un vistazo.
    """
    datos = _foto_en_base64(nombre_fichero)
    if datos is None:
        return None
    return (f'<img src="data:image/jpeg;base64,{datos}" '
            f'style="height:{alto}px;width:{alto}px;object-fit:contain;'
            f'background:#fff;border-radius:10px;'
            f'border:1px solid rgba(0,212,255,.2);">')


def _hueco(texto, alto):
    return (f'<div class="alm-sinfoto" style="height:{alto}px;width:{alto}px;">'
            f'{texto}</div>')


def _pintar_foto(resuelta, alto):
    """Pinta la foto, o explica por que no hay. Nunca una foto dudosa.

    Que el almacen se quede sin foto cuesta una comprobacion. Que vea la
    foto equivocada cuesta un paquete devuelto.
    """
    if resuelta["certeza"] == _cat.SEGURA and resuelta["foto"]:
        html = _foto_incrustada(resuelta["foto"], alto)
        if html:
            st.markdown(html, unsafe_allow_html=True)
        else:
            st.markdown(_hueco("Falta el fichero", alto), unsafe_allow_html=True)
    elif resuelta["certeza"] == _cat.DUDOSA:
        st.markdown(_hueco("Comprueba<br>este producto", alto),
                    unsafe_allow_html=True)
    else:
        st.markdown(_hueco("Sin foto<br>en el catalogo", alto),
                    unsafe_allow_html=True)


def _clave_pieza(orden, i, j):
    return f"_alm_ok_{orden}_{i}_{j}"


# Pasado este plazo, el pedido se avisa como viejo. No es un numero redondo
# porque si: de los 28.119 pedidos de la tienda, 26.397 tienen mas de un mes
# (27-oct-2026). Quien escribe seis digitos y acierta de casualidad lo normal
# es que acierte en uno viejo, no en el de hoy.
DIAS_PARA_AVISAR_DE_VIEJO = 30


def _dias_desde(fecha_iso):
    """Cuantos dias hace de una fecha ISO. None si no se puede saber.

    Se quita la zona horaria antes de restar: el backend manda la fecha tal
    como esta en la base, que unas veces trae zona y otras no, y restar una
    con zona de una sin ella revienta.
    """
    if not fecha_iso:
        return None
    try:
        momento = _dt.datetime.fromisoformat(str(fecha_iso).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if momento.tzinfo is not None:
        momento = momento.replace(tzinfo=None)
    return (_dt.datetime.now() - momento).days


def _parecidas_normalizadas(parecidas):
    """Acepta la lista nueva (con fecha) y la vieja (numeros pelados).

    El backend y el dashboard son dos servicios distintos en Railway: durante
    un despliegue uno puede ir por delante del otro unos segundos. Si llega la
    forma vieja, se pinta sin fecha en vez de romperse.
    """
    salida = []
    for p in parecidas or []:
        if isinstance(p, dict):
            salida.append(p)
        else:
            salida.append({"orden": str(p), "fecha": None, "plataforma": None})
    return salida


def _etiqueta_de_parecida(p):
    dias = _dias_desde(p.get("fecha"))
    trozos = [str(p.get("orden"))]
    if p.get("fecha"):
        trozos.append(str(p["fecha"])[:10])
    if p.get("plataforma"):
        trozos.append(str(p["plataforma"]).upper())
    if dias is not None and dias >= DIAS_PARA_AVISAR_DE_VIEJO:
        trozos.append(f"hace {dias} dias")
    return "  ·  ".join(trozos)


def page_almacen():
    st.markdown(_CSS_ALMACEN, unsafe_allow_html=True)
    tab_pack, tab_stock, tab_prod = st.tabs(
        ["Empaquetar", "Existencias", "Buscar producto"])
    with tab_pack:
        _almacen_empaquetar()
    with tab_stock:
        _almacen_existencias()
    with tab_prod:
        _almacen_buscar_producto()


def _pintar_contador():
    """La cuenta de ordenes abiertas hoy, con el desglose por canal.

    Va arriba del todo y es lo primero que se ve: el almacen lleva asi su
    propia cuenta del dia sin tener que preguntarsela a nadie.

    Si la API no responde no se pinta nada. Un contador es informacion de
    apoyo: que falte no puede estorbar a quien esta empaquetando.
    """
    dias = _contador()
    if not dias:
        return

    hoy = dias[0]
    canales = "".join(
        f'<div class="alm-dato"><div class="alm-dato-l">{canal}</div>'
        f'<div class="alm-dato-v">{cuantas}</div></div>'
        for canal, cuantas in (hoy.get("canales") or {}).items())
    anteriores = "  ·  ".join(
        f'{d["dia"][8:10]}/{d["dia"][5:7]} {d["total"]}' for d in dias[1:])

    st.markdown(
        f'<div class="alm-cinta">'
        f'<div class="alm-dato" style="border-color:rgba(0,255,136,.35)">'
        f'<div class="alm-dato-l">Ordenes revisadas hoy</div>'
        f'<div class="alm-dato-v" style="color:#00FF88">{hoy["total"]}</div></div>'
        f'{canales}</div>'
        + (f'<div class="alm-sub" style="margin-top:-8px">Dias anteriores: '
           f'{anteriores}</div>' if anteriores else ""),
        unsafe_allow_html=True)


def _abrir_la_parecida(etiquetas):
    """Lleva la orden elegida a la caja de busqueda.

    Va como callback del boton y no dentro de un `if st.button(...)`: en ese
    momento la caja de busqueda ya esta pintada, y Streamlit no deja escribir
    `st.session_state` de un widget ya pintado — revienta con
    StreamlitWidgetAlreadyInstantiatedError. Los callbacks corren al principio
    de la pasada siguiente, antes de pintar nada, que es cuando si se puede.

    Paso en produccion el 27-oct-2026, con el almacen ya usandolo.
    """
    elegida = st.session_state.get("_alm_parecidas")
    if elegida in etiquetas:
        st.session_state["_alm_orden"] = etiquetas[elegida]


def _caja_de_actualizar():
    """Subir el export de TikTok desde aqui, sin depender de nadie.

    Hasta ahora el fichero solo se podia subir desde el panel, al que el
    almacen no entra: si el dueño no estaba disponible, se empaquetaba con
    los pedidos de ayer y los de hoy no aparecian al buscarlos.

    No hace falta tocar el backend: `/api/import/orders` pide sesion, no rol
    (`Depends(get_current_user)`), asi que el almacen ya podia llamarlo — lo
    que faltaba era la puerta. Va plegada para no estorbar a quien solo
    viene a empaquetar, que es lo normal.
    """
    with st.expander("Actualizar pedidos  ·  subir el CSV de TikTok",
                     expanded=False):
        st.markdown(
            '<div class="alm-sub">Descarga el fichero <b>All order</b> de '
            'TikTok Shop y sueltalo aqui. Tarda un par de minutos; cuando '
            'termine, los pedidos nuevos ya se pueden buscar.</div>',
            unsafe_allow_html=True)
        fichero = st.file_uploader("CSV de pedidos de TikTok", type=["csv"],
                                   key="_alm_csv")
        if not st.button("Subir y actualizar", key="_alm_subir",
                         type="primary"):
            return
        if fichero is None:
            st.warning("Elige primero el fichero.")
            return
        with st.spinner("Subiendo y actualizando. No cierres la pagina."):
            hecho = api_post("/import/orders",
                             files={"file": (fichero.name, fichero.getvalue(),
                                             "text/csv")})
        if not hecho:
            return
        st.success(f"Listo: {hecho.get('inserted', 0)} pedidos nuevos, "
                   f"{hecho.get('updated', 0)} actualizados, "
                   f"{hecho.get('unchanged', 0)} ya estaban igual.")
        # Las existencias y el contador que haya en cache son de antes de la
        # subida. Se tiran para que lo siguiente que se mire ya sea lo nuevo.
        st.cache_data.clear()


def _almacen_empaquetar():
    st.markdown('<div class="alm-h1">Empaquetado</div>', unsafe_allow_html=True)
    _caja_de_actualizar()

    # La caja va lo primero, antes del contador y de cualquier explicacion:
    # se teclea de pie y antes habia que bajar la pagina para llegar a ella.
    numero = st.text_input(
        "Numero de orden",
        placeholder="577639922332832484 — o los ultimos digitos",
        key="_alm_orden").strip()
    st.markdown('<div class="alm-sub">Escribe el numero de orden y aparece todo '
                'lo que lleva dentro, con los packs ya desmontados en piezas.'
                '</div>', unsafe_allow_html=True)
    _pintar_contador()

    if not numero:
        st.info("Escribe un numero de orden para empezar.")
        return
    if len(numero) < 6:
        st.warning("Escribe al menos 6 digitos.")
        return

    datos = _pedido(numero)
    if datos is None:
        return

    if not datos.get("encontrada"):
        parecidas = _parecidas_normalizadas(datos.get("parecidas"))
        if not parecidas:
            st.error(f"No hay ninguna orden que contenga «{numero}».")
            return
        st.warning(f"No hay una orden exacta. {len(parecidas)} que se parecen, "
                   "de la mas reciente a la mas antigua:")
        etiquetas = {_etiqueta_de_parecida(p): p["orden"] for p in parecidas}
        st.selectbox("Ordenes parecidas", list(etiquetas), key="_alm_parecidas")
        st.button("Abrir esta orden", type="primary", key="_alm_abrir",
                  on_click=_abrir_la_parecida, args=(etiquetas,))
        return

    _ficha_del_pedido(datos)


def _limpiar_las_marcas(orden):
    """Desmarca todas las casillas de este pedido.

    Callback por la misma razon que _abrir_la_parecida: las casillas son
    widgets, y su estado solo se puede tocar antes de que se pinten.
    """
    for clave in [k for k in st.session_state
                  if k.startswith(f"_alm_ok_{orden}_")]:
        st.session_state.pop(clave, None)


@st.fragment
def _ficha_del_pedido(datos):
    """La ficha del pedido, en un fragmento aparte.

    Es lo que arregla la lentitud. Sin el fragmento, marcar una casilla
    repintaba la pagina ENTERA: las tres llamadas a la API otra vez, el
    apunte en `almacen_consultas` otra vez, y las fotos releidas de disco y
    recodificadas en base64. Marcar cinco piezas de un pedido eran quince
    viajes a Railway.

    Dentro de un fragmento, Streamlit solo repinta este bloque cuando se
    toca un widget suyo. Las casillas y la barra de progreso viven aqui, asi
    que marcar una pieza no cuesta ninguna llamada: ni una.
    """
    orden = datos["orden"]
    lineas = datos.get("lineas") or []
    piezas = datos.get("total_piezas", 0)

    hechas = sum(
        pieza["cantidad"]
        for i, linea in enumerate(lineas)
        for j, pieza in enumerate(linea.get("componentes") or [])
        if st.session_state.get(_clave_pieza(orden, i, j)))

    fecha = (datos.get("fecha") or "")[:10]
    st.markdown(f"""
      <div class="alm-cinta">
        <div class="alm-dato"><div class="alm-dato-l">Orden</div>
          <div class="alm-dato-v alm-sku">{orden}</div></div>
        <div class="alm-dato"><div class="alm-dato-l">Canal</div>
          <div class="alm-dato-v">{(datos.get('plataforma') or '-').upper()}</div></div>
        <div class="alm-dato"><div class="alm-dato-l">Fecha</div>
          <div class="alm-dato-v">{fecha or '-'}</div></div>
        <div class="alm-dato"><div class="alm-dato-l">Estado</div>
          <div class="alm-dato-v">{datos.get('estado') or '-'}</div></div>
        <div class="alm-dato"><div class="alm-dato-l">Piezas a meter</div>
          <div class="alm-dato-v" style="color:#00FF88">{piezas}</div></div>
      </div>
    """, unsafe_allow_html=True)

    dias = _dias_desde(datos.get("fecha"))
    if dias is not None and dias >= DIAS_PARA_AVISAR_DE_VIEJO:
        st.markdown(
            f'<div class="alm-aviso"><b>Este pedido es de hace {dias} dias</b> '
            f'({fecha}). Si has escrito solo los ultimos digitos, comprueba que '
            'es el que querias: hay numeros de orden que terminan igual.</div>',
            unsafe_allow_html=True)

    if piezas:
        st.progress(min(1.0, hechas / piezas), text=f"{hechas} de {piezas} piezas")

    dudosas = 0
    for i, linea in enumerate(lineas):
        with st.container(border=True):
            dudosas += _pintar_linea(orden, i, linea)

    if dudosas:
        st.markdown(
            f'<div class="alm-aviso">{dudosas} linea(s) sin foto segura. El '
            'SKU por si solo no basta para saber cual es: comprueba el detalle '
            'contra el albaran antes de cerrar la caja.</div>',
            unsafe_allow_html=True)

    st.button("Limpiar marcas", key=f"_alm_limpiar_{orden}",
              on_click=_limpiar_las_marcas, args=(orden,))


def _pintar_linea(orden, i, linea):
    """Una linea del pedido. Devuelve 1 si su foto no es segura."""
    foto, info = st.columns([1, 3])
    anuncio = _CATALOGO.resolver(product_id=linea.get("product_id"),
                                 nombre=linea.get("producto"),
                                 seller_sku=linea.get("seller_sku"))
    with foto:
        _pintar_foto(anuncio, 190)

    etiqueta = '<span class="alm-etq">pack</span>' if linea.get("es_combo") else ""
    componentes = linea.get("componentes") or []
    with info:
        st.markdown(
            f'<div><span class="alm-sku">'
            f'{linea.get("seller_sku") or "sin SKU"}</span>{etiqueta}</div>'
            f'<div class="alm-nombre">{linea.get("producto") or ""}</div>'
            f'<div style="margin-top:6px"><span class="alm-cant">'
            f'{linea.get("cantidad", 0)} ud. vendidas</span></div>',
            unsafe_allow_html=True)
        if anuncio["certeza"] == _cat.DUDOSA:
            st.markdown(f'<div class="alm-aviso">No enseno la foto del anuncio: '
                        f'{anuncio["motivo"]}.</div>', unsafe_allow_html=True)
        st.markdown("")
        for j, pieza in enumerate(componentes):
            _pintar_pieza(orden, i, j, pieza)
    return 0 if anuncio["certeza"] == _cat.SEGURA else 1


def _pintar_pieza(orden, i, j, pieza):
    """Una pieza con su cantidad y su casilla. Sin foto, a proposito.

    Las piezas llevaron miniatura hasta el 27-oct-2026 y fue un error. Una
    pieza es un producto del maestro y de muchos productos no existe foto
    suelta: solo se venden en set, asi que la unica imagen disponible era la
    del set. Resultado medido: el 37% de las piezas (3.301 de 8.919 en 90
    dias, 18 productos) ensenaban una cantidad distinta de la que habia que
    meter — una crema que iba sola con la foto de dos. En un almacen la foto
    manda sobre el numero, asi que eso son paquetes mal montados.

    La foto buena es la GRANDE, la del anuncio: esa es exacta porque es la
    foto de lo que se vendio (99,3% identificadas con certeza). La pieza
    necesita el nombre y la cantidad, no una imagen aproximada.
    """
    texto, cuenta, marca = st.columns([6, 1, 1.1])
    with texto:
        st.markdown(f'<div class="alm-pieza">{pieza.get("producto") or ""}</div>'
                    f'<div class="alm-pieza-sku">{pieza.get("sku") or ""}</div>',
                    unsafe_allow_html=True)
    with cuenta:
        st.markdown(f'<div class="alm-x">x{pieza.get("cantidad", 0)}</div>',
                    unsafe_allow_html=True)
    with marca:
        st.checkbox("Hecho", key=_clave_pieza(orden, i, j),
                    label_visibility="collapsed")


def _almacen_existencias():
    st.markdown('<div class="alm-h1">Existencias</div>', unsafe_allow_html=True)
    st.markdown('<div class="alm-sub">Lo que el sistema dice que hay. Si no '
                'cuadra con lo que ves en el estante, avisalo.</div>',
                unsafe_allow_html=True)

    filas = _existencias()
    if not filas:
        st.info("Todavia no hay existencias que mostrar.")
        return

    df = pd.DataFrame(filas)
    nombres = {"producto": "Producto", "en_almacen": "En almacen",
               "en_fbt": "En FBT", "total": "Total",
               "pendiente_de_enviar": "Pendiente de enviar",
               "disponible": "Disponible"}
    df = df.rename(columns={k: v for k, v in nombres.items() if k in df.columns})
    for col in [c for c in df.columns if c != "Producto"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    busca = st.text_input("Buscar producto", key="_alm_busca_stock",
                          placeholder="far away, imari, mesmerize...").strip()
    if busca:
        df = df[df["Producto"].str.contains(busca, case=False, na=False)]

    if "Total" in df.columns:
        negativos = df[df["Total"] < 0]
        if not negativos.empty:
            st.markdown(
                f'<div class="alm-aviso">{len(negativos)} producto(s) en '
                'negativo. Casi siempre es una factura que falta por meter, no '
                'mercancia perdida — pero conviene contarlos en el estante y '
                'avisar.</div>', unsafe_allow_html=True)

    st.caption(f"{len(df)} productos")
    st.dataframe(df.sort_values("Producto"), use_container_width=True,
                 hide_index=True, height=620)


def _almacen_buscar_producto():
    st.markdown('<div class="alm-h1">Buscar producto</div>',
                unsafe_allow_html=True)
    st.markdown('<div class="alm-sub">Para comprobar que es un SKU antes de '
                'cogerlo del estante.</div>', unsafe_allow_html=True)

    texto = st.text_input("SKU o nombre", key="_alm_busca_prod",
                          placeholder="AV-FOBM/3, far away, imari...").strip()
    if len(texto) < 2:
        st.info("Escribe al menos dos letras.")
        return

    patron = _cat.normalizar(texto)
    hallados = [e for e in _CATALOGO.por_id.values()
                if patron in _cat.normalizar(e["sku"])
                or patron in _cat.normalizar(e["nombre"])]
    if not hallados:
        st.warning(f"Nada en el catalogo de fotos con «{texto}».")
        return

    st.caption(f"{len(hallados)} coincidencia(s)")
    for entrada in sorted(hallados, key=lambda e: (e["sku"], e["nombre"]))[:30]:
        with st.container(border=True):
            foto, info = st.columns([1, 4])
            with foto:
                _pintar_foto({"certeza": _cat.SEGURA, "foto": entrada["foto"]}, 120)
            with info:
                st.markdown(
                    f'<div class="alm-sku">{entrada["sku"] or "sin SKU"}</div>'
                    f'<div class="alm-nombre">{entrada["nombre"]}</div>',
                    unsafe_allow_html=True)
