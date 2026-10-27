"""La pagina del almacen: empaquetar un pedido mirando las fotos.

Vive aparte de app.py por dos razones. app.py ya pasa de 3.300 lineas, y
sobre todo: aqui se puede importar y probar sin arrancar Streamlit ni la API,
que es lo que hace `backend/tests/test_pagina_almacen.py`.
"""
import base64 as _b64
import datetime as _dt

import pandas as pd
import streamlit as st
from api_client import api_get

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
</style>
"""


def _foto_incrustada(nombre_fichero, alto):
    """La foto como etiqueta img. Se incrusta para maquetar la ficha entera.

    Son cuatro o cinco fotos por pedido, de 30 KB cada una: incrustarlas sale
    mas barato que una peticion por imagen y deja fijar el alto exacto, que es
    lo que hace que la lista se lea de un vistazo.
    """
    ruta = _cat.CARPETA / nombre_fichero
    if not ruta.exists():
        return None
    datos = _b64.b64encode(ruta.read_bytes()).decode()
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


def _almacen_empaquetar():
    st.markdown('<div class="alm-h1">Empaquetado</div>', unsafe_allow_html=True)
    st.markdown('<div class="alm-sub">Escribe el numero de orden y aparece todo '
                'lo que lleva dentro, con los packs ya desmontados en piezas.'
                '</div>', unsafe_allow_html=True)

    izq, _der = st.columns([2, 1])
    with izq:
        numero = st.text_input(
            "Numero de orden",
            placeholder="577639922332832484 — o los ultimos digitos",
            key="_alm_orden").strip()

    if not numero:
        st.info("Escribe un numero de orden para empezar.")
        return
    if len(numero) < 6:
        st.warning("Escribe al menos 6 digitos.")
        return

    datos = api_get(f"/almacen/orden/{numero}")
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
        elegida = st.selectbox("Ordenes parecidas", list(etiquetas),
                               key="_alm_parecidas")
        if st.button("Abrir esta orden", type="primary", key="_alm_abrir"):
            st.session_state["_alm_orden"] = etiquetas[elegida]
            st.rerun()
        return

    _ficha_del_pedido(datos)


def _ficha_del_pedido(datos):
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
            f'<div class="alm-aviso">{dudosas} producto(s) sin foto segura. El '
            'SKU por si solo no basta para saber cual es: compruebalo contra el '
            'albaran antes de cerrar la caja.</div>', unsafe_allow_html=True)

    if st.button("Limpiar marcas", key=f"_alm_limpiar_{orden}"):
        for clave in [k for k in st.session_state
                      if k.startswith(f"_alm_ok_{orden}_")]:
            st.session_state.pop(clave, None)
        st.rerun()


def _pintar_linea(orden, i, linea):
    """Una linea del pedido. Devuelve cuantas piezas quedan sin foto segura."""
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
        sin_foto = 0
        for j, pieza in enumerate(componentes):
            if not _pintar_pieza(orden, i, j, pieza):
                sin_foto += 1
    return sin_foto


def _pintar_pieza(orden, i, j, pieza):
    """Una pieza suelta con su casilla. Devuelve si tiene foto segura."""
    mini, texto, cuenta, marca = st.columns([0.9, 5, 1, 1.1])
    resuelta = _CATALOGO.resolver_pieza(nombre=pieza.get("producto"),
                                        anuncios=pieza.get("anuncios"))
    with mini:
        _pintar_foto(resuelta, 62)
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
    return resuelta["certeza"] == _cat.SEGURA


def _almacen_existencias():
    st.markdown('<div class="alm-h1">Existencias</div>', unsafe_allow_html=True)
    st.markdown('<div class="alm-sub">Lo que el sistema dice que hay. Si no '
                'cuadra con lo que ves en el estante, avisalo.</div>',
                unsafe_allow_html=True)

    filas = api_get("/almacen/resumen")
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
