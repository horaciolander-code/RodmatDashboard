"""Subir el export de pedidos desde la propia pagina del almacen.

Vive aparte de `pagina_almacen.py` porque esa pagina ya pasa de 500 lineas, y
porque lo de aqui se prueba solo: `de_quien_es` va de bytes a nombre de
tienda, sin Streamlit por medio.

El almacen no entra al panel. Hasta que esta caja existio, si el dueño no
estaba disponible se empaquetaba con los pedidos de ayer y los de hoy no
aparecian al buscarlos. No hace falta tocar el backend: los dos endpoints de
import piden sesion, no rol (`Depends(get_current_user)`), asi que el almacen
ya podia llamarlos — lo que faltaba era la puerta.
"""
import streamlit as st
from api_client import api_post


# Columnas que trae el export de una tienda y no el de la otra.
#
# Hacen falta porque el importador de TikTok NO comprueba columnas: de cada
# fila solo mira "Order ID" y "SKU ID", y el export de Temu trae las dos.
# Colado por la caja de TikTok entraria sin una queja y los pedidos de Temu
# quedarian guardados como de TikTok, con cantidad 1 y sin estado. Eso ya no
# se arregla desde la pantalla: hay que limpiar la base a mano. Con las dos
# cajas juntas, equivocarse es cuestion de tiempo.
#
# Se miran en minusculas, asi que no vale ninguna que solo cambie de caja:
# TikTok manda "Product Name" y Temu "product name", y "Quantity" no es
# "quantity purchased".
COLUMNAS_PROPIAS = {
    "TikTok": ("seller sku", "order status", "created time"),
    "Temu": ("contribution sku", "order item status", "quantity purchased"),
}

CANALES = (
    {"tienda": "TikTok", "ruta": "/import/orders",
     "clave": "_alm_csv", "boton": "_alm_subir",
     "ayuda": "Descarga el fichero <b>All order</b> de TikTok Shop y sueltalo "
              "aqui."},
    {"tienda": "Temu", "ruta": "/import/temu-orders",
     "clave": "_alm_csv_temu", "boton": "_alm_subir_temu",
     "ayuda": "En Temu Seller Center, <b>Pedidos &rarr; exportar CSV</b>. Es "
              "el de pedidos, no el de Informes."},
)


def de_quien_es(datos):
    """De que tienda es este CSV, mirando solo la cabecera.

    Devuelve None cuando no reconoce ninguna de las dos. A proposito: si
    manana TikTok renombra una columna, el almacen sigue pudiendo subir. Solo
    se planta cuando ve que el fichero es claramente de la otra tienda.
    """
    lineas = (datos or b"")[:8192].decode("utf-8-sig", "ignore").splitlines()
    cabecera = lineas[0].lower() if lineas else ""
    for tienda, columnas in COLUMNAS_PROPIAS.items():
        if any(c in cabecera for c in columnas):
            return tienda
    return None


def _contar_lo_que_hizo(hecho):
    """Lo que el import devuelve, dicho para quien empaqueta.

    El aviso de columnas que falten llega del backend en `warning` y es lo
    unico que se pinta: con el fichero equivocado, "0 pedidos nuevos" se lee
    como "ya estaba todo".
    """
    if hecho.get("warning"):
        st.error(hecho["warning"])
    else:
        texto = (f"Listo: {hecho.get('inserted', 0)} pedidos nuevos, "
                 f"{hecho.get('updated', 0)} actualizados")
        if "unchanged" in hecho:
            texto += f", {hecho['unchanged']} ya estaban igual"
        st.success(texto + ".")

    # Un seller SKU sin combo no se desmonta en piezas: la linea sale entera
    # y el almacen no sabe que meter en la caja. Lo arregla el dueño en
    # Gestion Combos, pero aqui hay que saber por que falta.
    sin_combo = hecho.get("unknown_skus") or []
    if sin_combo:
        st.warning(f"{len(sin_combo)} SKU sin combo asignado: sus pedidos "
                   "apareceran sin piezas hasta que se den de alta  ·  "
                   + ", ".join(str(s) for s in sin_combo[:10]))

    # Estos dos SI descuentan, y del producto equivocado. Aqui no se pueden
    # arreglar, asi que se dice el numero y a quien avisar, sin el detalle
    # que solo sirve en el panel.
    dudosos = len(hecho.get("a_confirmar") or []) + \
        len(hecho.get("sellers_duplicados") or [])
    if dudosos:
        st.error(f"{dudosos} anuncio(s) nuevos reclaman un SKU que ya "
                 "descuenta de otro producto. Avisa antes de fiarte del "
                 "stock: el inventario puede estar restando mal.")


def _un_canal(canal):
    st.markdown(f'<div class="alm-sub"><b>{canal["tienda"]}</b> &nbsp;·&nbsp; '
                f'{canal["ayuda"]} Tarda un par de minutos; cuando termine, '
                'los pedidos nuevos ya se pueden buscar.</div>',
                unsafe_allow_html=True)
    fichero = st.file_uploader(f"CSV de pedidos de {canal['tienda']}",
                              type=["csv"], key=canal["clave"])
    if not st.button(f"Subir y actualizar {canal['tienda']}",
                     key=canal["boton"], type="primary"):
        return
    if fichero is None:
        st.warning("Elige primero el fichero.")
        return

    datos = fichero.getvalue()
    parece = de_quien_es(datos)
    if parece and parece != canal["tienda"]:
        st.error(f"Ese fichero es el export de {parece}, no el de "
                 f"{canal['tienda']}. Subelo en el bloque de {parece} y no "
                 "pasa nada: no se ha guardado nada.")
        return

    with st.spinner("Subiendo y actualizando. No cierres la pagina."):
        hecho = api_post(canal["ruta"],
                         files={"file": (fichero.name, datos, "text/csv")})
    if not hecho:
        return
    _contar_lo_que_hizo(hecho)
    # Las existencias y el contador que haya en cache son de antes de la
    # subida. Se tiran para que lo siguiente que se mire ya sea lo nuevo.
    st.cache_data.clear()


def caja():
    """Va plegada para no estorbar a quien solo viene a empaquetar."""
    with st.expander("Actualizar pedidos  ·  subir el CSV de TikTok o de Temu",
                     expanded=False):
        for i, canal in enumerate(CANALES):
            if i:
                st.markdown("---")
            _un_canal(canal)
