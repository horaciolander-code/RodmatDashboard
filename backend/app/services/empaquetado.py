"""Que piezas hay que meter en la caja para una linea de pedido.

Esto tiene que decir EXACTAMENTE lo mismo que el calculo de stock. Si el
almacen empaqueta tres frascos y el inventario descuenta dos, el descuadre
aparece semanas despues y nadie sabe de donde sale. Por eso la regla vive
aqui, en un sitio, y `backend/tests/test_empaquetado.py` compara cada caso
contra `stock_calculator.decompose_orders`, que es quien mueve el stock.

Las dos reglas que no son obvias:

  El SKU bueno es `seller_sku` y, si viene vacio, el `sku` numerico de la
  plataforma. Es lo que stock_calculator llama SKU_ID_Clean.

  En Amazon y Walmart, si el SKU esta en el sku_map de su canal, el
  importador YA multiplico la cantidad al insertar la venta. Aplicar
  ademas el combo descontaria el doble — paso de verdad con AV-BSS/2, que
  restaba 4 frascos por pack en vez de 2.

Modulo puro: sin pandas, sin sqlalchemy, sin red.
"""

CANALES_QUE_EXPANDEN_AL_IMPORTAR = ("amazon", "walmart")


def sku_limpio(seller_sku, sku) -> str:
    """El identificador con el que se busca el combo."""
    for valor in (seller_sku, sku):
        texto = str(valor).strip() if valor is not None else ""
        if texto and texto.lower() != "nan":
            return texto
    return ""


def ya_venia_expandida(plataforma, clave, skus_de_mapa) -> bool:
    """La cantidad ya la multiplico el importador y no hay que tocarla."""
    canal = str(plataforma or "tiktok").strip().lower()
    if canal not in CANALES_QUE_EXPANDEN_AL_IMPORTAR:
        return False
    return clave in (skus_de_mapa or set())


def componentes_de(clave, nombre_producto, cantidad, combos, *,
                   plataforma=None, skus_de_mapa=None) -> list:
    """Las piezas sueltas de una linea. Devuelve siempre al menos una.

    `combos` es {combo_sku: [{"product", "units"}]}, tal cual lo construye
    stock_calculator._build_combo_dict. La busqueda es por texto exacto,
    igual que alli: cambiar eso aqui descuadraria los dos lados.
    """
    try:
        unidades = int(cantidad)
    except (TypeError, ValueError):
        unidades = 0

    expandida = ya_venia_expandida(plataforma, clave, skus_de_mapa)
    piezas = (combos or {}).get(clave) if not expandida else None

    if not piezas:
        nombre = str(nombre_producto or "").strip()
        if not nombre or nombre.lower() == "nan":
            nombre = clave
        return [{"producto": nombre, "cantidad": unidades, "de_combo": False}]

    return [{"producto": p["product"],
             "cantidad": unidades * int(p["units"]),
             "de_combo": True}
            for p in piezas]


def total_piezas(lineas) -> int:
    """Cuantas unidades sueltas salen del almacen para un pedido entero."""
    return sum(c["cantidad"] for linea in lineas for c in linea)
