"""Cuando un seller SKU no significa lo mismo en las dos plataformas.

Hay dos cosas que se parecen y hay que tratar al contrario:

  SKU NUEVO            `AV-MS/2` no esta en combos todavia. No descuenta
                       nada, asi que no puede equivocarse: entra, y se
                       asigna luego en Gestion > Gestion Combos. Bloquear
                       la carga por esto es lo que rompia el proceso.

  SKU YA EN USO        Llega un anuncio de TEMU que no habiamos visto nunca
                       y reclama un seller SKU que YA descuenta por sus
                       componentes. Aqui no se puede dejar pasar en
                       silencio: si el anuncio no es ese producto, cada
                       venta descuenta del stock equivocado y nadie lo ve.
                       En TEMU ya ha pasado reusar un seller SKU para un
                       articulo distinto al de TikTok.

El seller SKU no es la relacion: la relacion es el ID de producto de la
plataforma. Por eso el aviso se mide por ID de anuncio, no por el texto del
SKU, y salta UNA vez por anuncio nuevo — cuando aun se puede corregir.

Modulo puro: sin pandas, sin sqlalchemy, sin modelos, para que corra en el
job de aislamiento del CI.
"""
from __future__ import annotations


def _norm(v) -> str:
    return str(v or "").replace("\t", "").strip().lower()


def anuncios_a_confirmar(lineas, ids_ya_conocidos, sellers_que_descuentan) -> list:
    """Anuncios nuevos que reclaman un seller SKU que ya descuenta.

    `lineas` es un iterable de (id_de_anuncio, seller_sku, nombre).
    `ids_ya_conocidos` son los IDs de anuncio ya vistos en pedidos.
    `sellers_que_descuentan` son los seller SKU con componentes asignados.

    Devuelve [(seller_sku, id_de_anuncio, nombre), ...] sin repetir, en
    orden estable. Un anuncio ya conocido no vuelve a avisar: la primera vez
    es cuando se confirma o se corrige.
    """
    vistos = {_norm(x) for x in ids_ya_conocidos if _norm(x)}
    descuentan = {_norm(x) for x in sellers_que_descuentan if _norm(x)}

    fuera, ya_avisado = [], set()
    for id_anuncio, seller, nombre in lineas:
        ida, sell = _norm(id_anuncio), _norm(seller)
        if not ida or not sell:
            continue
        if sell not in descuentan:
            continue          # SKU nuevo: no descuenta, no puede equivocarse
        if ida in vistos:
            continue          # anuncio ya conocido: ya se reviso en su dia
        if (sell, ida) in ya_avisado:
            continue
        ya_avisado.add((sell, ida))
        fuera.append((seller, id_anuncio, nombre))
    return sorted(fuera, key=lambda t: (_norm(t[0]), _norm(t[1])))


def sellers_con_varios_anuncios(lineas, ids_ya_conocidos=()) -> list:
    """Seller SKU que llegan con dos IDs de anuncio distintos.

    El mismo seller SKU etiquetando dos anuncios de la misma plataforma: uno
    de los dos descuenta del producto equivocado.

    Solo se avisa si al menos uno de los anuncios es NUEVO. Sin esta
    condicion el aviso saltaria en todas las cargas para siempre en cuanto
    un producto tenga dos anuncios legitimos en TEMU — y un aviso que sale
    cada dia deja de leerse, que es como se cuela el que importa.
    """
    vistos = {_norm(x) for x in ids_ya_conocidos if _norm(x)}
    por_seller = {}
    for id_anuncio, seller, _nombre in lineas:
        ida, sell = _norm(id_anuncio), _norm(seller)
        if ida and sell:
            por_seller.setdefault(sell, set()).add(ida)
    return sorted((s, sorted(ids)) for s, ids in por_seller.items()
                  if len(ids) > 1 and not ids <= vistos)


def texto_a_confirmar(items, limite: int = 8) -> str:
    """Una linea por anuncio, con su ID, que es lo unico que lo identifica."""
    partes = [f"{s} -> anuncio {i}" + (f" ({n[:40]})" if n else "")
              for s, i, n in items[:limite]]
    if len(items) > limite:
        partes.append(f"y {len(items) - limite} mas")
    return " · ".join(partes)
