"""De que marca es una cosa, deducido en servidor.

Modulo puro a proposito: sin fastapi, sin sqlalchemy, sin modelos. Asi se
puede probar en el job de aislamiento del CI, que solo tiene pytest y pandas.
"""
from __future__ import annotations


def marca_de_los_componentes(products) -> str | None:
    """La marca de un combo sale de sus productos, no de lo que mande el cliente.

    Hasta el 15-oct los combos nacian con `brand_id` NULL: ni el endpoint ni
    el schema la admitian. Dos consecuencias, las dos silenciosas:

      - el importador de TEMU saca la marca del combo para etiquetar la venta,
        asi que esas ventas entraban sin marca
      - `GET /api/combos` filtra por marca, asi que un combo sin marca es
        invisible para un usuario con marca asignada

    Mismo criterio que `POST /api/inventory/incoming`: se deriva en servidor.

    Si los componentes son de marcas DISTINTAS se devuelve None a proposito.
    Un combo mixto no pertenece a ninguna, y marcarlo con una de ellas
    falsearia el filtro de la otra — es la regla 2, Nokal y Rodmat no se
    mezclan, aplicada a las marcas de dentro de una tienda.
    """
    marcas = {getattr(p, "brand_id", None) for p in (products or [])}
    marcas.discard(None)
    marcas.discard("")
    return next(iter(marcas)) if len(marcas) == 1 else None
