from typing import List
from pydantic import BaseModel


class ImportResult(BaseModel):
    total_rows: int
    inserted: int
    updated: int
    # Filas que llegaron IGUAL a como ya estaban: el UPSERT no las reescribe.
    # En la carga diaria de TikTok son ~17.900 de 18.000, y reescribirlas era
    # lo que agotaba el presupuesto de Disk IO del proyecto.
    unchanged: int = 0
    errors: int
    unknown_skus: List[str] = []
    # True SOLO cuando el import se abandono sin escribir nada (inventario de
    # Amazon/Walmart: sus filas necesitan un product_id y no hay donde poner
    # una fila de un producto que no existe).
    #
    # En TEMU NO se bloquea: los pedidos entran y el SKU sin combo se revisa
    # en Gestion > Gestion Combos. El panel pintaba cualquier `unknown_skus`
    # como "Bloqueado" y escondia el resultado real, asi que una carga de TEMU
    # que SI habia entrado parecia haber fallado.
    bloqueado: bool = False
    # Anuncios de TEMU nuevos que reclaman un seller SKU que YA descuenta por
    # sus componentes. No bloquean, pero hay que confirmarlos: si el anuncio
    # no es ese producto, cada venta descuenta del stock equivocado.
    a_confirmar: List[dict] = []
    # El mismo seller SKU etiquetando dos anuncios de la misma plataforma.
    # Esto no es ambiguo, es un error: uno de los dos descuenta mal.
    sellers_duplicados: List[dict] = []
    warning: str | None = None
    # Resumen legible de lo que hizo el import. Sin este campo, el response_model
    # descartaría en silencio lo que devuelva el parser.
    detalle: str | None = None
