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
    warning: str | None = None
    # Resumen legible de lo que hizo el import. Sin este campo, el response_model
    # descartaría en silencio lo que devuelva el parser.
    detalle: str | None = None
