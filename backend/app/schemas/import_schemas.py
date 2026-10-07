from typing import List
from pydantic import BaseModel


class ImportResult(BaseModel):
    total_rows: int
    inserted: int
    updated: int
    errors: int
    unknown_skus: List[str] = []
    warning: str | None = None
    # Resumen legible de lo que hizo el import. Sin este campo, el response_model
    # descartaría en silencio lo que devuelva el parser.
    detalle: str | None = None
