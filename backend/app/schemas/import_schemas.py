from pydantic import BaseModel


class ImportResult(BaseModel):
    total_rows: int
    inserted: int
    updated: int
    errors: int
