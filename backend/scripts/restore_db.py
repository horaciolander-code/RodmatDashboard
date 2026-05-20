"""
Rodmat V2 — Database Restore Script
Restaura tablas desde un ZIP de backup generado por backup_db.py.

Uso:
    railway run python scripts/restore_db.py --backup backups/rodmat_backup_2026-05-17_12-00-00.zip
    railway run python scripts/restore_db.py --backup <zip> --tables incoming_stock
    railway run python scripts/restore_db.py --backup <zip> --dry-run

IMPORTANTE: Por defecto hace UPSERT (insert or update) — NUNCA borra filas existentes.
Para reemplazar una tabla completamente, usar --replace (pide confirmación).
"""
import argparse
import json
import sys
import zipfile
from io import BytesIO
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
from sqlalchemy import create_engine, inspect, text

from app.config import DATABASE_URL

# Columna PK por tabla (para el upsert)
PK_MAP = {
    "stores": "id",
    "users": "id",
    "products": "id",
    "combos": "id",
    "combo_items": "id",
    "initial_inventory": "id",
    "incoming_stock": "id",
    "sales_orders": "id",
    "affiliate_sales": "id",
    "import_history": "id",
    "amazon_sku_map": "id",
    "fbt_inventory": "id",
}


def restore(zip_path: Path, tables_filter: list[str], dry_run: bool, replace: bool):
    engine = create_engine(DATABASE_URL)
    inspector = inspect(engine)
    existing_tables = set(inspector.get_table_names())

    with zipfile.ZipFile(zip_path, "r") as zf:
        names = zf.namelist()
        manifest_data = json.loads(zf.read("manifest.json"))
        print(f"Backup del: {manifest_data['timestamp']}")
        print(f"Host origen: {manifest_data.get('database_url_host', 'desconocido')}\n")

        json_files = {Path(n).stem: n for n in names if n.endswith(".json") and n != "manifest.json"}

        for table, filename in json_files.items():
            if tables_filter and table not in tables_filter:
                continue
            if table not in existing_tables:
                print(f"  [SKIP] {table} — tabla no existe en la BD destino")
                continue

            df = pd.read_json(BytesIO(zf.read(filename)), orient="records")
            if df.empty:
                print(f"  [SKIP] {table} — sin datos en el backup")
                continue

            source_rows = len(df)

            if dry_run:
                print(f"  [DRY-RUN] {table}: {source_rows} filas — no se escribe nada")
                continue

            if replace:
                # Full replace — mostrar cuántas filas se borrarán y pedir confirmación
                with engine.connect() as conn:
                    current_count = conn.execute(text(f'SELECT COUNT(*) FROM "{table}"')).scalar()
                print(f"\n  ADVERTENCIA: --replace va a borrar {current_count} filas de '{table}' y reemplazarlas con {source_rows}.")
                confirm = input(f"  Escribe 'SI' para confirmar el reemplazo de '{table}': ")
                if confirm.strip().upper() != "SI":
                    print(f"  [CANCELADO] {table}")
                    continue
                with engine.begin() as conn:
                    conn.execute(text(f'DELETE FROM "{table}"'))
                    df.to_sql(table, conn, if_exists="append", index=False)
                print(f"  [REPLACE] {table}: {source_rows} filas insertadas")
            else:
                # UPSERT (append+update por PK)
                pk = PK_MAP.get(table, "id")
                inserted = 0
                updated = 0
                with engine.begin() as conn:
                    for _, row in df.iterrows():
                        row_dict = {k: v for k, v in row.items() if pd.notna(v)}
                        exists = conn.execute(
                            text(f'SELECT 1 FROM "{table}" WHERE {pk} = :{pk}'),
                            {pk: row_dict[pk]}
                        ).fetchone()
                        if exists:
                            set_clause = ", ".join(f'"{k}" = :{k}' for k in row_dict if k != pk)
                            if set_clause:
                                conn.execute(text(f'UPDATE "{table}" SET {set_clause} WHERE {pk} = :{pk}'), row_dict)
                            updated += 1
                        else:
                            cols = ", ".join(f'"{k}"' for k in row_dict)
                            vals = ", ".join(f':{k}' for k in row_dict)
                            conn.execute(text(f'INSERT INTO "{table}" ({cols}) VALUES ({vals})'), row_dict)
                            inserted += 1
                print(f"  [UPSERT] {table}: {inserted} insertadas, {updated} actualizadas")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Rodmat V2 — DB Restore")
    parser.add_argument("--backup", required=True, help="Ruta al ZIP de backup")
    parser.add_argument("--tables", default="", help="Tablas a restaurar (vacío = todas)")
    parser.add_argument("--dry-run", action="store_true", help="Simular sin escribir")
    parser.add_argument("--replace", action="store_true", help="Reemplazar tabla completa (pide confirmación)")
    args = parser.parse_args()

    zip_path = Path(args.backup)
    if not zip_path.exists():
        print(f"ERROR: No se encuentra el archivo {zip_path}")
        sys.exit(1)

    tables_filter = [t.strip() for t in args.tables.split(",") if t.strip()] if args.tables else []

    print(f"Restaurando Rodmat V2 desde: {zip_path}")
    if args.dry_run:
        print("MODO DRY-RUN — no se escribe nada\n")

    restore(zip_path, tables_filter, dry_run=args.dry_run, replace=args.replace)
    print("\nRestaura completada.")
