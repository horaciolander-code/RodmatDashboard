"""
Rodmat V2 — Database Backup Script
Exporta todas las tablas a JSON + CSV, genera manifest, comprime en ZIP.
READ-ONLY — no modifica nada en producción.

Uso:
    railway run python scripts/backup_db.py
    railway run python scripts/backup_db.py --output /tmp/backups
    railway run python scripts/backup_db.py --tables incoming_stock,products
"""
import argparse
import json
import os
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path

# Allow importing from parent (app/)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
from sqlalchemy import create_engine, inspect, text

from app.config import DATABASE_URL

TABLES = [
    "stores",
    "users",
    "products",
    "combos",
    "combo_items",
    "initial_inventory",
    "incoming_stock",
    "sales_orders",
    "affiliate_sales",
    "import_history",
    "amazon_sku_map",
    "fbt_inventory",
    "report_logs",
    "bank_transactions",
]


def backup(output_dir: Path, tables: list[str]) -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
    backup_dir = output_dir / stamp
    backup_dir.mkdir(parents=True, exist_ok=True)

    engine = create_engine(DATABASE_URL)
    inspector = inspect(engine)
    existing = set(inspector.get_table_names())

    manifest = {"timestamp": stamp, "database_url_host": _safe_host(DATABASE_URL), "tables": {}}

    with engine.connect() as conn:
        for table in tables:
            if table not in existing:
                print(f"  [SKIP] {table} — no existe en la BD")
                manifest["tables"][table] = {"rows": 0, "status": "missing"}
                continue

            df = pd.read_sql(text(f'SELECT * FROM "{table}"'), conn)
            rows = len(df)

            df.to_json(backup_dir / f"{table}.json", orient="records", date_format="iso", indent=2)
            df.to_csv(backup_dir / f"{table}.csv", index=False)

            manifest["tables"][table] = {"rows": rows, "status": "ok"}
            print(f"  [OK] {table}: {rows} filas")

    manifest_path = backup_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False))

    zip_path = output_dir / f"rodmat_backup_{stamp}.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in backup_dir.iterdir():
            zf.write(f, f.name)

    # Clean up uncompressed folder
    for f in backup_dir.iterdir():
        f.unlink()
    backup_dir.rmdir()

    size_kb = zip_path.stat().st_size / 1024
    print(f"\nBackup completado: {zip_path} ({size_kb:.1f} KB)")
    print(f"Total tablas: {len([t for t in manifest['tables'].values() if t['status'] == 'ok'])}")
    return zip_path


def _safe_host(url: str) -> str:
    """Returns only host:port from a DATABASE_URL (no credentials)."""
    try:
        from urllib.parse import urlparse
        p = urlparse(url)
        return f"{p.hostname}:{p.port}"
    except Exception:
        return "unknown"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Rodmat V2 — DB Backup")
    parser.add_argument("--output", default="backups", help="Directorio de salida")
    parser.add_argument("--tables", default="", help="Tablas separadas por coma (vacío = todas)")
    args = parser.parse_args()

    output_dir = Path(args.output)
    tables = [t.strip() for t in args.tables.split(",") if t.strip()] if args.tables else TABLES

    print(f"Iniciando backup de Rodmat V2...")
    print(f"  Host BD: {_safe_host(DATABASE_URL)}")
    print(f"  Tablas:  {', '.join(tables)}")
    print(f"  Salida:  {output_dir.resolve()}\n")

    backup(output_dir, tables)
