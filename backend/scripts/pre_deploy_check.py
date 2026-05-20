"""
Rodmat V2 — Pre-Deploy Check
Verifica integridad de la BD y saca un backup antes de cualquier deploy.

Uso:
    railway run python scripts/pre_deploy_check.py
    railway run python scripts/pre_deploy_check.py --skip-backup

Salida 0 = OK, salida 1 = problemas detectados (deploy debe abortarse).
"""
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import create_engine, inspect, text
from app.config import DATABASE_URL

CRITICAL_TABLES = [
    "stores", "users", "products", "incoming_stock",
    "initial_inventory", "sales_orders", "fbt_inventory",
]

# Mínimos esperados de filas (alertar si caen por debajo)
MIN_ROWS = {
    "stores": 1,
    "users": 1,
    "products": 1,
    "incoming_stock": 1,
}


def check_db() -> list[str]:
    errors = []
    engine = create_engine(DATABASE_URL)
    inspector = inspect(engine)
    existing = set(inspector.get_table_names())

    for table in CRITICAL_TABLES:
        if table not in existing:
            errors.append(f"TABLA FALTANTE: {table}")
            continue
        with engine.connect() as conn:
            count = conn.execute(text(f'SELECT COUNT(*) FROM "{table}"')).scalar()
        min_r = MIN_ROWS.get(table, 0)
        if count < min_r:
            errors.append(f"FILAS INSUFICIENTES en {table}: {count} (mínimo {min_r})")
        else:
            print(f"  [OK] {table}: {count} filas")

    return errors


def run_backup() -> bool:
    print("\nTomando backup previo al deploy...")
    result = subprocess.run(
        [sys.executable, "scripts/backup_db.py"],
        capture_output=False,
    )
    return result.returncode == 0


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Pre-deploy check para Rodmat V2")
    parser.add_argument("--skip-backup", action="store_true", help="Omitir backup (no recomendado)")
    args = parser.parse_args()

    print("=== PRE-DEPLOY CHECK — Rodmat V2 ===\n")
    print("Verificando integridad de la base de datos...")
    errors = check_db()

    if errors:
        print("\n[FALLOS DETECTADOS]")
        for e in errors:
            print(f"  ✗ {e}")
        print("\nDeploy ABORTADO. Corrige los problemas antes de desplegar.")
        sys.exit(1)

    print("\nIntegridad OK.")

    if not args.skip_backup:
        ok = run_backup()
        if not ok:
            print("ERROR: El backup falló. Deploy ABORTADO.")
            sys.exit(1)
    else:
        print("Backup omitido por --skip-backup.")

    print("\n=== PRE-DEPLOY CHECK APROBADO ===")
    sys.exit(0)
