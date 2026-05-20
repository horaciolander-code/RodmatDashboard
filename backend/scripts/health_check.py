"""
Rodmat V2 — Health Check
Verifica conectividad a BD, conteos de tablas y anomalías de datos.

Uso:
    railway run python scripts/health_check.py
    railway run python scripts/health_check.py --json

Retorna 0 si todo OK, 1 si hay anomalías críticas.
"""
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import create_engine, inspect, text
from app.config import DATABASE_URL

CHECKS = {
    "incoming_stock": {
        "min_rows": 1,
        "anomaly_sql": "SELECT COUNT(*) FROM incoming_stock WHERE qty_ordered < 0",
        "anomaly_label": "qty_ordered negativa",
    },
    "initial_inventory": {
        "min_rows": 0,
        "anomaly_sql": "SELECT COUNT(*) FROM initial_inventory WHERE quantity < 0",
        "anomaly_label": "quantity negativa",
    },
    "products": {
        "min_rows": 1,
        "anomaly_sql": None,
        "anomaly_label": None,
    },
    "stores": {
        "min_rows": 1,
        "anomaly_sql": None,
        "anomaly_label": None,
    },
    "users": {
        "min_rows": 1,
        "anomaly_sql": None,
        "anomaly_label": None,
    },
    "fbt_inventory": {
        "min_rows": 0,
        "anomaly_sql": "SELECT COUNT(*) FROM fbt_inventory WHERE total_units < 0",
        "anomaly_label": "total_units negativo",
    },
}


def run_health_check() -> dict:
    result = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "status": "ok",
        "tables": {},
        "warnings": [],
        "errors": [],
    }

    try:
        engine = create_engine(DATABASE_URL)
        inspector = inspect(engine)
        existing = set(inspector.get_table_names())
    except Exception as e:
        result["status"] = "critical"
        result["errors"].append(f"No se puede conectar a la BD: {e}")
        return result

    with engine.connect() as conn:
        for table, cfg in CHECKS.items():
            entry = {}
            if table not in existing:
                entry["status"] = "missing"
                result["errors"].append(f"Tabla faltante: {table}")
                result["status"] = "critical"
            else:
                count = conn.execute(text(f'SELECT COUNT(*) FROM "{table}"')).scalar()
                entry["rows"] = count
                entry["status"] = "ok"

                if count < cfg["min_rows"]:
                    entry["status"] = "warning"
                    result["warnings"].append(f"{table}: solo {count} filas (mínimo {cfg['min_rows']})")

                if cfg["anomaly_sql"]:
                    anom = conn.execute(text(cfg["anomaly_sql"])).scalar()
                    entry["anomalies"] = anom
                    if anom > 0:
                        entry["status"] = "warning"
                        result["warnings"].append(f"{table}: {anom} registros con {cfg['anomaly_label']}")

            result["tables"][table] = entry

    if result["errors"]:
        result["status"] = "critical"
    elif result["warnings"]:
        result["status"] = "warning"

    return result


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", dest="as_json", action="store_true")
    args = parser.parse_args()

    report = run_health_check()

    if args.as_json:
        print(json.dumps(report, indent=2, ensure_ascii=False))
    else:
        print(f"=== HEALTH CHECK — {report['timestamp']} ===")
        print(f"Estado global: {report['status'].upper()}\n")
        for table, info in report["tables"].items():
            rows = info.get("rows", "N/A")
            status = info.get("status", "?")
            anom = info.get("anomalies", 0)
            anom_str = f" | anomalías: {anom}" if anom else ""
            print(f"  {table:<22} {rows:>6} filas  [{status}]{anom_str}")
        if report["warnings"]:
            print("\nAVISOS:")
            for w in report["warnings"]:
                print(f"  [!] {w}")
        if report["errors"]:
            print("\nERRORES:")
            for e in report["errors"]:
                print(f"  ✗ {e}")

    sys.exit(0 if report["status"] == "ok" else 1)
