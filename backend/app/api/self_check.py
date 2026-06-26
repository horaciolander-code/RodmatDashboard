"""Self-check endpoint — validar salud del sistema tras cada deploy.
NO modifica datos. NO envía emails. Solo lectura. Protegido con INTERNAL_API_KEY."""
import time
from fastapi import APIRouter, Depends, HTTPException, Header
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.config import INTERNAL_API_KEY
from app.database import get_db
from app.models.store import Store

router = APIRouter(prefix="/api/health", tags=["health"])

RODMAT_STORE = "13c02ce8-7761-43b7-9525-fb5aad6f0a09"


def _require_internal_key(x_api_key: str = Header(...)):
    if not INTERNAL_API_KEY or x_api_key != INTERNAL_API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API key")


@router.post("/check")
def run_self_check(db: Session = Depends(get_db),
                   _: None = Depends(_require_internal_key)):
    """POST /api/health/check con header X-API-Key.
    Ejecuta 8 checks read-only. Devuelve {status, passed, failed, checks}."""
    checks = []
    t0 = time.time()

    def chk(name, fn):
        start = time.time()
        try:
            fn()
            checks.append({"name": name, "status": "pass",
                           "duration_s": round(time.time() - start, 2)})
        except Exception as e:
            checks.append({"name": name, "status": "fail",
                           "duration_s": round(time.time() - start, 2),
                           "error": str(e)[:200]})

    chk("db_ping",        lambda: db.execute(text("SELECT 1")).scalar())
    chk("stores_query",   lambda: db.query(Store).count())
    chk("products_query", lambda: db.execute(text(
        "SELECT COUNT(*) FROM products WHERE store_id=:s"),
        {"s": RODMAT_STORE}).scalar())
    chk("sales_query",    lambda: db.execute(text(
        "SELECT COUNT(*) FROM sales_orders WHERE store_id=:s"),
        {"s": RODMAT_STORE}).scalar())
    chk("import_history", lambda: db.execute(text(
        "SELECT MAX(imported_at) FROM import_history WHERE store_id=:s"),
        {"s": RODMAT_STORE}).scalar())
    chk("agent_runs",     lambda: db.execute(text(
        "SELECT COUNT(*) FROM agent_runs WHERE run_at::date=CURRENT_DATE")).scalar())

    def _stock_check():
        from app.services.stock_calculator import calculate_stock
        result = calculate_stock(db, RODMAT_STORE)
        if result is None or len(result) == 0:
            raise RuntimeError("calculate_stock returned empty")
    chk("stock_calculator", _stock_check)

    def _report_check():
        from app.services.daily_report_service import build_report
        html, subject = build_report(db, RODMAT_STORE)
        if not html or len(html) < 100:
            raise RuntimeError("build_report returned empty/short HTML")
    chk("daily_report_build", _report_check)

    passed = sum(1 for c in checks if c["status"] == "pass")
    failed = sum(1 for c in checks if c["status"] == "fail")
    return {
        "status": "healthy" if failed == 0 else "unhealthy",
        "passed": passed, "failed": failed, "total": len(checks),
        "total_duration_s": round(time.time() - t0, 2),
        "checks": checks,
    }
