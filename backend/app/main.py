import logging
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from sqlalchemy import text

from app.config import APP_VERSION, CORS_ORIGINS, ENVIRONMENT
from app.database import engine, Base, SessionLocal
from app.api import stores, products, combos, inventory, sales, auth, imports, analytics, reports, admin, agents, finance

import app.models  # noqa: F401

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("rodmat")

limiter = Limiter(key_func=get_remote_address)

app = FastAPI(title="Rodmat Dashboard V2", version=APP_VERSION)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(stores.router)
app.include_router(products.router)
app.include_router(combos.router)
app.include_router(inventory.router)
app.include_router(sales.router)
app.include_router(auth.router)
app.include_router(imports.router)
app.include_router(analytics.router)
app.include_router(reports.router)
app.include_router(admin.router)
app.include_router(agents.router)
app.include_router(finance.router)


def _start_scheduler():
    """Background thread: runs agents at 11:00 UTC (7AM EDT) and reports at 12:00 UTC (8AM EDT)."""
    import threading
    import time
    import schedule as sched

    from app.models.store import Store

    def _run_agents():
        # Lazy imports: pandas-heavy modules only loaded at run time, not startup
        from app.services.agents import prism_agent, haiku_agent, faraway_agent, mesmerize_agent
        logger.info("Scheduler: running agents for all stores")
        db = SessionLocal()
        try:
            store_ids = [s.id for s in db.query(Store).filter(Store.is_active == True).all()]
            for store_id in store_ids:
                for name, agent in [
                    ("prism", prism_agent), ("haiku", haiku_agent),
                    ("faraway", faraway_agent), ("mesmerize", mesmerize_agent),
                ]:
                    try:
                        ran = agent.run(db, store_id)
                        logger.info("Agent %s store %s: %s", name, store_id[:8], "sent" if ran else "skipped")
                    except Exception as exc:
                        logger.exception("Agent %s failed for store %s: %s", name, store_id[:8], exc)
        finally:
            db.close()

    def _run_reports():
        # Lazy import: pandas/numpy only loaded at run time, not startup
        from app.services.daily_report_service import run_all_reports
        logger.info("Scheduler: running daily reports for all stores")
        db = SessionLocal()
        try:
            results = run_all_reports(db)
            logger.info("Daily reports done: %s", results)
        except Exception as exc:
            logger.exception("Daily reports failed: %s", exc)
        finally:
            db.close()

    # 13:00 UTC = 9AM EDT / 15:00 España (CEST) — gives time to upload CSV before report runs
    # 16:00 UTC = 12PM EDT / 18:00 España — daily report after CSV is uploaded
    sched.every().day.at("13:00").do(_run_agents)
    sched.every().day.at("16:00").do(_run_reports)
    logger.info("Scheduler configured: agents at 13:00 UTC, reports at 16:00 UTC")

    def _loop():
        while True:
            sched.run_pending()
            time.sleep(30)

    t = threading.Thread(target=_loop, daemon=True, name="rodmat-scheduler")
    t.start()


def _prewarm_cache():
    """Pre-warm stock DataFrame cache for all active stores. Runs in background thread."""
    import threading, time

    def _do():
        time.sleep(15)  # wait for DB pool to settle after startup
        from app.models.store import Store
        from app.services.analytics_service import _get_stock_df, _df_cache
        db = SessionLocal()
        try:
            stores = db.query(Store).filter(Store.is_active == True).all()
            for store in stores:
                key = (store.id, 30)
                ts, _ = _df_cache.get(key, (datetime.min, None))
                if (datetime.now() - ts).total_seconds() > 290:
                    logger.info("Pre-warming stock cache for store %s", store.id[:8])
                    _get_stock_df(db, store.id, 30)
                    logger.info("Cache warm for store %s", store.id[:8])
        except Exception as exc:
            logger.exception("Cache pre-warm failed: %s", exc)
        finally:
            db.close()

    threading.Thread(target=_do, daemon=True, name="cache-prewarm").start()


@app.on_event("startup")
def on_startup():
    import app.models  # noqa: F401
    Base.metadata.create_all(bind=engine)
    logger.info("Database tables verified/created on startup")
    if ENVIRONMENT != "dev":
        _start_scheduler()
        _prewarm_cache()


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.exception("Unhandled error on %s %s", request.method, request.url.path)
    detail = "Internal server error"
    if ENVIRONMENT == "dev":
        detail = f"{type(exc).__name__}: {exc}"
    return JSONResponse(status_code=500, content={"detail": detail})


@app.get("/api/health", tags=["health"])
def health_check():
    db_ok = False
    try:
        db = SessionLocal()
        db.execute(text("SELECT 1"))
        db.close()
        db_ok = True
    except Exception:
        pass
    # Trigger background cache refresh if near expiry (keeps cache warm between cron pings)
    if ENVIRONMENT != "dev":
        _prewarm_cache()
    return {
        "status": "ok" if db_ok else "degraded",
        "version": APP_VERSION,
        "database": "connected" if db_ok else "error",
    }


# Serve React panel — must be after all API routes
# Priority: backend/static/ (Railway deploy) → full repo frontend/setup-panel/dist (local dev)
_dist = Path(__file__).resolve().parent.parent / "static"
if not _dist.exists():
    _dist = Path(__file__).resolve().parent.parent.parent / "frontend" / "setup-panel" / "dist"

if _dist.exists():
    app.mount("/assets", StaticFiles(directory=str(_dist / "assets")), name="assets")

    @app.get("/{full_path:path}", include_in_schema=False)
    async def serve_react(full_path: str):
        return FileResponse(str(_dist / "index.html"))
else:
    @app.get("/", include_in_schema=False)
    async def root():
        return {"app": "Rodmat Dashboard V2", "version": APP_VERSION, "docs": "/docs", "health": "/api/health"}
