import logging

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from sqlalchemy import text

from app.config import APP_VERSION, CORS_ORIGINS, ENVIRONMENT
from app.database import engine, Base, SessionLocal
from app.api import stores, products, combos, inventory, sales, auth, imports, analytics, reports, admin

# Import all models so Base.metadata knows about them
import app.models  # noqa: F401

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("rodmat")

# Rate limiter (shared instance, imported by route modules)
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


@app.on_event("startup")
def on_startup():
    import app.models  # noqa: F401 — ensure all models are registered
    Base.metadata.create_all(bind=engine)
    logger.info("Database tables verified/created on startup")


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
    return {
        "status": "ok" if db_ok else "degraded",
        "version": APP_VERSION,
        "database": "connected" if db_ok else "error",
    }
