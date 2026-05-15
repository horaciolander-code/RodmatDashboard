import logging
import sys
from pathlib import Path

# Ensure backend/ is on sys.path so "app" package is importable
sys.path.insert(0, str(Path(__file__).resolve().parent))

from app.config import DATA_DIR, ENVIRONMENT
from app.database import engine, Base, SessionLocal
from app.models.store import Store

# Import all models so tables are registered
import app.models  # noqa: F401

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("rodmat")


def init_db():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    Base.metadata.create_all(bind=engine)
    logger.info("Database ready at %s", DATA_DIR / "rodmat_v2.db")


def seed_demo_store():
    db = SessionLocal()
    try:
        if db.query(Store).count() == 0:
            demo = Store(
                name="Rodmat",
                owner_email="demo@rodmat.com",
                currency="USD",
                timezone="America/New_York",
                settings={"demo": True},
            )
            db.add(demo)
            db.commit()
            logger.info("Demo store 'Rodmat' created (id=%s)", demo.id)
        else:
            logger.info("Database already has %d store(s), skipping seed", db.query(Store).count())
    finally:
        db.close()


if __name__ == "__main__":
    import uvicorn

    init_db()

    if ENVIRONMENT != "prod":
        seed_demo_store()

    logger.info("Starting server at http://localhost:8000 (env=%s)", ENVIRONMENT)
    logger.info("Swagger UI: http://localhost:8000/docs")
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=(ENVIRONMENT == "dev"),
    )
