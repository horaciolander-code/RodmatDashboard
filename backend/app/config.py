import os
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")
except ImportError:
    pass

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR.parent / "data"

def con_driver_explicito(url: str) -> str:
    """Escribe el driver en la URL de Postgres en vez de dejarlo al azar.

    SQLAlchemy 2.1 cambió el driver por defecto de `postgresql://` de psycopg2
    a psycopg (v3), que no está instalado. El 11-oct tumbó el despliegue: el
    primer rebuild desde el 21-sep se trajo SQLAlchemy 2.1.4 y el servicio no
    arrancaba —`ModuleNotFoundError: No module named 'psycopg'`—, con el build
    en verde y el healthcheck fallando durante cinco minutos.

    Con el driver escrito, la versión de SQLAlchemy ya no decide por nosotros.
    Si algún día se migra a psycopg v3, se cambia aquí y se añade a
    requirements.txt — a propósito, no por sorpresa.
    """
    for prefijo in ("postgresql://", "postgres://"):   # el 2º lo usan algunos proveedores
        if url.startswith(prefijo):
            return "postgresql+psycopg2://" + url[len(prefijo):]
    return url


# Database: env var for PostgreSQL; SQLite always uses absolute DATA_DIR path
_db_env = os.getenv("DATABASE_URL", "")
if _db_env.startswith("postgres"):
    DATABASE_URL = con_driver_explicito(_db_env)
else:
    DATABASE_URL = f"sqlite:///{DATA_DIR / 'rodmat_v2.db'}"

# JWT
JWT_SECRET = os.getenv("JWT_SECRET", "rodmat-v2-dev-secret-change-in-prod")
JWT_ALGORITHM = "HS256"
TOKEN_EXPIRE_MINUTES = int(os.getenv("TOKEN_EXPIRE_MINUTES", "480"))

# Superadmin & internal keys
SUPERADMIN_EMAIL = os.getenv("SUPERADMIN_EMAIL", "")
INTERNAL_API_KEY = os.getenv("INTERNAL_API_KEY", "")

# CORS
_cors_raw = os.getenv("CORS_ORIGINS", "http://localhost:3000,http://localhost:8501")
CORS_ORIGINS = [origin.strip() for origin in _cors_raw.split(",") if origin.strip()]

# Environment
ENVIRONMENT = os.getenv("ENVIRONMENT", "dev")

# Upload limits
MAX_UPLOAD_SIZE = 50 * 1024 * 1024  # 50MB

APP_VERSION = "2.2.0"
