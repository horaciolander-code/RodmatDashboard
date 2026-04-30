import os
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")
except ImportError:
    pass

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR.parent / "data"

# Database: env var for PostgreSQL; SQLite always uses absolute DATA_DIR path
_db_env = os.getenv("DATABASE_URL", "")
if _db_env.startswith("postgresql"):
    DATABASE_URL = _db_env
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

APP_VERSION = "2.0.0"
