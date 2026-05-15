import uuid
from datetime import datetime, timedelta, timezone

import bcrypt
from jose import jwt, JWTError

from app.config import JWT_SECRET, JWT_ALGORITHM, TOKEN_EXPIRE_MINUTES


def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()


def verify_password(plain: str, hashed: str) -> bool:
    return bcrypt.checkpw(plain.encode(), hashed.encode())


def create_access_token(user_id: str, store_id: str, role: str) -> str:
    now = datetime.now(timezone.utc)
    expire = now + timedelta(minutes=TOKEN_EXPIRE_MINUTES)
    payload = {
        "sub": user_id,
        "store_id": store_id,
        "role": role,
        "exp": expire,
        "iat": now,
        "jti": str(uuid.uuid4()),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def decode_token(token: str) -> dict:
    """Decode and validate JWT. Raises JWTError on failure."""
    return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
