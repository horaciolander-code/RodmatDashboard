from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.orm import Session
from jose import JWTError

from app.database import get_db
from app.models.user import User
from app.services.auth_service import decode_token

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login")


def get_current_user(
    token: str = Depends(oauth2_scheme),
    db: Session = Depends(get_db),
) -> User:
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or expired token",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = decode_token(token)
        user_id: str = payload.get("sub")
        if user_id is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception

    user = db.query(User).filter(User.id == user_id).first()
    if user is None:
        raise credentials_exception
    return user


def require_admin(user: User = Depends(get_current_user)) -> User:
    if user.role not in ("admin", "superadmin"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required",
        )
    return user


def require_superadmin(user: User = Depends(get_current_user)) -> User:
    if user.role != "superadmin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Superadmin access required",
        )
    return user


def get_current_store_id(user: User = Depends(get_current_user)) -> str:
    return user.store_id

def require_finance_enabled(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> User:
    """403 unless the user's store has settings.modules_enabled.finance == True.

    The finance module is operator-specific (Rodmat's own bank ingest), not a
    generic SaaS feature. Tenants without it explicitly enabled cannot reach
    /api/finance/* endpoints — protects against new tenants accidentally seeing
    or hitting this surface."""
    from app.models.store import Store
    store = db.query(Store).filter(Store.id == user.store_id).first()
    settings = (store.settings or {}) if store else {}
    enabled = bool((settings.get("modules_enabled") or {}).get("finance", False))
    if not enabled:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Finance module is not enabled for this store",
        )
    return user
