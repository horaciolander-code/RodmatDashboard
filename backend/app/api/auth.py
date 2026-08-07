import logging

from fastapi import APIRouter, Depends, Header, HTTPException, Request, status
from fastapi.security import OAuth2PasswordRequestForm
from slowapi import Limiter
from slowapi.util import get_remote_address
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.store import Store
from app.models.brand import Brand
from app.models.user import User
from app.schemas.user import UserRegister, TokenResponse, UserResponse
from app.services.auth_service import hash_password, verify_password, create_access_token
from app.dependencies import get_current_user

logger = logging.getLogger("rodmat.auth")
limiter = Limiter(key_func=get_remote_address)

router = APIRouter(prefix="/api/auth", tags=["auth"])


def _require_internal_key(x_api_key: str = Header(...)):
    from app.config import INTERNAL_API_KEY
    if not INTERNAL_API_KEY or x_api_key != INTERNAL_API_KEY:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Registration is closed")


@router.post("/register", response_model=TokenResponse, status_code=201)
@limiter.limit("3/hour")
def register(request: Request, payload: UserRegister,
             db: Session = Depends(get_db),
             _: None = Depends(_require_internal_key)):
    email = payload.email.lower()
    existing = db.query(User).filter(func.lower(User.email) == email).first()
    if existing:
        raise HTTPException(status_code=409, detail="Email already registered")

    store = Store(name=payload.store_name, owner_email=email)
    db.add(store)
    db.flush()

    user = User(
        email=email,
        hashed_password=hash_password(payload.password),
        store_id=store.id,
        role="admin",
    )
    db.add(user)
    db.commit()
    db.refresh(user)

    token = create_access_token(user.id, store.id, user.role)
    return TokenResponse(access_token=token)


@router.post("/login", response_model=TokenResponse)
@limiter.limit("20/minute")
def login(request: Request, form: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
    username = (form.username or "").strip().lower()
    user = db.query(User).filter(func.lower(User.email) == username).first()
    if not user or not verify_password(form.password, user.hashed_password):
        logger.warning("Failed login attempt for email=%s from ip=%s", username, request.client.host)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    token = create_access_token(user.id, user.store_id, user.role)
    return TokenResponse(access_token=token)


@router.get("/me", response_model=UserResponse)
def get_me(user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    store = db.query(Store).filter(Store.id == user.store_id).first()
    settings = (store.settings or {}) if store else {}
    brands_on = bool(getattr(store, "brands_enabled", False)) if store else False

    user_brand = None
    if user.brand_id:
        user_brand = db.query(Brand).filter(Brand.id == user.brand_id).first()

    available = []
    if brands_on and store:
        rows = db.query(Brand).filter(Brand.store_id == store.id, Brand.is_active == True).order_by(Brand.slug).all()
        available = [{
            "id": b.id,
            "slug": b.slug,
            "display_name": b.display_name,
            "brand_color": b.brand_color,
            "absorbs_shared_costs": b.absorbs_shared_costs,
        } for b in rows]

    return UserResponse(
        id=user.id,
        email=user.email,
        store_id=user.store_id,
        store_name=store.name if store else None,
        role=user.role,
        created_at=user.created_at,
        modules_enabled=settings.get("modules_enabled"),
        platforms_enabled=settings.get("platforms_enabled"),
        brand_id=user.brand_id,
        brand_slug=user_brand.slug if user_brand else None,
        brand_display_name=user_brand.display_name if user_brand else None,
        brands_enabled=brands_on,
        available_brands=available if brands_on else None,
    )
