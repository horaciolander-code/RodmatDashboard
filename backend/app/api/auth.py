import logging

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.security import OAuth2PasswordRequestForm
from slowapi import Limiter
from slowapi.util import get_remote_address
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.store import Store
from app.models.user import User
from app.schemas.user import UserRegister, TokenResponse, UserResponse
from app.services.auth_service import hash_password, verify_password, create_access_token
from app.dependencies import get_current_user

logger = logging.getLogger("rodmat.auth")
limiter = Limiter(key_func=get_remote_address)

router = APIRouter(prefix="/api/auth", tags=["auth"])


@router.post("/register", response_model=TokenResponse, status_code=201)
@limiter.limit("3/hour")
def register(request: Request, payload: UserRegister, db: Session = Depends(get_db)):
    existing = db.query(User).filter(User.email == payload.email).first()
    if existing:
        raise HTTPException(status_code=409, detail="Email already registered")

    store = Store(name=payload.store_name, owner_email=payload.email)
    db.add(store)
    db.flush()

    user = User(
        email=payload.email,
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
@limiter.limit("5/minute")
def login(request: Request, form: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
    user = db.query(User).filter(User.email == form.username).first()
    if not user or not verify_password(form.password, user.hashed_password):
        logger.warning("Failed login attempt for email=%s from ip=%s", form.username, request.client.host)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    token = create_access_token(user.id, user.store_id, user.role)
    return TokenResponse(access_token=token)


@router.get("/me", response_model=UserResponse)
def get_me(user: User = Depends(get_current_user)):
    return user
