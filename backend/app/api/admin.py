from fastapi import APIRouter, Depends, Header, HTTPException, status
from pydantic import BaseModel, EmailStr, field_validator
from sqlalchemy.orm import Session
import re

from app.config import INTERNAL_API_KEY
from app.database import get_db
from app.dependencies import require_superadmin
from app.models.store import Store
from app.models.user import User
from app.services.auth_service import hash_password

router = APIRouter(prefix="/api/admin", tags=["admin"])


def _require_internal_key(x_api_key: str = Header(...)):
    if not INTERNAL_API_KEY or x_api_key != INTERNAL_API_KEY:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid API key")


class NewStoreRequest(BaseModel):
    store_name: str
    owner_email: EmailStr
    password: str
    timezone: str = "America/New_York"
    currency: str = "USD"

    @field_validator("password")
    @classmethod
    def password_strength(cls, v: str) -> str:
        if len(v) < 8:
            raise ValueError("Password must be at least 8 characters")
        if not re.search(r"\d", v):
            raise ValueError("Password must contain at least one digit")
        return v

    @field_validator("store_name")
    @classmethod
    def name_not_empty(cls, v: str) -> str:
        if len(v.strip()) < 2:
            raise ValueError("Store name must be at least 2 characters")
        return v.strip()


@router.post("/stores", status_code=201)
def create_store(
    payload: NewStoreRequest,
    db: Session = Depends(get_db),
    _: None = Depends(_require_internal_key),
):
    if db.query(User).filter(User.email == payload.owner_email).first():
        raise HTTPException(status_code=409, detail="Email already registered")

    store = Store(
        name=payload.store_name,
        owner_email=payload.owner_email,
        timezone=payload.timezone,
        currency=payload.currency,
    )
    db.add(store)
    db.flush()

    user = User(
        email=payload.owner_email,
        hashed_password=hash_password(payload.password),
        store_id=store.id,
        role="admin",
    )
    db.add(user)
    db.commit()
    db.refresh(store)
    db.refresh(user)

    return {
        "store_id": store.id,
        "user_id": user.id,
        "store_name": store.name,
        "owner_email": user.email,
    }


class SuperadminRequest(BaseModel):
    store_name: str
    owner_email: EmailStr
    password: str

    @field_validator("password")
    @classmethod
    def password_strength(cls, v: str) -> str:
        if len(v) < 8:
            raise ValueError("Password must be at least 8 characters")
        if not re.search(r"\d", v):
            raise ValueError("Password must contain at least one digit")
        return v


@router.post("/create-superadmin", status_code=201)
def create_superadmin(
    payload: SuperadminRequest,
    db: Session = Depends(get_db),
    _: None = Depends(_require_internal_key),
):
    """One-time endpoint: creates the Rodmat master store + superadmin account."""
    if db.query(User).filter(User.email == payload.owner_email).first():
        raise HTTPException(status_code=409, detail="Email already registered")

    store = Store(name=payload.store_name, owner_email=payload.owner_email)
    db.add(store)
    db.flush()

    user = User(
        email=payload.owner_email,
        hashed_password=hash_password(payload.password),
        store_id=store.id,
        role="superadmin",
    )
    db.add(user)
    db.commit()
    db.refresh(user)

    return {"store_id": store.id, "user_id": user.id, "role": "superadmin"}


@router.get("/stores", status_code=200)
def list_stores_key(
    db: Session = Depends(get_db),
    _: None = Depends(_require_internal_key),
):
    stores = db.query(Store).all()
    return [{"id": s.id, "name": s.name, "owner_email": s.owner_email, "created_at": str(s.created_at)} for s in stores]


@router.get("/stores/all", status_code=200)
def list_stores_superadmin(
    user: User = Depends(require_superadmin),
    db: Session = Depends(get_db),
):
    """Lista todas las tiendas — solo accesible con JWT de superadmin."""
    stores = db.query(Store).order_by(Store.name).all()
    return [{"id": s.id, "name": s.name, "owner_email": s.owner_email} for s in stores]


class CreateUserRequest(BaseModel):
    email: EmailStr
    password: str
    store_id: str
    role: str = "viewer"

    @field_validator("role")
    @classmethod
    def valid_role(cls, v: str) -> str:
        allowed = {"superadmin", "admin", "viewer", "warehouse"}
        if v not in allowed:
            raise ValueError(f"role must be one of {allowed}")
        return v

    @field_validator("password")
    @classmethod
    def password_min(cls, v: str) -> str:
        if len(v) < 8:
            raise ValueError("Password must be at least 8 characters")
        return v


@router.post("/users", status_code=201)
def create_user(
    payload: CreateUserRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_superadmin),
):
    """Crea un usuario adicional para una tienda existente. Solo superadmin."""
    if db.query(User).filter(User.email == payload.email).first():
        raise HTTPException(status_code=409, detail="Email already registered")
    store = db.query(Store).filter(Store.id == payload.store_id).first()
    if not store:
        raise HTTPException(status_code=404, detail="Store not found")
    user = User(
        email=payload.email,
        hashed_password=hash_password(payload.password),
        store_id=payload.store_id,
        role=payload.role,
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return {"user_id": user.id, "email": user.email, "role": user.role, "store_id": user.store_id}


@router.get("/users", status_code=200)
def list_users(
    store_id: str | None = None,
    db: Session = Depends(get_db),
    _: User = Depends(require_superadmin),
):
    """Lista usuarios. Filtra por store_id si se indica. Solo superadmin."""
    q = db.query(User)
    if store_id:
        q = q.filter(User.store_id == store_id)
    return [{"user_id": u.id, "email": u.email, "role": u.role, "store_id": u.store_id} for u in q.all()]


class UpdateUserRequest(BaseModel):
    store_id: str | None = None
    role: str | None = None

    @field_validator("role")
    @classmethod
    def valid_role(cls, v: str | None) -> str | None:
        if v is not None and v not in {"superadmin", "admin", "viewer", "warehouse"}:
            raise ValueError("role inválido")
        return v


@router.patch("/users/{user_id}", status_code=200)
def update_user(
    user_id: str,
    payload: UpdateUserRequest,
    db: Session = Depends(get_db),
    _: User = Depends(require_superadmin),
):
    """Actualiza store_id o role de un usuario. Solo superadmin."""
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    if payload.store_id:
        store = db.query(Store).filter(Store.id == payload.store_id).first()
        if not store:
            raise HTTPException(status_code=404, detail="Store not found")
        user.store_id = payload.store_id
    if payload.role:
        user.role = payload.role
    db.commit()
    return {"user_id": user.id, "email": user.email, "role": user.role, "store_id": user.store_id}
