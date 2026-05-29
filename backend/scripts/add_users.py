"""
Add users to the Rodmat store.
Run once: railway run python scripts/add_users.py

Passwords are read from env vars:
  USER_PASSWORD_HORACIO   (default prompts interactively)
  USER_PASSWORD_INFO
  USER_PASSWORD_JH

Or set a single default for all: USER_PASSWORD_DEFAULT
"""
import os
import sys
import getpass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import app.models  # noqa: F401
from app.database import SessionLocal
from app.models.store import Store
from sqlalchemy import func

from app.models.user import User
from app.services.auth_service import hash_password


def _get_password(env_key: str, label: str) -> str:
    default_env = os.getenv("USER_PASSWORD_DEFAULT")
    pw = os.getenv(env_key) or default_env
    if not pw:
        pw = getpass.getpass(f"Password para {label}: ")
    if not pw:
        print(f"ERROR: No se proporcionó contraseña para {label}")
        sys.exit(1)
    return pw


NEW_USERS = [
    {"email": "horacio@rodmat.com", "pw_env": "USER_PASSWORD_HORACIO", "role": "superadmin"},
    {"email": "info@rodmat.com",    "pw_env": "USER_PASSWORD_INFO",    "role": "superadmin"},
    {"email": "jh@rodmat.com",      "pw_env": "USER_PASSWORD_JH",      "role": "admin"},
]

db = SessionLocal()
try:
    rodmat_store = db.query(Store).filter(Store.name == "Rodmat").first()
    if not rodmat_store:
        print("ERROR: Tienda 'Rodmat' no encontrada.")
        sys.exit(1)
    print(f"Tienda Rodmat encontrada (id={rodmat_store.id[:8]})\n")

    for u in NEW_USERS:
        u["email"] = u["email"].lower()
        pw = _get_password(u["pw_env"], u["email"])
        existing = db.query(User).filter(func.lower(User.email) == u["email"]).first()
        if existing:
            existing.hashed_password = hash_password(pw)
            existing.role = u["role"]
            print(f"  UPDATED  {u['email']}  ({u['role']})")
        else:
            new_user = User(
                email=u["email"],
                hashed_password=hash_password(pw),
                store_id=rodmat_store.id,
                role=u["role"],
            )
            db.add(new_user)
            print(f"  CREATED  {u['email']}  ({u['role']})")

    db.commit()
    print("\nTodos los usuarios creados/actualizados correctamente.")

finally:
    db.close()
