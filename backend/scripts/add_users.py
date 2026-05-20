"""
Add users to the Rodmat store.
Run once: railway run python scripts/add_users.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import app.models  # noqa: F401
from app.database import SessionLocal
from app.models.store import Store
from app.models.user import User
from app.services.auth_service import hash_password

NEW_USERS = [
    {"email": "horacio@rodmat.com",  "password": ">[REMOVED_FROM_HISTORY]", "role": "superadmin"},
    {"email": "info@rodmat.com",     "password": ">[REMOVED_FROM_HISTORY]", "role": "superadmin"},
    {"email": "Jh@rodmat.com",       "password": ">[REMOVED_FROM_HISTORY]", "role": "admin"},
]

db = SessionLocal()
try:
    rodmat_store = db.query(Store).filter(Store.name == "Rodmat").first()
    if not rodmat_store:
        print("ERROR: Tienda 'Rodmat' no encontrada.")
        sys.exit(1)
    print(f"Tienda Rodmat encontrada (id={rodmat_store.id[:8]})\n")

    for u in NEW_USERS:
        existing = db.query(User).filter(User.email == u["email"]).first()
        if existing:
            existing.hashed_password = hash_password(u["password"])
            existing.role = u["role"]
            print(f"  UPDATED  {u['email']}  ({u['role']})")
        else:
            new_user = User(
                email=u["email"],
                hashed_password=hash_password(u["password"]),
                store_id=rodmat_store.id,
                role=u["role"],
            )
            db.add(new_user)
            print(f"  CREATED  {u['email']}  ({u['role']})")

    db.commit()
    print("\nTodos los usuarios creados/actualizados correctamente.")
    print("\nResumen de accesos:")
    print("  horacio@rodmat.com  / >[REMOVED_FROM_HISTORY]  → superadmin (todo + StoreSwitcher)")
    print("  info@rodmat.com     / >[REMOVED_FROM_HISTORY]  → superadmin (todo + StoreSwitcher)")
    print("  Jh@rodmat.com       / >[REMOVED_FROM_HISTORY]  → admin      (Dashboard + Gestion + Finance)")
    print("  Rodmatwh@gmail.com  / >[REMOVED_FROM_HISTORY]  → superadmin (sin cambios)")
    print("  gestion@rodmat.com  / >[REMOVED_FROM_HISTORY] → warehouse (solo Gestion)")

finally:
    db.close()
