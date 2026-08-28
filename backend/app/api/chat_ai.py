"""Chat AI endpoint — DEMO gated to a single user by email."""
from __future__ import annotations
import logging
from typing import Any, Optional, Union
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.database import get_db
from app.dependencies import get_current_user
from app.models.user import User
from app.services.chat_ai_service import answer_question

log = logging.getLogger("rodmat.chat_ai_api")

router = APIRouter(prefix="/api/chat", tags=["chat_ai"])

# ACL — DEMO: solo este email tiene acceso
ALLOWED_EMAILS = {"rodmatwh@gmail.com"}


class QueryIn(BaseModel):
    question: str


class QueryOut(BaseModel):
    answer: str
    path: str
    tool_used: Optional[str] = None
    sql_executed: Optional[str] = None
    raw_data: Optional[Any] = None
    router_reason: Optional[str] = None


def _require_beta_access(user: User = Depends(get_current_user)) -> User:
    if (user.email or "").lower() not in ALLOWED_EMAILS:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Chat AI está en beta. Pídele acceso a Horacio.",
        )
    return user


@router.post("/query", response_model=QueryOut)
def chat_query(
    payload: QueryIn,
    user: User = Depends(_require_beta_access),
    db: Session = Depends(get_db),
):
    q = (payload.question or "").strip()
    if not q or len(q) > 500:
        raise HTTPException(400, "Pregunta vacía o demasiado larga (max 500 chars).")
    log.info(f"chat_query user={user.email} q={q[:80]!r}")
    try:
        result = answer_question(db, user.store_id, user.brand_id, q)
    except Exception as e:
        log.exception("answer_question crashed")
        raise HTTPException(500, f"Error interno: {str(e)[:100]}")
    return QueryOut(**result)


@router.get("/beta-check")
def beta_check(user: User = Depends(get_current_user)):
    """Frontend uses this to decide whether to show the Chat AI page."""
    return {"enabled": (user.email or "").lower() in ALLOWED_EMAILS}
