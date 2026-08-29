"""FUSE Demo public endpoint — LLM proxy for FUSE_DEMO frontend."""
from __future__ import annotations
import os, logging, time
from typing import Any, Optional, List, Dict
from collections import defaultdict, deque
from fastapi import APIRouter, HTTPException, Header, Request
from pydantic import BaseModel
import httpx

log = logging.getLogger("fuse.demo")
router = APIRouter(prefix="/api/fuse-demo", tags=["fuse_demo"])

GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL = os.getenv("GROQ_MODEL", "openai/gpt-oss-20b")
GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"
DEMO_TOKEN = os.getenv("FUSE_DEMO_TOKEN", "fuse-demo-2026")

_RATE_LIMIT = 100
_RATE_WINDOW = 3600
_ip_buckets: Dict[str, deque] = defaultdict(deque)


def _check_rate_limit(ip: str) -> bool:
    now = time.time()
    b = _ip_buckets[ip]
    while b and b[0] < now - _RATE_WINDOW: b.popleft()
    if len(b) >= _RATE_LIMIT: return False
    b.append(now)
    return True


class ChatIn(BaseModel):
    messages: List[Dict[str, str]]
    temperature: float = 0.4
    max_tokens: int = 800
    response_format: Optional[str] = None


class ChatOut(BaseModel):
    content: str
    model: str


def _get_ip(request: Request) -> str:
    fwd = request.headers.get("x-forwarded-for", "")
    if fwd: return fwd.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


@router.post("/chat", response_model=ChatOut)
def fuse_chat(payload: ChatIn, request: Request, x_demo_token: str = Header(default="")):
    if x_demo_token != DEMO_TOKEN:
        raise HTTPException(403, "Invalid demo token")
    ip = _get_ip(request)
    if not _check_rate_limit(ip):
        raise HTTPException(429, f"Rate limit: {_RATE_LIMIT}/h per IP")
    if not GROQ_API_KEY:
        raise HTTPException(500, "GROQ_API_KEY not configured")

    body: Dict[str, Any] = {
        "model": GROQ_MODEL,
        "messages": payload.messages,
        "temperature": payload.temperature,
        "max_tokens": min(payload.max_tokens, 2000),
    }
    if payload.response_format == "json_object":
        body["response_format"] = {"type": "json_object"}

    try:
        r = httpx.post(GROQ_URL,
            headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
            json=body, timeout=25)
        r.raise_for_status()
    except httpx.HTTPError as e:
        log.exception(f"Groq: {e}")
        raise HTTPException(502, f"Upstream LLM error: {str(e)[:150]}")

    data = r.json()
    return ChatOut(content=data["choices"][0]["message"]["content"], model=data.get("model", GROQ_MODEL))


@router.get("/health")
def health():
    return {"ok": True, "groq_configured": bool(GROQ_API_KEY), "rate_limit": f"{_RATE_LIMIT}/hour/IP"}
