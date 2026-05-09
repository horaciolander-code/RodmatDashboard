import logging

from fastapi import APIRouter, BackgroundTasks, Depends, Header, HTTPException, status
from sqlalchemy.orm import Session

from app.config import INTERNAL_API_KEY
from app.database import get_db, SessionLocal
from app.models.user import User
from app.models.store import Store
from app.dependencies import get_current_user

logger = logging.getLogger("rodmat.agents")

router = APIRouter(prefix="/api/agents", tags=["agents"])


def _require_internal_key(x_api_key: str = Header(...)):
    if not INTERNAL_API_KEY or x_api_key != INTERNAL_API_KEY:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid API key")


def _all_store_ids(db: Session) -> list[str]:
    return [s.id for s in db.query(Store).filter(Store.is_active == True).all()]


def _run_agent_bg(agent_module_name: str, store_id: str, force: bool):
    """Run a single agent in a background task with its own DB session."""
    db = SessionLocal()
    try:
        import importlib
        agent = importlib.import_module(f"app.services.agents.{agent_module_name}_agent")
        ran = agent.run(db, store_id, force=force)
        logger.info("Agent %s store %s bg: %s", agent_module_name, store_id[:8], "sent" if ran else "skipped")
    except Exception as exc:
        logger.exception("Agent %s bg failed for store %s: %s", agent_module_name, store_id[:8], exc)
    finally:
        db.close()


@router.post("/run-all")
def run_all_agents(
    db: Session = Depends(get_db),
    _: None = Depends(_require_internal_key),
):
    """Cron endpoint — called daily. Runs whichever agents are scheduled for today (all stores)."""
    from app.services.agents import prism_agent, haiku_agent, faraway_agent, mesmerize_agent

    results = {}
    for store_id in _all_store_ids(db):
        store_results = {}
        for name, agent in [
            ("prism",     prism_agent),
            ("haiku",     haiku_agent),
            ("faraway",   faraway_agent),
            ("mesmerize", mesmerize_agent),
        ]:
            try:
                ran = agent.run(db, store_id)
                store_results[name] = "sent" if ran else "skipped"
            except Exception as exc:
                logger.exception("Agent %s failed for store %s", name, store_id)
                store_results[name] = f"error: {exc}"
        results[store_id] = store_results

    logger.info("run-all agents: %s", results)
    return {"status": "done", "results": results}


@router.get("/diag-groq")
def diag_groq(user: User = Depends(get_current_user)):
    """Synchronous Groq diagnostic — returns result or error directly."""
    import os
    key = os.getenv("GROQ_API_KEY", "")
    if not key:
        return {"groq_key_set": False, "error": "GROQ_API_KEY env var is empty"}
    try:
        from groq import Groq
        client = Groq(api_key=key)
        r = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": "Reply with just: OK"}],
            max_tokens=5,
        )
        return {"groq_key_set": True, "key_prefix": key[:12] + "...", "result": r.choices[0].message.content}
    except Exception as exc:
        return {"groq_key_set": True, "key_prefix": key[:12] + "...", "error": str(exc)}


@router.post("/prism")
def run_prism(
    background_tasks: BackgroundTasks,
    force: bool = False,
    user: User = Depends(get_current_user),
):
    """Queues PRISM in background — returns immediately to avoid Railway 60s timeout."""
    background_tasks.add_task(_run_agent_bg, "prism", user.store_id, force)
    return {"status": "queued", "agent": "PRISM", "store": user.store_id[:8]}


@router.post("/haiku")
def run_haiku(
    background_tasks: BackgroundTasks,
    force: bool = False,
    user: User = Depends(get_current_user),
):
    """Queues HAIKU in background — returns immediately to avoid Railway 60s timeout."""
    background_tasks.add_task(_run_agent_bg, "haiku", user.store_id, force)
    return {"status": "queued", "agent": "HAIKU", "store": user.store_id[:8]}


@router.post("/faraway")
def run_faraway(
    background_tasks: BackgroundTasks,
    force: bool = False,
    user: User = Depends(get_current_user),
):
    """Queues FARAWAY in background — returns immediately to avoid Railway 60s timeout."""
    background_tasks.add_task(_run_agent_bg, "faraway", user.store_id, force)
    return {"status": "queued", "agent": "FARAWAY", "store": user.store_id[:8]}


@router.post("/mesmerize")
def run_mesmerize(
    background_tasks: BackgroundTasks,
    force: bool = False,
    user: User = Depends(get_current_user),
):
    """Queues MESMERIZE in background — returns immediately to avoid Railway 60s timeout."""
    background_tasks.add_task(_run_agent_bg, "mesmerize", user.store_id, force)
    return {"status": "queued", "agent": "MESMERIZE", "store": user.store_id[:8]}
