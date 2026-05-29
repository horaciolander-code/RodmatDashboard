import logging

from fastapi import APIRouter, BackgroundTasks, Depends, Header, HTTPException, status
from sqlalchemy.orm import Session

from app.config import INTERNAL_API_KEY
from app.database import get_db, SessionLocal
from app.models.user import User
from app.dependencies import get_current_user

logger = logging.getLogger("rodmat.agents")

router = APIRouter(prefix="/api/agents", tags=["agents"])


def _require_internal_key(x_api_key: str = Header(...)):
    if not INTERNAL_API_KEY or x_api_key != INTERNAL_API_KEY:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid API key")


def _run_agent_bg(agent_module_name: str, store_id: str, force: bool, test_email: str | None = None):
    """Run a single agent in a background task with its own DB session."""
    db = SessionLocal()
    try:
        import importlib
        agent = importlib.import_module(f"app.services.agents.{agent_module_name}_agent")
        ran = agent.run(db, store_id, force=force, test_email=test_email)
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
    """Cron endpoint — called daily. Runs whichever agents are scheduled for
    today (all stores), gated by data freshness. Logs every run attempt to
    agent_runs and sends a consolidated stale-data alert per store if any
    agent was skipped because no file was uploaded today."""
    from app.services.scheduled_jobs import run_scheduled_agents
    results = run_scheduled_agents(db)
    logger.info("run-all agents: %s", results)
    return {"status": "done", "results": results}


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
    test_email: str | None = None,
    user: User = Depends(get_current_user),
):
    """Queues FARAWAY in background. test_email overrides recipients (for testing, solo a ti)."""
    background_tasks.add_task(_run_agent_bg, "faraway", user.store_id, force, test_email)
    return {"status": "queued", "agent": "FARAWAY", "store": user.store_id[:8], "test_email": test_email}


@router.post("/mesmerize")
def run_mesmerize(
    background_tasks: BackgroundTasks,
    force: bool = False,
    user: User = Depends(get_current_user),
):
    """Queues MESMERIZE in background — returns immediately to avoid Railway 60s timeout."""
    background_tasks.add_task(_run_agent_bg, "mesmerize", user.store_id, force)
    return {"status": "queued", "agent": "MESMERIZE", "store": user.store_id[:8]}
