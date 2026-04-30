import logging

from fastapi import APIRouter, Depends, Header, HTTPException, status
from sqlalchemy.orm import Session

from app.config import INTERNAL_API_KEY
from app.database import get_db
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


@router.post("/prism")
def run_prism(
    force: bool = False,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Run PRISM agent for the authenticated user's store (manual trigger)."""
    from app.services.agents import prism_agent
    try:
        ran = prism_agent.run(db, user.store_id, force=force)
        return {"status": "sent" if ran else "skipped"}
    except Exception as exc:
        logger.exception("PRISM failed for store %s", user.store_id)
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/haiku")
def run_haiku(
    force: bool = False,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Run HAIKU agent for the authenticated user's store (manual trigger)."""
    from app.services.agents import haiku_agent
    try:
        ran = haiku_agent.run(db, user.store_id, force=force)
        return {"status": "sent" if ran else "skipped"}
    except Exception as exc:
        logger.exception("HAIKU failed for store %s", user.store_id)
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/faraway")
def run_faraway(
    force: bool = False,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Run FARAWAY agent for the authenticated user's store (manual trigger)."""
    from app.services.agents import faraway_agent
    try:
        ran = faraway_agent.run(db, user.store_id, force=force)
        return {"status": "sent" if ran else "skipped"}
    except Exception as exc:
        logger.exception("FARAWAY failed for store %s", user.store_id)
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/mesmerize")
def run_mesmerize(
    force: bool = False,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Run MESMERIZE agent for the authenticated user's store (manual trigger)."""
    from app.services.agents import mesmerize_agent
    try:
        ran = mesmerize_agent.run(db, user.store_id, force=force)
        return {"status": "sent" if ran else "skipped"}
    except Exception as exc:
        logger.exception("MESMERIZE failed for store %s", user.store_id)
        raise HTTPException(status_code=500, detail=str(exc))
