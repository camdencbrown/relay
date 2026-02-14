"""
Analytics endpoints - platform usage metrics.
"""

from fastapi import APIRouter

from ..storage import Storage

router = APIRouter(prefix="/analytics", tags=["analytics"])

_storage = Storage()


@router.get("/summary")
async def get_analytics_summary():
    """Event counts grouped by type + recent 50 events."""
    return _storage.get_analytics_summary()


@router.get("/events")
async def list_analytics_events(event_type: str = None, pipeline_id: str = None, limit: int = 100):
    """List analytics events with optional filters."""
    events = _storage.list_events(event_type=event_type, pipeline_id=pipeline_id, limit=limit)
    return {"events": events, "count": len(events)}
