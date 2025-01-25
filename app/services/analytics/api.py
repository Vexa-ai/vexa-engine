from fastapi import APIRouter, Depends, HTTPException, Header
from typing import Optional, Dict, Any
from datetime import datetime
from pydantic import BaseModel
from uuid import UUID
import json
from psql_models import Base, UserToken
from psql_helpers import async_session
from sqlalchemy import select
from .models import AnalyticsSession, AnalyticsEvent, PageView, FeatureUsage

# Import auth helper
async def get_optional_user(authorization: Optional[str] = Header(None)):
    if not authorization:
        return None
    try:
        token = authorization.split(' ')[1]
        async with async_session() as session:
            stmt = select(UserToken).where(UserToken.token == token)
            result = await session.execute(stmt)
            user_token = result.scalar_one_or_none()
            if user_token:
                return (user_token.user_id, token)
    except Exception:
        return None
    return None

router = APIRouter(prefix="/analytics", tags=["analytics"])

class SessionStart(BaseModel):
    referrer: Optional[str]
    initial_path: str
    utm_source: Optional[str]
    utm_medium: Optional[str]
    utm_campaign: Optional[str]
    utm_content: Optional[str]
    utm_term: Optional[str]
    device_info: Dict[str, Any]

class EventCreate(BaseModel):
    event_type: str
    event_name: str
    path: str
    duration: Optional[int]
    event_metadata: Optional[Dict[str, Any]]

@router.post("/session/start")
async def start_session(
    data: SessionStart,
    current_user: Optional[tuple] = Depends(get_optional_user)
):
    try:
        session = AnalyticsSession(
            session_id=str(uuid.uuid4()),
            user_id=current_user[0] if current_user else None,
            start_time=datetime.utcnow(),
            initial_referrer=data.referrer,
            initial_path=data.initial_path,
            utm_source=data.utm_source,
            utm_medium=data.utm_medium,
            utm_campaign=data.utm_campaign,
            utm_content=data.utm_content,
            utm_term=data.utm_term,
            device_type=data.device_info.get('type'),
            browser=data.device_info.get('browser'),
            os=data.device_info.get('os'),
            is_mobile=data.device_info.get('isMobile')
        )
        
        async with async_session() as db:
            db.add(session)
            await db.commit()
            
        return {"session_id": session.session_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/event")
async def track_event(
    session_id: str,
    event: EventCreate
):
    try:
        analytics_event = AnalyticsEvent(
            session_id=session_id,
            event_type=event.event_type,
            event_name=event.event_name,
            path=event.path,
            duration=event.duration,
            event_metadata=event.event_metadata
        )
        
        async with async_session() as db:
            db.add(analytics_event)
            await db.commit()
            
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/pageview")
async def track_pageview(
    session_id: str,
    path: str,
    scroll_depth: Optional[int] = None
):
    try:
        page_view = PageView(
            session_id=session_id,
            path=path,
            scroll_depth=scroll_depth
        )
        
        async with async_session() as db:
            db.add(page_view)
            await db.commit()
            
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/feature")
async def track_feature(
    session_id: str,
    feature: str,
    action: str,
    metadata: Optional[Dict[str, Any]] = None
):
    try:
        feature_usage = FeatureUsage(
            session_id=session_id,
            feature=feature,
            action=action,
            metadata=metadata
        )
        
        async with async_session() as db:
            db.add(feature_usage)
            await db.commit()
            
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 