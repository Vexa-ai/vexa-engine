from fastapi import APIRouter, Depends, HTTPException
from typing import List, Union, Optional
from datetime import datetime
from uuid import UUID
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from psql_models import User, AccessLevel
from psql_helpers import get_session
from transcript_manager import TranscriptManager
from auth import get_current_user

router = APIRouter(prefix="/api/transcripts", tags=["transcripts"])

class SegmentBase(BaseModel):
    content: str
    start_timestamp: datetime
    confidence: float
    segment_id: int
    words: List[List[Union[str, float]]]  # [text, start_time, end_time]
    speaker: str

class LegacySegment(SegmentBase):
    html_content: Optional[str] = None
    html_content_short: Optional[str] = None
    keywords: List[str] = []
    end_timestamp: Optional[datetime] = None

class UpstreamSegment(SegmentBase):
    meeting_id: str
    end_timestamp: datetime
    server_timestamp: datetime
    transcription_timestamp: datetime
    present_user_ids: List[str] = []
    partially_present_user_ids: List[str] = []

class TranscriptAccessUpdate(BaseModel):
    target_user_id: UUID
    access_level: AccessLevel

class TranscriptError(HTTPException):
    def __init__(self, detail: str, status_code: int = 400):
        super().__init__(status_code=status_code, detail=detail)

class TranscriptNotFoundError(TranscriptError):
    def __init__(self, transcript_id: UUID):
        super().__init__(
            detail=f"Transcript not found: {transcript_id}",
            status_code=404
        )

class ContentNotFoundError(TranscriptError):
    def __init__(self, content_id: UUID):
        super().__init__(
            detail=f"Content not found: {content_id}",
            status_code=404
        )

class AccessDeniedError(TranscriptError):
    def __init__(self):
        super().__init__(
            detail="Access denied",
            status_code=403
        )

async def get_transcript_manager():
    return await TranscriptManager.create()

@router.post("/segments/{content_id}")
async def ingest_transcript_segments(
    content_id: UUID,
    segments: List[Union[LegacySegment, UpstreamSegment]],
    current_user: User = Depends(get_current_user),
    manager: TranscriptManager = Depends(get_transcript_manager),
    session: AsyncSession = Depends(get_session)
) -> List[dict]:
    try:
        # Convert Pydantic models to dicts
        segment_dicts = [segment.model_dump() for segment in segments]
        
        # Ingest segments
        transcripts = await manager.ingest_transcript_segments(
            content_id=content_id,
            segments=segment_dicts,
            user_id=current_user.id,
            session=session
        )
        
        # Return formatted segments
        return await manager.get_transcript_segments(
            content_id=content_id,
            user_id=current_user.id,
            session=session
        )
        
    except ValueError as e:
        if "Content not found" in str(e):
            raise ContentNotFoundError(content_id)
        raise TranscriptError(str(e))

@router.get("/segments/{content_id}")
async def get_transcript_segments(
    content_id: UUID,
    current_user: User = Depends(get_current_user),
    manager: TranscriptManager = Depends(get_transcript_manager),
    session: AsyncSession = Depends(get_session)
) -> List[dict]:
    try:
        return await manager.get_transcript_segments(
            content_id=content_id,
            user_id=current_user.id,
            session=session
        )
    except Exception as e:
        raise TranscriptError(str(e))

@router.put("/{transcript_id}/access")
async def update_transcript_access(
    transcript_id: UUID,
    access_update: TranscriptAccessUpdate,
    current_user: User = Depends(get_current_user),
    manager: TranscriptManager = Depends(get_transcript_manager),
    session: AsyncSession = Depends(get_session)
) -> dict:
    try:
        success = await manager.update_transcript_access(
            transcript_id=transcript_id,
            user_id=current_user.id,
            target_user_id=access_update.target_user_id,
            access_level=access_update.access_level,
            session=session
        )
        
        if not success:
            raise AccessDeniedError()
            
        return {"status": "success"}
        
    except ValueError as e:
        if "Transcript not found" in str(e):
            raise TranscriptNotFoundError(transcript_id)
        raise TranscriptError(str(e)) 