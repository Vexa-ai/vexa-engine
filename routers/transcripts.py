from fastapi import APIRouter, Depends, HTTPException
from typing import List, Union, Optional, Dict, Any
from datetime import datetime
from uuid import UUID, uuid5, NAMESPACE_URL
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from psql_models import User, AccessLevel, ExternalIDType
from psql_helpers import get_session
from transcript_manager import TranscriptManager
from auth import get_current_user
import logging
from sqlalchemy.exc import IntegrityError

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/transcripts", tags=["transcripts"])

def generate_deterministic_uuid(external_id: str, external_id_type: str) -> UUID:
    """Generate a deterministic UUID from external_id and type"""
    name = f"{external_id_type}:{external_id}"
    return uuid5(NAMESPACE_URL, name)

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
    def __init__(self, external_id: str, external_id_type: str):
        super().__init__(
            detail=f"Content not found with external_id {external_id} and type {external_id_type}",
            status_code=404
        )

class AccessDeniedError(TranscriptError):
    def __init__(self):
        super().__init__(
            detail="Access denied",
            status_code=403
        )

class InvalidExternalIDTypeError(TranscriptError):
    def __init__(self, external_id_type: str):
        super().__init__(
            detail=f"Invalid external_id_type: {external_id_type}",
            status_code=400
        )

async def get_transcript_manager():
    return await TranscriptManager.create()

@router.post("/segments/{external_id_type}/{external_id}")
async def ingest_transcript_segments(
    external_id: str,
    external_id_type: str,
    segments: List[Dict[str, Any]],
    current_user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
    transcript_manager: TranscriptManager = Depends(get_transcript_manager)
):
    try:
        # Validate external_id_type
        if external_id_type not in [e.value for e in ExternalIDType]:
            raise InvalidExternalIDTypeError(external_id_type)

        result = await transcript_manager.ingest_transcript_segments(
            external_id=external_id,
            external_id_type=external_id_type,
            segments=segments,
            user_id=current_user.id,
            session=session
        )
        return result
    except InvalidExternalIDTypeError as e:
        raise e
    except ValueError as e:
        if "Content not found" in str(e):
            raise ContentNotFoundError(external_id, external_id_type)
        raise HTTPException(status_code=400, detail=str(e))
    except IntegrityError as e:
        # Check if it's specifically a user not found error
        if 'content_access_user_id_fkey' in str(e) and 'not present in table "users"' in str(e):
            raise HTTPException(
                status_code=404,
                detail="User not found. Cannot create content access record."
            )
        # Re-raise other integrity errors
        raise
    except Exception as e:
        logger.error(f"Error ingesting transcript segments: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/segments/{external_id_type}/{external_id}")
async def get_transcript_segments(
    external_id: str,
    external_id_type: str,
    last_msg_timestamp: Optional[datetime] = None,
    current_user: User = Depends(get_current_user),
    manager: TranscriptManager = Depends(get_transcript_manager),
    session: AsyncSession = Depends(get_session)
) -> List[dict]:
    try:
        # Validate external_id_type
        if external_id_type not in [e.value for e in ExternalIDType]:
            raise InvalidExternalIDTypeError(external_id_type)

        return await manager.get_transcript_segments(
            external_id=external_id,
            external_id_type=external_id_type,
            user_id=current_user.id,
            last_msg_timestamp=last_msg_timestamp,
            session=session
        )
    except InvalidExternalIDTypeError as e:
        raise e
    except ValueError as e:
        if "Content not found" in str(e):
            raise ContentNotFoundError(external_id, external_id_type)
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error getting transcript segments: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

@router.put("/transcripts/{transcript_id}/access")
async def update_transcript_access(
    transcript_id: UUID,
    access_update: TranscriptAccessUpdate,
    current_user: User = Depends(get_current_user),
    manager: TranscriptManager = Depends(get_transcript_manager),
    session: AsyncSession = Depends(get_session)
) -> dict:
    try:
        result = await manager.update_transcript_access(
            transcript_id=transcript_id,
            user_id=current_user.id,
            target_user_id=access_update.target_user_id,
            access_level=access_update.access_level,
            session=session
        )
        
        if result:
            return {"status": "success"}
        else:
            raise HTTPException(status_code=404, detail="Transcript not found")
    except ValueError as e:
        if "Transcript not found" in str(e):
            raise HTTPException(status_code=404, detail="Transcript not found")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error updating transcript access: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error") 