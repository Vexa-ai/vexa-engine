from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Any, Tuple
from uuid import UUID, uuid4, uuid5, NAMESPACE_URL
from sqlalchemy import select, delete, and_, func
from sqlalchemy.ext.asyncio import AsyncSession
from psql_models import Content, Transcript, TranscriptAccess, AccessLevel, ContentType, ExternalIDType, UserContent, ContentAccess
from psql_helpers import get_session
import logging

logger = logging.getLogger(__name__)

def generate_deterministic_uuid(external_id: str, external_id_type: str) -> UUID:
    """Generate a deterministic UUID from external_id and type"""
    name = f"{external_id_type}:{external_id}"
    return uuid5(NAMESPACE_URL, name)

class TranscriptManager:
    def __init__(self):
        pass
    
    @classmethod
    async def create(cls):
        return cls()
    
    def _detect_format(self, segment: Dict[str, Any]) -> str:
        """Detect if segment is from legacy or upstream API"""
        # Legacy segments have html_content and html_content_short fields
        if 'html_content' in segment or 'html_content_short' in segment:
            return 'legacy'
        # Upstream segments have meeting_id field
        if 'meeting_id' in segment:
            return 'upstream'
        return 'legacy'  # Default to legacy if can't determine
    
    def _estimate_end_timestamp(self, start_time: datetime, words: List) -> datetime:
        """Estimate end timestamp for legacy segments"""
        if not words:
            return start_time + timedelta(seconds=5)  # Default duration
            
        # Use last word's end time if available
        last_word = words[-1]
        if len(last_word) >= 3:
            duration = last_word[2]  # End time of last word
            return start_time + timedelta(seconds=duration)
            
        return start_time + timedelta(seconds=5)  # Fallback
    
    def _normalize_segment(self, segment: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize segment data to common format"""
        format_type = self._detect_format(segment)
        
        # Parse timestamps
        start_time = datetime.fromisoformat(segment['start_timestamp'].replace('Z', '+00:00'))
        
        if format_type == 'legacy':
            end_time = self._estimate_end_timestamp(start_time, segment['words'])
        else:
            end_time = datetime.fromisoformat(segment['end_timestamp'].replace('Z', '+00:00'))
        
        # Build metadata
        metadata = {
            'source': format_type,
            'speaker': segment['speaker'],
            'original_format': segment,
            'versions': {}
        }
        
        # Add upstream-specific fields if available
        if format_type == 'upstream':
            metadata.update({
                'server_timestamp': segment.get('server_timestamp'),
                'transcription_timestamp': segment.get('transcription_timestamp'),
                'present_user_ids': segment.get('present_user_ids', []),
                'partially_present_user_ids': segment.get('partially_present_user_ids', [])
            })
        
        return {
            'text_content': segment['content'],
            'start_timestamp': start_time,
            'end_timestamp': end_time,
            'confidence': segment['confidence'],
            'original_segment_id': segment['segment_id'],
            'word_timing_data': {'words': segment['words']},
            'segment_metadata': metadata
        }
    
    def _validate_segment(self, segment: Dict[str, Any]) -> None:
        """Validate segment data before ingestion"""
        # Convert list format to dict if needed
        if isinstance(segment, list):
            if len(segment) < 6:
                raise ValueError("Invalid list segment format - missing required fields")
            segment = {
                'content': segment[0],
                'start_timestamp': segment[1],
                'confidence': segment[2],
                'segment_id': segment[3],
                'words': segment[4],
                'speaker': segment[5]
            }
        # Check required fields
        required_fields = ['content', 'start_timestamp', 'confidence', 
                         'segment_id', 'words', 'speaker']
        for field in required_fields:
            if field not in segment or segment[field] is None:
                raise ValueError(f"Missing required field: {field}")
        # Validate types
        if not isinstance(segment['content'], str):
            raise ValueError("Content must be a string")
        if not isinstance(segment['segment_id'], int):
            raise ValueError("segment_id must be an integer")
        if not isinstance(segment['confidence'], (int, float)) or not 0 <= segment['confidence'] <= 1:
            raise ValueError("confidence must be a number between 0 and 1")
        if not isinstance(segment['words'], list):
            raise ValueError("words must be a list")
        # Validate timestamps
        try:
            datetime.fromisoformat(segment['start_timestamp'].replace('Z', '+00:00'))
            if 'end_timestamp' in segment:
                datetime.fromisoformat(segment['end_timestamp'].replace('Z', '+00:00'))
        except ValueError as e:
            raise ValueError(f"Invalid timestamp format: {str(e)}")
        # Validate word timing data
        for word in segment['words']:
            if not isinstance(word, list) or len(word) != 3:
                raise ValueError("Each word must be a list of [text, start_time, end_time]")
            if not isinstance(word[0], str):
                raise ValueError("Word text must be a string")
            if not isinstance(word[1], (int, float)) or not isinstance(word[2], (int, float)):
                raise ValueError("Word timings must be numbers")
            if word[1] > word[2]:
                raise ValueError("Word start time must be before end time")
    
    async def _get_or_create_content(
        self,
        external_id: str,
        external_id_type: str,
        user_id: UUID,
        session: AsyncSession
    ) -> Content:
        """Get or create content with the given external ID and type"""
        # Generate deterministic UUID for content
        content_id = generate_deterministic_uuid(external_id, external_id_type)
        
        # Check if content exists
        content = await session.execute(
            select(Content).where(Content.id == content_id)
        )
        content = content.scalar_one_or_none()
        
        if content:
            return content
            
        # Create new content
        content = Content(
            id=content_id,
            type=ContentType.MEETING.value,
            text="",
            timestamp=datetime.now(timezone.utc),
            external_id=external_id,
            external_id_type=external_id_type
        )
        # Set user_id for mock session
        setattr(content, '_user_id', user_id)
        session.add(content)
        
        # Create user content association
        user_content = UserContent(
            content_id=content.id,
            user_id=user_id,
            access_level=AccessLevel.OWNER.value,
            created_by=user_id,
            is_owner=True
        )
        session.add(user_content)
        
        # Create content access
        content_access = ContentAccess(
            content_id=content.id,
            user_id=user_id,
            access_level=AccessLevel.OWNER.value,
            granted_by=user_id
        )
        session.add(content_access)
        
        await session.commit()
        return content
    
    async def ingest_transcript_segments(
        self,
        external_id: str,
        external_id_type: str,
        segments: List[Dict[str, Any]],
        user_id: UUID,
        session: AsyncSession = None
    ) -> List[Transcript]:
        async with (session or get_session()) as session:
            try:
                # Get or create content
                content = await self._get_or_create_content(
                    external_id=external_id,
                    external_id_type=external_id_type,
                    user_id=user_id,
                    session=session
                )
                
                # Validate and normalize all segments
                normalized_segments = []
                for segment in segments:
                    self._validate_segment(segment)
                    normalized = self._normalize_segment(segment)
                    normalized_segments.append(normalized)
                
                # Check for existing transcripts for this content
                existing_q = await session.execute(select(Transcript).where(Transcript.content_id==content.id))
                existing = {t.original_segment_id: t for t in existing_q.scalars().all()}
                transcripts = []
                for seg in normalized_segments:
                    seg_id = seg['original_segment_id']
                    if seg_id in existing:
                        transcripts.append(existing[seg_id])
                    else:
                        t = Transcript(content_id=content.id,
                                       text_content=seg['text_content'],
                                       start_timestamp=seg['start_timestamp'],
                                       end_timestamp=seg['end_timestamp'],
                                       confidence=seg['confidence'],
                                       original_segment_id=seg['original_segment_id'],
                                       word_timing_data=seg['word_timing_data'],
                                       segment_metadata=seg['segment_metadata'])
                        transcripts.append(t)
                        ta = TranscriptAccess(transcript=t, user_id=user_id, access_level=AccessLevel.OWNER.value, granted_by=user_id)
                        session.add(ta)
                        session.add(t)
                await session.commit()
                return transcripts
                
            except Exception as e:
                await session.rollback()
                logger.error(f"Error ingesting transcript segments: {str(e)}")
                raise
    
    async def get_transcript_segments(
        self,
        external_id: str,
        external_id_type: str,
        user_id: UUID,
        last_msg_timestamp: Optional[datetime] = None,
        session: AsyncSession = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        async with (session or get_session()) as session:
            try:
                # Get content ID
                content_id = generate_deterministic_uuid(external_id, external_id_type)
                
                # Check if content exists and user has access
                content = await session.execute(
                    select(Content)
                    .join(UserContent)
                    .where(
                        and_(
                            Content.id == content_id,
                            UserContent.user_id == user_id,
                            UserContent.access_level != AccessLevel.REMOVED.value
                        )
                    )
                )
                content = content.scalar_one_or_none()
                
                if not content:
                    raise ValueError("Content not found")
                
                # Build query for transcripts
                query = select(Transcript).where(Transcript.content_id == content_id)
                
                # Add timestamp filter if provided
                if last_msg_timestamp:
                    query = query.where(Transcript.start_timestamp > last_msg_timestamp)
                
                # Order by timestamp and limit results
                query = query.order_by(Transcript.start_timestamp).limit(limit)
                
                # Get transcripts
                transcripts = await session.execute(query)
                transcripts = transcripts.scalars().all()
                
                # Convert to segments with simplified format
                segments = []
                for transcript in transcripts:
                    metadata = transcript.segment_metadata or {}
                    segment = {
                        "id": str(transcript.id),
                        "speaker": metadata.get("speaker", "Unknown"),
                        "content": transcript.text_content,
                        "segment_id": transcript.original_segment_id,
                        "html_content": None,  # Placeholder for future implementation
                        "timestamp": transcript.start_timestamp.isoformat()
                    }
                    segments.append(segment)
                
                return segments
                
            except Exception as e:
                logger.error(f"Error getting transcript segments: {str(e)}")
                raise
    
    async def update_transcript_access(
        self,
        transcript_id: UUID,
        user_id: UUID,
        target_user_id: UUID,
        access_level: str,
        session: AsyncSession = None
    ) -> bool:
        async with (session or get_session()) as session:
            try:
                # Check if transcript exists and user has owner access
                q = await session.execute(
                    select(TranscriptAccess).where(and_(
                        TranscriptAccess.transcript_id == transcript_id,
                        TranscriptAccess.user_id == user_id,
                        TranscriptAccess.access_level == AccessLevel.OWNER.value
                    ))
                )
                owner_access = q.scalar_one_or_none()
                if not owner_access:
                    # Check if transcript exists at all
                    q_exist = await session.execute(select(Transcript).where(Transcript.id == transcript_id))
                    transcript_exist = q_exist.scalar_one_or_none()
                    if transcript_exist:
                        raise ValueError("Access denied")
                    else:
                        raise ValueError("Transcript not found")
                
                # Check for existing access and update or create
                q_existing = await session.execute(
                    select(TranscriptAccess).where(and_(
                        TranscriptAccess.transcript_id == transcript_id,
                        TranscriptAccess.user_id == target_user_id
                    ))
                )
                existing_access = q_existing.scalar_one_or_none()
                
                if existing_access:
                    existing_access.access_level = access_level
                else:
                    new_access = TranscriptAccess(
                        transcript_id=transcript_id,
                        user_id=target_user_id,
                        access_level=access_level,
                        granted_by=user_id
                    )
                    session.add(new_access)
                
                await session.commit()
                return True
            except Exception as e:
                await session.rollback()
                logger.warning(f"{str(e)}")
                raise 