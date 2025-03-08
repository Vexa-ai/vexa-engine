from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Any
from uuid import UUID, uuid5, NAMESPACE_URL
from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession
from models.db import Content, Transcript, TranscriptAccess, AccessLevel, ContentType, UserContent, ContentAccess
from services.psql_helpers import get_session
import logging
import json

logger = logging.getLogger(__name__)

def datetime_to_str(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    return obj

class DateTimeJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, UUID):
            return str(obj)
        return super().default(obj)

def generate_deterministic_uuid(external_id: str, external_id_type: str) -> UUID:
    name = f"{external_id_type}:{external_id}"
    return uuid5(NAMESPACE_URL, name)

class TranscriptManager:
    def __init__(self):
        pass
    
    @classmethod
    async def create(cls):
        return cls()
    
    def _parse_timestamp(self, timestamp) -> datetime:
        if isinstance(timestamp, (int, float)):
            base_time = datetime.fromtimestamp(0, timezone.utc)
            return base_time + timedelta(seconds=timestamp)
        elif isinstance(timestamp, str):
            return datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        elif isinstance(timestamp, datetime):
            return timestamp
        raise ValueError(f"Unsupported timestamp format: {type(timestamp)}")
    
    def _validate_segment(self, segment: Dict[str, Any]) -> None:
        # Check required fields
        required_fields = ['content', 'start_timestamp', 'words']
        for field in required_fields:
            if field not in segment:
                raise ValueError(f"Missing required field: {field}")
        
        # Set defaults for optional fields
        if 'confidence' not in segment or segment['confidence'] is None:
            segment['confidence'] = 0.0
            
        # Validate content type
        if not isinstance(segment['content'], str):
            raise ValueError("Content must be a string")
            
        # Validate words structure
        if not isinstance(segment['words'], list) or not segment['words']:
            raise ValueError("Words must be a non-empty list")
            
        # Check word format
        for word in segment['words']:
            if not isinstance(word, dict):
                raise ValueError("Words must be dictionaries")
            if not all(k in word for k in ['word', 'start', 'end']):
                raise ValueError("Each word must have 'word', 'start', and 'end' keys")
    
    def _normalize_segment(self, segment: Dict[str, Any]) -> Dict[str, Any]:
        # Parse timestamps
        start_time = self._parse_timestamp(segment['start_timestamp'])
        
        # Get end_timestamp or estimate from words
        if 'end_timestamp' in segment and segment['end_timestamp'] is not None:
            end_time = self._parse_timestamp(segment['end_timestamp'])
        elif segment['words']:
            # Use last word's end time
            last_word = segment['words'][-1]
            duration = last_word.get('end', 0)
            end_time = start_time + timedelta(seconds=duration)
        else:
            # Default duration
            end_time = start_time + timedelta(seconds=5)
        
        # Build metadata
        metadata = {
            'speaker': segment.get('speaker'),
            'present_user_ids': segment.get('present_user_ids', [])
        }
        
        # Add server_timestamp if present
        if 'server_timestamp' in segment:
            server_time = self._parse_timestamp(segment['server_timestamp'])
            metadata['server_timestamp'] = datetime_to_str(server_time)
        
        # Format word timing data
        return {
            'text_content': segment['content'],
            'speaker': segment.get('speaker'),
            'start_timestamp': start_time,
            'end_timestamp': end_time,
            'confidence': segment.get('confidence', 0.0),
            'word_timing_data': {'words': segment['words']},
            'segment_metadata': metadata
        }
    
    async def _get_or_create_content(
        self,
        external_id: str,
        external_id_type: str,
        user_ids: List[UUID],
        session: AsyncSession
    ) -> Content:
        # Generate deterministic UUID
        content_id = generate_deterministic_uuid(external_id, external_id_type)
        
        # Check if content exists
        content = await session.execute(
            select(Content).where(Content.id == content_id)
        )
        content = content.scalar_one_or_none()
        
        if content:
            # Ensure all users have access
            for user_id in user_ids:
                existing_access = await session.execute(
                    select(ContentAccess).where(
                        and_(
                            ContentAccess.content_id == content.id,
                            ContentAccess.user_id == user_id
                        )
                    )
                )
                existing_access = existing_access.scalar_one_or_none()
                
                if not existing_access:
                    content_access = ContentAccess(
                        content_id=content.id,
                        user_id=user_id,
                        access_level=AccessLevel.OWNER.value,
                        granted_by=user_ids[0] if user_ids else user_id
                    )
                    session.add(content_access)
            
            return content
            
        # Create new content
        content = Content(
            id=content_id,
            type=ContentType.MEETING.value,
            text="",
            timestamp=datetime.now(timezone.utc),
            external_id=external_id,
            external_id_type=external_id_type,
            last_update=datetime.now(timezone.utc)
        )
        session.add(content)
        
        # Create access for all users
        for user_id in user_ids:
            # Create user content
            user_content = UserContent(
                content_id=content.id,
                user_id=user_id,
                access_level=AccessLevel.OWNER.value,
                created_by=user_ids[0] if user_ids else user_id,
                is_owner=True
            )
            session.add(user_content)
            
            # Create content access
            content_access = ContentAccess(
                content_id=content.id,
                user_id=user_id,
                access_level=AccessLevel.OWNER.value,
                granted_by=user_ids[0] if user_ids else user_id
            )
            session.add(content_access)
        
        await session.commit()
        return content
    
    async def ingest_transcript_segments(
        self,
        external_id: str,
        external_id_type: str,
        segments: List[Dict[str, Any]],
        session: AsyncSession = None
    ) -> List[Transcript]:
        async with (session or get_session()) as session:
            try:
                # Extract user IDs
                all_user_ids = set()
                for segment in segments:
                    self._validate_segment(segment)
                    for user_id_str in segment.get('present_user_ids', []):
                        try:
                            all_user_ids.add(UUID(user_id_str))
                        except ValueError:
                            logger.warning(f"Invalid UUID: {user_id_str}")
                
                user_ids_list = list(all_user_ids)
                
                # Get or create content
                content = await self._get_or_create_content(
                    external_id=external_id,
                    external_id_type=external_id_type,
                    user_ids=user_ids_list,
                    session=session
                )
                
                # Normalize segments
                normalized_segments = [self._normalize_segment(segment) for segment in segments]
                
                # Check for existing transcripts
                existing_q = await session.execute(select(Transcript).where(Transcript.content_id==content.id))
                existing = existing_q.scalars().all()
                
                transcripts = []
                for seg in normalized_segments:
                    # Extract segment user IDs
                    segment_user_ids = set()
                    segment_metadata = seg['segment_metadata']
                    if 'present_user_ids' in segment_metadata:
                        for user_id_str in segment_metadata['present_user_ids']:
                            try:
                                segment_user_ids.add(UUID(user_id_str))
                            except ValueError:
                                continue
                    
                    # Use all user IDs if this segment has none
                    if not segment_user_ids:
                        segment_user_ids = all_user_ids
                    
                    # Create new transcript
                    t = Transcript(
                        content_id=content.id,
                        text_content=seg['text_content'],
                        speaker=seg['speaker'],
                        start_timestamp=seg['start_timestamp'],
                        end_timestamp=seg['end_timestamp'],
                        confidence=seg['confidence'],
                        word_timing_data=seg['word_timing_data'],
                        segment_metadata=seg['segment_metadata']
                    )
                    transcripts.append(t)
                    session.add(t)
                    
                    # Create access for users
                    for user_id in segment_user_ids:
                        granter_id = next(iter(segment_user_ids)) if segment_user_ids else None
                        if not granter_id and user_ids_list:
                            granter_id = user_ids_list[0]
                        
                        if granter_id:
                            ta = TranscriptAccess(
                                transcript=t,
                                user_id=user_id,
                                access_level=AccessLevel.OWNER.value,
                                granted_by=granter_id
                            )
                            session.add(ta)
                
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
                
                # Check access
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
                
                # Build query
                query = select(Transcript).where(Transcript.content_id == content_id)
                
                if last_msg_timestamp:
                    query = query.where(Transcript.start_timestamp > last_msg_timestamp)
                
                query = query.order_by(Transcript.start_timestamp).limit(limit)
                
                # Get transcripts
                transcripts = await session.execute(query)
                transcripts = transcripts.scalars().all()
                
                # Format response
                return [{
                    "id": str(t.id),
                    "speaker": t.speaker,
                    "content": t.text_content,
                    "timestamp": t.start_timestamp.isoformat()
                } for t in transcripts]
                
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
                # Check owner access
                q = await session.execute(
                    select(TranscriptAccess).where(and_(
                        TranscriptAccess.transcript_id == transcript_id,
                        TranscriptAccess.user_id == user_id,
                        TranscriptAccess.access_level == AccessLevel.OWNER.value
                    ))
                )
                owner_access = q.scalar_one_or_none()
                if not owner_access:
                    # Check if transcript exists
                    q_exist = await session.execute(select(Transcript).where(Transcript.id == transcript_id))
                    transcript_exist = q_exist.scalar_one_or_none()
                    if transcript_exist:
                        raise ValueError("Access denied")
                    else:
                        raise ValueError("Transcript not found")
                
                # Update or create access
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