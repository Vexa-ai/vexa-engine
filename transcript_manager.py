from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
from uuid import UUID
from sqlalchemy import select, and_, func
from sqlalchemy.ext.asyncio import AsyncSession
from psql_models import Content, Transcript, TranscriptAccess, AccessLevel
from psql_helpers import get_session
import logging

logger = logging.getLogger(__name__)

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
        required_fields = ['content', 'start_timestamp', 'confidence', 
                         'segment_id', 'words', 'speaker']
        
        # Check required fields
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
    
    async def ingest_transcript_segments(
        self,
        content_id: UUID,
        segments: List[Dict[str, Any]],
        user_id: UUID,
        session: AsyncSession = None
    ) -> List[Transcript]:
        async with (session or get_session()) as session:
            try:
                # Verify content exists
                content = await session.execute(
                    select(Content).where(Content.id == content_id)
                )
                if not content.scalar_one_or_none():
                    raise ValueError(f"Content not found: {content_id}")
                
                # Validate and normalize all segments first
                normalized_segments = []
                for segment in segments:
                    self._validate_segment(segment)
                    normalized = self._normalize_segment(segment)
                    normalized_segments.append(normalized)
                
                # Create transcript records
                transcripts = []
                for segment in normalized_segments:
                    transcript = Transcript(
                        content_id=content_id,
                        text_content=segment['text_content'],
                        start_timestamp=segment['start_timestamp'],
                        end_timestamp=segment['end_timestamp'],
                        confidence=segment['confidence'],
                        original_segment_id=segment['original_segment_id'],
                        word_timing_data=segment['word_timing_data'],
                        segment_metadata=segment['segment_metadata']
                    )
                    transcripts.append(transcript)
                    
                    # Create owner access
                    transcript_access = TranscriptAccess(
                        transcript=transcript,
                        user_id=user_id,
                        access_level=AccessLevel.OWNER,
                        granted_by=user_id
                    )
                    session.add(transcript_access)
                
                session.add_all(transcripts)
                await session.commit()
                
                return transcripts
                
            except Exception as e:
                await session.rollback()
                logger.error(f"Error ingesting transcript segments: {str(e)}")
                raise
    
    async def get_transcript_segments(
        self,
        content_id: UUID,
        user_id: UUID,
        session: AsyncSession = None
    ) -> List[Dict[str, Any]]:
        async with (session or get_session()) as session:
            try:
                # Get transcripts with access check
                query = (
                    select(Transcript)
                    .join(TranscriptAccess)
                    .where(and_(
                        Transcript.content_id == content_id,
                        TranscriptAccess.user_id == user_id,
                        TranscriptAccess.access_level != AccessLevel.REMOVED
                    ))
                    .order_by(Transcript.start_timestamp)
                )
                
                result = await session.execute(query)
                transcripts = result.scalars().all()
                
                # Format transcripts as segments
                segments = []
                for transcript in transcripts:
                    segment = {
                        'content': transcript.text_content,
                        'start_timestamp': transcript.start_timestamp.isoformat(),
                        'end_timestamp': transcript.end_timestamp.isoformat(),
                        'confidence': transcript.confidence,
                        'segment_id': transcript.original_segment_id,
                        'words': transcript.word_timing_data['words'],
                        'speaker': transcript.segment_metadata['speaker'],
                        'segment_metadata': transcript.segment_metadata
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
        access_level: AccessLevel,
        session: AsyncSession = None
    ) -> bool:
        if user_id == target_user_id:
            logger.warning("Cannot modify own access level")
            return False
            
        async with (session or get_session()) as session:
            try:
                # Check if transcript exists
                transcript = await session.execute(
                    select(Transcript).where(Transcript.id == transcript_id)
                )
                if not transcript.scalar_one_or_none():
                    logger.warning(f"Transcript not found: {transcript_id}")
                    return False
                
                # Check if user has owner access
                access_check = await session.execute(
                    select(TranscriptAccess)
                    .where(and_(
                        TranscriptAccess.transcript_id == transcript_id,
                        TranscriptAccess.user_id == user_id,
                        TranscriptAccess.access_level == AccessLevel.OWNER
                    ))
                )
                if not access_check.scalar_one_or_none():
                    logger.warning(f"User {user_id} does not have owner access to transcript {transcript_id}")
                    return False
                
                # Update or create access for target user
                existing_access = await session.execute(
                    select(TranscriptAccess)
                    .where(and_(
                        TranscriptAccess.transcript_id == transcript_id,
                        TranscriptAccess.user_id == target_user_id
                    ))
                )
                existing = existing_access.scalar_one_or_none()
                
                if existing:
                    existing.access_level = access_level
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
                logger.error(f"Error updating transcript access: {str(e)}")
                raise 