from datetime import datetime, timezone, timedelta
from sqlalchemy import select, and_
from psql_models import User, Meeting, UserMeeting, Speaker, meeting_speaker_association
from psql_helper import get_session
from vexa import VexaAuth, VexaAPI

class MeetingsMonitor:
    def __init__(self):
        self.vexa_auth = VexaAuth()
        
    async def _get_meeting_data(self, meeting_id: str, user_id: str) -> tuple:
        token = await self.vexa_auth.get_user_token(user_id=user_id)
        if not token:
            return None, None, None
        
        vexa = VexaAPI(token=token)
        try:
            df, _, _, speakers, transcript = await vexa.get_transcription(meeting_id=meeting_id)
            transcript_text = '\n'.join([f"{row['speaker']}: {row['content']}" for _, row in df.iterrows()])
            return transcript_text, speakers, transcript
        except Exception as e:
            print(f"Error fetching transcript for meeting {meeting_id}: {str(e)}")
            return None, None, None

    async def _ensure_speakers(self, speakers: list, session) -> dict:
        speaker_map = {}
        for speaker_name in speakers:
            if not speaker_name:  # Skip empty speaker names
                continue
                
            stmt = select(Speaker).where(Speaker.name == speaker_name)
            speaker = await session.scalar(stmt)
            
            if not speaker:
                speaker = Speaker(name=speaker_name)
                session.add(speaker)
                await session.flush()
            
            speaker_map[speaker_name] = speaker
        
        return speaker_map

    # ... (keep existing _ensure_user_exists and _parse_timestamp methods) ...

    async def upsert_meetings(self, meetings_data: list, active_seconds: int = 300):
        async with get_session() as session:
            cutoff = datetime.utcnow() - timedelta(seconds=active_seconds)
            
            for meeting_id, user_id, timestamp in meetings_data:
                # Ensure user exists
                await self._ensure_user_exists(user_id, session)
                
                # Parse timestamp and get meeting data
                meeting_time = self._parse_timestamp(timestamp)
                meeting_name = f"call_{timestamp}"
                
                # Fetch transcript and speakers
                transcript_text, speakers, raw_transcript = await self._get_meeting_data(meeting_id, user_id)
                
                # Check if meeting exists
                stmt = select(Meeting).where(Meeting.meeting_id == meeting_id)
                meeting = await session.scalar(stmt)
                
                if meeting_time > cutoff:
                    # Active meeting - update or create
                    if not meeting:
                        meeting = Meeting(
                            meeting_id=meeting_id,
                            timestamp=meeting_time,
                            meeting_name=meeting_name,
                            transcript=transcript_text if transcript_text else None
                        )
                        session.add(meeting)
                    else:
                        meeting.timestamp = meeting_time
                        meeting.meeting_name = meeting_name
                        if transcript_text:
                            meeting.transcript = transcript_text
                else:
                    # Inactive meeting - only create if doesn't exist
                    if not meeting:
                        meeting = Meeting(
                            meeting_id=meeting_id,
                            timestamp=meeting_time,
                            meeting_name=meeting_name,
                            transcript=transcript_text if transcript_text else None
                        )
                        session.add(meeting)
                
                # Handle speakers if we have them
                if speakers:
                    speaker_objects = await self._ensure_speakers(speakers, session)
                    # Clear existing speaker associations
                    await session.execute(
                        meeting_speaker_association.delete().where(
                            meeting_speaker_association.c.meeting_id == meeting.id
                        )
                    )
                    # Add new speaker associations
                    meeting.speakers = list(speaker_objects.values())
                
                # Handle UserMeeting association
                stmt = select(UserMeeting).where(
                    and_(
                        UserMeeting.meeting_id == meeting_id,
                        UserMeeting.user_id == user_id
                    )
                )
                user_meeting = await session.scalar(stmt)
                
                if not user_meeting:
                    user_meeting = UserMeeting(
                        meeting_id=meeting_id,
                        user_id=user_id,
                        created_at=meeting_time,
                        created_by=user_id,
                        is_owner=True,
                        access_level='search'
                    )
                    session.add(user_meeting)
            
            await session.commit() 