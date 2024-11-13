from datetime import datetime, timezone, timedelta
from sqlalchemy import select, and_
from psql_models import User, Meeting, UserMeeting
from psql_helper import get_session
from vexa import VexaAuth, VexaAPI

class MeetingsMonitor:
    def __init__(self):
        self.vexa_auth = VexaAuth()
        
    async def _ensure_user_exists(self, user_id: str, session) -> None:
        # Check if user exists
        stmt = select(User).where(User.id == user_id)
        user = await session.scalar(stmt)
        
        if not user:
            # Get user token and info if user doesn't exist
            token = await self.vexa_auth.get_user_token(user_id=user_id)
            if token:
                vexa = VexaAPI(token=token)
                user_info = await vexa.get_user_info()
                
                # Create new user
                user = User(
                    id=user_id,
                    email=user_info.get('email', ''),
                    username=user_info.get('username', ''),
                    first_name=user_info.get('first_name', ''),
                    last_name=user_info.get('last_name', ''),
                    image=user_info.get('image', ''),
                    created_timestamp=datetime.now(timezone.utc),
                    updated_timestamp=datetime.now(timezone.utc)
                )
                session.add(user)
                await session.flush()

    def _parse_timestamp(self, timestamp: str) -> datetime:
        # Handle both formats: with and without microseconds
        try:
            return datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%fZ')
        except ValueError:
            return datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%SZ')

    async def upsert_meetings(self, meetings_data: list, active_seconds: int = 300):
        async with get_session() as session:
            cutoff = datetime.utcnow() - timedelta(seconds=active_seconds)
            
            for meeting_id, user_id, timestamp in meetings_data:
                # Ensure user exists before creating meeting
                await self._ensure_user_exists(user_id, session)
                
                # Parse timestamp
                meeting_time = self._parse_timestamp(timestamp)
                meeting_name = f"call_{timestamp}"
                
                # Check if meeting exists
                stmt = select(Meeting).where(Meeting.meeting_id == meeting_id)
                meeting = await session.scalar(stmt)
                
                if meeting_time > cutoff:
                    # Active meeting - update or create
                    if not meeting:
                        meeting = Meeting(
                            meeting_id=meeting_id,
                            timestamp=meeting_time,
                            meeting_name=meeting_name
                        )
                        session.add(meeting)
                    else:
                        meeting.timestamp = meeting_time
                        meeting.meeting_name = meeting_name
                else:
                    # Inactive meeting - only create if doesn't exist
                    if not meeting:
                        meeting = Meeting(
                            meeting_id=meeting_id,
                            timestamp=meeting_time,
                            meeting_name=meeting_name
                        )
                        session.add(meeting)
                
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