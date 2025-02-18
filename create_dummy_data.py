from datetime import datetime, timedelta
import asyncio
import uuid
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from psql_models import User, Meeting, UserMeeting, Speaker, meeting_speaker_association, AccessLevel, UserToken
from psql_helpers import get_session
from pydantic_models import DummyTranscript, DummyTranscriptEntry
from core import system_msg, user_msg
from prompts import Prompts
from typing import List
import re
from pydantic import BaseModel

class MeetingInput(BaseModel):
    date: datetime
    title: str
    attendees: List[str]
    summary: str

def parse_meeting_input(input_text: str) -> MeetingInput:
    # Extract date
    date_match = re.search(r'Date: (.+?)(?:\n|$)', input_text)
    date = datetime.strptime(date_match.group(1).strip(), '%B %d, %Y') if date_match else None
    
    # Extract title (first line without numbering)
    title = input_text.split('\n')[0].strip()
    title = re.sub(r'^\d+\.\s*', '', title)
    
    # Extract attendees
    attendees = []
    in_attendees = False
    for line in input_text.split('\n'):
        if 'Attendees:' in line:
            in_attendees = True
            continue
        if in_attendees and line.strip() and '–' in line:
            name = line.split('–')[0].strip()
            attendees.append(name)
        if in_attendees and ('Summary:' in line or not line.strip()):
            break
    
    # Extract summary
    summary = ''
    in_summary = False
    for line in input_text.split('\n'):
        if 'Summary:' in line:
            in_summary = True
            continue
        if in_summary and line.strip() and 'Action Items:' not in line:
            summary += line.strip() + ' '
        if 'Action Items:' in line:
            break
    
    return MeetingInput(
        date=date,
        title=title,
        attendees=attendees,
        summary=summary.strip()
    )

async def create_user_with_token(session: AsyncSession) -> tuple[User, str]:
    user_query = select(User).where(User.email == "maya_patel@example.com")
    existing_user = await session.execute(user_query)
    user = existing_user.scalar_one_or_none()
    
    if not user:
        user = User(
            id=uuid.uuid4(),
            email="maya_patel@example.com",
            username="Maya Patel",
            first_name="Maya",
            last_name="Patel",
            created_timestamp=datetime.utcnow(),
            updated_timestamp=datetime.utcnow(),
            is_indexed=True
        )
        session.add(user)
        await session.flush()
    
    token_query = select(UserToken).where(UserToken.user_id == user.id)
    existing_token = await session.execute(token_query)
    token_record = existing_token.scalar_one_or_none()
    
    if not token_record:
        token = f"{uuid.uuid4()}"
        token_record = UserToken(
            token=token,
            user_id=user.id,
            created_at=datetime.utcnow()
        )
        session.add(token_record)
        await session.flush()
    
    return user, token_record.token

async def create_dummy_data():
    prompts = Prompts()
    inputs = [inp.strip() for inp in prompts.dummy_input_prompts.split('|||') if inp.strip()]
    
    async with get_session() as session:
        try:
            user, token = await create_user_with_token(session)
            await session.commit()
            
            for input_text in inputs:
                # Generate transcript with structured output
                inp = [
                    system_msg(prompts.create_dummy_trasncript),
                    user_msg(input_text)
                ]
                transcript = await DummyTranscript.call(inp)
                
                # Create meeting using inferred data
                meeting = Meeting(
                    timestamp=transcript.meeting_date,
                    meeting_name=None,
                    meeting_summary=None,
                    transcript=str([{
                        'speaker': entry.speaker,
                        'timestamp': entry.timestamp.isoformat(),
                        'content': entry.content
                    } for entry in transcript.entries]),
                    is_indexed=True,
                    last_update=datetime.utcnow()
                )
                session.add(meeting)
                await session.flush()
                
                # Create user meeting access
                user_meeting = UserMeeting(
                    meeting_id=meeting.meeting_id,
                    user_id=user.id,
                    access_level=AccessLevel.OWNER.value,
                    is_owner=True,
                    created_by=user.id
                )
                session.add(user_meeting)
                
                # Create speakers using inferred speakers list
                for speaker_name in transcript.speakers:
                    speaker = await session.scalar(select(Speaker).where(Speaker.name == speaker_name))
                    if not speaker:
                        speaker = Speaker(name=speaker_name)
                        session.add(speaker)
                        await session.flush()
                    
                    await session.execute(
                        meeting_speaker_association.insert().values(
                            meeting_id=meeting.id,
                            speaker_id=speaker.id
                        )
                    )
                
                await session.commit()
            
            return user.id, token
            
        except Exception as e:
            await session.rollback()
            raise Exception(f"Failed to create dummy data: {str(e)}")

if __name__ == "__main__":
    user_id, token = asyncio.run(create_dummy_data())
    print(f"Created user ID: {user_id}")
    print(f"Access token: {token}")