from fastapi import Depends, FastAPI, status
from fastapi.responses import JSONResponse
from fastapi.requests import Request
from sqlalchemy import select, func, or_
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional
from pydantic import BaseModel
from back import models, schemas, database

app = FastAPI()

@app.get("/user/meetings/timestamps")
async def get_meeting_timestamps(user_info: schemas.UserInfo = Depends(get_current_user)):
    """Get the timestamps of the first and last meetings for the user"""
    async with get_session() as session:
        # Query to get the earliest and latest meeting timestamps
        query = select(
            func.min(models.Meeting.timestamp).label('first_meeting'),
            func.max(models.Meeting.timestamp).label('last_meeting')
        ).where(
            or_(
                models.Meeting.owner_id == user_info.user_id,
                models.Meeting.meeting_id.in_(
                    select(models.MeetingShare.meeting_id).where(
                        models.MeetingShare.user_id == user_info.user_id,
                        models.MeetingShare.access_level.in_([
                            models.AccessLevel.SEARCH.value,
                            models.AccessLevel.TRANSCRIPT.value
                        ])
                    )
                )
            )
        )

        result = await session.execute(query)
        timestamps = result.first()
 