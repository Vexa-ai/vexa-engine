import uuid
from typing import List, Optional, Tuple
from pydantic import BaseModel, Field
from vexa import VexaAPI
from psql_models import User, UserToken, async_session
from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.concurrency import run_in_threadpool
from datetime import datetime
from sqlalchemy.exc import SQLAlchemyError

class TokenData(BaseModel):
    token: str = Field(default_factory=lambda: str(uuid.uuid4()))
    user_id: uuid.UUID
    user_name: str

class TokenManager:
    def __init__(self):
        self.session_factory = async_session

    async def submit_token(self, token: str) -> Tuple[str | None, str | None]:
        vexa = VexaAPI(token=token)
        await vexa.get_user_info()
        
        if vexa.user_id is None or vexa.user_name is None:
            return None, None

        async with self.session_factory() as session:
            # Check if user exists
            user = await session.execute(
                select(User).filter_by(id=vexa.user_id)
            )
            user = user.scalar_one_or_none()

            if not user:
                # Create new user
                user = User(
                    id=vexa.user_id,
                    name=vexa.user_name,
                )
                session.add(user)

            # Update or create token
            token_record = await session.execute(
                select(UserToken).filter_by(token=token)
            )
            token_record = token_record.scalar_one_or_none()

            if token_record:
                token_record.last_used_at = datetime.utcnow()
            else:
                token_record = UserToken(
                    token=token,
                    user_id=vexa.user_id,
                    user_name=vexa.user_name
                )
                session.add(token_record)

            await session.commit()
            return str(vexa.user_id), vexa.user_name

    async def check_token(self, token: str) -> Tuple[str | None, str | None]:
        async with self.session_factory() as session:
            result = await session.execute(
                select(UserToken).filter_by(token=token)
            )
            token_record = result.scalar_one_or_none()
            
            if token_record:
                token_record.last_used_at = datetime.utcnow()
                await session.commit()
                return str(token_record.user_id), token_record.user_name
            
            return None, None

    async def get_user_tokens(self, user_id: uuid.UUID) -> list[str]:
        async with self.session_factory() as session:
            try:
                query = select(UserToken.token).filter(UserToken.user_id == user_id)
                result = await session.execute(query)
                return [row[0] for row in result.fetchall()]

            except SQLAlchemyError as e:
                print(f"Database error: {e}")
                return []

    async def revoke_token(self, token: str) -> bool:
        async with self.session_factory() as session:
            try:
                query = select(UserToken).filter(UserToken.token == token)
                result = await session.execute(query)
                token_obj = result.scalar_one_or_none()
                
                if token_obj:
                    await session.delete(token_obj)
                    await session.commit()
                    return True
                return False

            except SQLAlchemyError as e:
                print(f"Database error: {e}")
                return False

    async def revoke_all_user_tokens(self, user_id: uuid.UUID) -> int:
        async with self.session_factory() as session:
            try:
                query = select(UserToken).filter(UserToken.user_id == user_id)
                result = await session.execute(query)
                tokens = result.scalars().all()
                
                revoked_count = 0
                for token in tokens:
                    await session.delete(token)
                    revoked_count += 1
                
                await session.commit()
                return revoked_count

            except SQLAlchemyError as e:
                print(f"Database error: {e}")
                return 0
