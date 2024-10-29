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

    async def submit_token(self, token: str) -> Tuple[Optional[str], Optional[str]]:
        try:
            # First check if token already exists and is valid
            async with async_session() as session:
                result = await session.execute(
                    select(UserToken).filter_by(token=token)
                )
                existing_token = result.scalar_one_or_none()
                
                if existing_token:
                    # Update last_used_at and return existing user info
                    existing_token.last_used_at = datetime.utcnow()
                    await session.commit()
                    return str(existing_token.user_id), existing_token.user_name

            # If token doesn't exist, validate with Vexa API and create new record
            vexa_api = VexaAPI(token=token)
            user_info = await vexa_api.get_user_info()
            
            async with async_session() as session:
                # Create user if doesn't exist
                user = User(
                    id=user_info["id"],
                    email=user_info["email"],
                    username=user_info.get("username"),
                    first_name=user_info.get("first_name"),
                    last_name=user_info.get("last_name"),
                    image=user_info.get("image")
                )
                
                # Merge user and await the operation
                merged_user = await session.merge(user)
                await session.flush()
                
                # Create new token record
                user_token = UserToken(
                    token=token,
                    user_id=merged_user.id,
                    user_name=f"{user_info.get('first_name', '')} {user_info.get('last_name', '')}".strip(),
                    last_used_at=datetime.utcnow()
                )
                session.add(user_token)
                await session.commit()
                
                return str(merged_user.id), user_token.user_name
                
        except Exception as e:
            print(f"Error in submit_token: {e}")
            return None, None

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
