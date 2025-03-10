from typing import Dict, Optional
import logging
import asyncio
from sqlalchemy.ext.asyncio import AsyncSession
from services.psql_helpers import get_session
import uuid
import re
from models.db import User, UserToken
from sqlalchemy import select
from fastapi import HTTPException

import os
import httpx
from pydantic import BaseModel
from httpx import AsyncClient
from pathlib import Path

# Create logs directory if it doesn't exist
log_dir = Path('logs')
log_dir.mkdir(exist_ok=True)

# Configure logging
logger = logging.getLogger(__name__)
file_handler = logging.FileHandler(log_dir / 'auth_manager.log')
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)
logger.setLevel(logging.INFO)

TRANSCRIPTION_SERVICE_API_URL = os.getenv('TRANSCRIPTION_SERVICE_API_URL')
TRANSCRIPTION_SERVICE_API_TOKEN = os.getenv('TRANSCRIPTION_SERVICE_API_TOKEN')

class TokenResponse(BaseModel):
    user_id: str
    user_name: str
    email: str
    image: Optional[str] = None

class AuthManager:
    def __init__(self):
        self.stream_queue_url = os.getenv('TRANSCRIPTION_SERVICE_API_URL')
        self.stream_queue_api_key = os.getenv('TRANSCRIPTION_SERVICE_API_TOKEN')

    @classmethod
    async def create(cls):
        return cls()
    
    def _validate_email(self, email: str) -> bool:
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, email))
    
    async def _propagate_token_to_stream(self, token: str, user_id: uuid.UUID) -> None:
        """Propagate token to stream queue service"""
        async with AsyncClient() as client:
            try:
                response = await client.post(
                    f"{self.stream_queue_url}/api/v1/users/add-token",
                    headers={
                        "Authorization": f"Bearer {self.stream_queue_api_key}"  # Add API key to headers
                    },
                    json={
                        "token": token,
                        "user_id": str(user_id),
                        "enable_status": True
                    }
                )
                response.raise_for_status()
                logger.info(f"Token propagated to stream queue: {response.json()}")
            except Exception as e:
                logger.error(f"Failed to propagate token to stream queue: {e}")
                raise

    async def _create_user_token(self, session: AsyncSession, user_id: uuid.UUID) -> str:
        token = str(uuid.uuid4())
        user_token = UserToken(token=token, user_id=user_id)
        session.add(user_token)
        await self._propagate_token_to_stream(token, user_id)
        return token


    async def default_auth(
        self,
        email: str,
        username: Optional[str] = None,
        first_name: Optional[str] = None,
        last_name: Optional[str] = None,
        utm_params: Optional[Dict[str, str]] = None,
        session: AsyncSession = None
    ) -> Dict:
        if not self._validate_email(email):
            raise ValueError("Invalid email format")

        async with (session or get_session()) as session:
            # Check if user exists
            query = select(User).where(User.email == email)
            existing_user = (await session.execute(query)).scalar_one_or_none()
            
            if existing_user:
                user = existing_user
                token = await self._create_user_token(session, user.id)
            else:
                # Create new user
                user = User(
                    email=email,
                    username=username,
                    first_name=first_name,
                    last_name=last_name
                )
                session.add(user)
                await session.flush()  # Get the user.id
                
                # Create token
                token = await self._create_user_token(session, user.id)

            await session.commit()
            try:
                await self._propagate_token_to_stream(token, user.id)
            except Exception as e:
                logger.error(f"Token validation propagation error: {e}")
            
            
            return {
                "user_id": str(user.id),
                "token": token,
                "email": user.email,
                "username": user.username
            }


    async def submit_token(self, token: str, session: AsyncSession = None) -> TokenResponse:
        async with (session or get_session()) as session:
            try:
                query = select(UserToken, User).join(User).where(UserToken.token == token)
                result = await session.execute(query)
                token_user = result.first()
                
                if not token_user:
                    raise HTTPException(status_code=401, detail="Invalid token")
                    
                _, user = token_user
                
                try:
                    await self._propagate_token_to_stream(token, user.id)
                except Exception as e:
                    logger.error(f"Token validation propagation error: {e}")
                
                return TokenResponse(
                    user_id=str(user.id),
                    user_name=user.username or user.email,
                    email=user.email,
                    image=user.image
                )
            except HTTPException:
                raise
            except Exception as e:
                logger.error(f"Token validation failed: {str(e)}", exc_info=True)
                raise HTTPException(status_code=500, detail="Internal server error") 