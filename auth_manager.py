from typing import Dict, Optional
import logging
import asyncio
from sqlalchemy.ext.asyncio import AsyncSession
from psql_helpers import get_session
import uuid
import re
from psql_models import User, UserToken, UTMParams
from sqlalchemy import select
from fastapi import HTTPException
from google.oauth2 import id_token
from google.auth.transport import requests
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

STREAMQUEUE_URL = os.getenv('STREAMQUEUE_URL')
STREAMQUEUE_API_KEY = os.getenv('STREAMQUEUE_API_KEY')

class TokenResponse(BaseModel):
    user_id: str
    user_name: str
    email: str
    image: Optional[str] = None

class AuthManager:
    def __init__(self):
        self.stream_queue_url = os.getenv('STREAMQUEUE_URL')
        self.stream_queue_api_key = os.getenv('STREAMQUEUE_API_KEY')

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

    async def _store_utm_params(self, session: AsyncSession, user_id: uuid.UUID, utm_params: Dict[str, str]):
        if not utm_params:
            return
        utm = UTMParams(
            user_id=user_id,
            utm_source=utm_params.get('utm_source'),
            utm_medium=utm_params.get('utm_medium'),
            utm_campaign=utm_params.get('utm_campaign'),
            utm_term=utm_params.get('utm_term'),
            utm_content=utm_params.get('utm_content'),
            ref=utm_params.get('ref')
        )
        session.add(utm)

    async def _verify_google_token(self, token: str) -> Dict:
        try:
            # Specify the CLIENT_ID of your app that was created in Google Developer Console
            idinfo = id_token.verify_oauth2_token(token, requests.Request())
            
            if idinfo['iss'] not in ['accounts.google.com', 'https://accounts.google.com']:
                raise ValueError('Wrong issuer.')
                
            return {
                'email': idinfo['email'],
                'name': idinfo.get('name'),
                'given_name': idinfo.get('given_name'),
                'family_name': idinfo.get('family_name'),
                'picture': idinfo.get('picture')
            }
        except ValueError as e:
            logger.error(f"Token verification failed: {str(e)}")
            raise ValueError("Invalid token")

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
                
                # Store UTM params
                await self._store_utm_params(session, user.id, utm_params or {})
            
            await session.commit()
            
            return {
                "user_id": str(user.id),
                "token": token,
                "email": user.email,
                "username": user.username
            }

    async def google_auth(
        self,
        token: str,
        utm_params: Optional[Dict[str, str]] = None,
        max_retries: int = 3,
        base_delay: int = 1,
        session: AsyncSession = None
    ) -> Dict:
        async with (session or get_session()) as session:
            try:
                for attempt in range(max_retries):
                    try:
                        await asyncio.sleep(base_delay * (attempt + 1))
                        google_info = await self._verify_google_token(token)
                        
                        # Create or get user
                        result = await self.default_auth(
                            email=google_info['email'],
                            username=google_info.get('name'),
                            first_name=google_info.get('given_name'),
                            last_name=google_info.get('family_name'),
                            utm_params=utm_params,
                            session=session
                        )
                        return result
                        
                    except Exception as e:
                        if "Token used too early" in str(e) and attempt < max_retries - 1:
                            continue
                        raise
            except Exception as e:
                logger.error(f"Google auth failed: {str(e)}", exc_info=True)
                if "Token used too early" in str(e):
                    raise ValueError("Authentication timing error. Please try again.")
                raise ValueError(str(e))

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