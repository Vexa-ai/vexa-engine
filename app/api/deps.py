from typing import Optional
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from contextlib import asynccontextmanager

from app.models.psql_models import User, UserToken
from app.utils.db import async_session

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

async def get_db():
    async with async_session() as session:
        try:
            yield session
            await session.commit()
        except:
            await session.rollback()
            raise

async def get_current_user(
    token: str = Depends(oauth2_scheme),
    session: AsyncSession = Depends(get_db)
) -> tuple[str, str, str]:
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    
    # Get user token from database
    query = select(UserToken).where(UserToken.token == token)
    result = await session.execute(query)
    user_token = result.scalar_one_or_none()
    
    if not user_token:
        raise credentials_exception
        
    # Get user from database
    query = select(User).where(User.id == user_token.user_id)
    result = await session.execute(query)
    user = result.scalar_one_or_none()
    
    if not user:
        raise credentials_exception
        
    return str(user.id), user.email, user.username
