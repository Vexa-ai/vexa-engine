import logging
from fastapi import HTTPException, Header, Depends
from services.auth import AuthManager, TokenResponse
from models.db import User
from sqlalchemy.ext.asyncio import AsyncSession
from services.psql_helpers import get_session
from sqlalchemy import select
from uuid import UUID
    
logger = logging.getLogger(__name__)
auth_manager = None

async def get_auth_manager():
    global auth_manager
    if auth_manager is None:
        auth_manager = await AuthManager.create()
    return auth_manager

async def get_current_user(
    authorization: str = Header(...),
    session: AsyncSession = Depends(get_session)
):
    logger.debug("Checking authorization token")
    token = authorization.split("Bearer ")[-1]
    try:
        auth_mgr = await get_auth_manager()
        token_response = await auth_mgr.submit_token(token, session)
        user_id = token_response.user_id
        user_name = token_response.user_name
        
        # Verify user exists in database
        user_query = select(User).where(User.id == UUID(user_id))
        result = await session.execute(user_query)
        user = result.scalar_one_or_none()
        
        if not user:
            logger.error(f"User {user_id} authenticated but not found in database")
            raise HTTPException(status_code=404, detail="User not found in database")
        
        logger.debug(f"Authenticated user: {user_name} ({user_id})")
        return user
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Authentication error: {str(e)}")
        raise HTTPException(status_code=401, detail="Authentication failed")

