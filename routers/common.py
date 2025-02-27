import logging
from fastapi import HTTPException, Header, Depends
from token_manager import TokenManager
from psql_models import User
from sqlalchemy.ext.asyncio import AsyncSession
from psql_helpers import get_session
from sqlalchemy import select
from uuid import UUID

logger = logging.getLogger(__name__)
token_manager = TokenManager()

async def get_current_user(
    authorization: str = Header(...),
    session: AsyncSession = Depends(get_session)
):
    logger.debug("Checking authorization token")
    token = authorization.split("Bearer ")[-1]
    try:
        user_id, user_name = await token_manager.check_token(token)
        if not user_id:
            logger.warning("Invalid token provided")
            raise HTTPException(status_code=401, detail="Invalid token")
        
        # Verify user exists in database
        user_query = select(User).where(User.id == UUID(user_id))
        result = await session.execute(user_query)
        user = result.scalar_one_or_none()
        
        if not user:
            logger.error(f"User {user_id} authenticated but not found in database")
            raise HTTPException(status_code=404, detail="User not found in database")
        
        logger.debug(f"Authenticated user: {user_name} ({user_id})")
        return user_id, user_name, token
    except Exception as e:
        logger.error(f"Authentication failed: {str(e)}", exc_info=True)
        raise HTTPException(status_code=401, detail="Invalid token")

