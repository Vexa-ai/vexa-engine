import logging
from fastapi import HTTPException, Header
from token_manager import TokenManager

logger = logging.getLogger(__name__)
token_manager = TokenManager()

async def get_current_user(authorization: str = Header(...)):
    logger.debug("Checking authorization token")
    token = authorization.split("Bearer ")[-1]
    try:
        user_id, user_name = await token_manager.check_token(token)
        if not user_id:
            logger.warning("Invalid token provided")
            raise HTTPException(status_code=401, detail="Invalid token")
        logger.debug(f"Authenticated user: {user_name} ({user_id})")
        return user_id, user_name, token
    except Exception as e:
        logger.error(f"Authentication failed: {str(e)}", exc_info=True)
        raise HTTPException(status_code=401, detail="Invalid token")

