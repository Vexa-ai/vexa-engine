from typing import Dict, Optional
from vexa import VexaAuth
import logging
import asyncio
from sqlalchemy.ext.asyncio import AsyncSession
from psql_helpers import get_session

logger = logging.getLogger(__name__)

class AuthManager:
    def __init__(self):
        self.vexa_auth = VexaAuth()
    
    @classmethod
    async def create(cls):
        return cls()
    
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
                        result = await self.vexa_auth.google_auth(
                            token=token,
                            utm_params=utm_params or {}
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