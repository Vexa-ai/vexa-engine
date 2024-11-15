from google.auth.exceptions import GoogleAuthError
from google.auth.transport import requests
from google.oauth2 import id_token
from pydantic import BaseModel
import logging
from typing import Optional
from starlette.concurrency import run_in_threadpool

logger = logging.getLogger(__name__)

class UserInfo(BaseModel):
    sub: str
    email: str
    email_verified: bool
    name: Optional[str] = None
    given_name: Optional[str] = None
    family_name: Optional[str] = None
    picture: Optional[str] = None
    locale: Optional[str] = None

class GoogleError(Exception):
    pass

class GoogleClient:
    def __init__(self, client_id: str = "733104961366-fre6q88nt37qk26nfnemrpquvh8in4k7.apps.googleusercontent.com"):
        self.client_id = client_id
        
    async def get_user_info(self, token: str) -> UserInfo:
        try:
            # Verify the token in threadpool since google auth is blocking
            id_info = await run_in_threadpool(
                id_token.verify_oauth2_token,
                token, 
                requests.Request(), 
                self.client_id
            )
            return UserInfo(**id_info)
            
        except GoogleAuthError as e:
            logger.error(f"Google auth error: {str(e)}")
            raise GoogleError(f"Failed to verify Google token: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error during Google auth: {str(e)}")
            raise GoogleError(f"Unexpected error during Google auth: {str(e)}") 