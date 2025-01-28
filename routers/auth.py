from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, Dict
from auth_manager import AuthManager
from token_manager import TokenManager
import logging

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/auth", tags=["auth"])

class GoogleAuthRequest(BaseModel):
    utm_source: Optional[str] = None
    utm_medium: Optional[str] = None
    utm_campaign: Optional[str] = None
    utm_term: Optional[str] = None
    utm_content: Optional[str] = None
    ref: Optional[str] = None
    token: str

class SubmitTokenRequest(BaseModel):
    token: str

class TokenResponse(BaseModel):
    user_id: str
    user_name: str
    image: Optional[str] = None

@router.post("/google", response_model=dict)
async def google_auth(request: GoogleAuthRequest):
    try:
        auth_manager = await AuthManager.create()
        params = {k: v for k, v in {
            "utm_source": request.utm_source,
            "utm_medium": request.utm_medium,
            "utm_campaign": request.utm_campaign,
            "utm_term": request.utm_term,
            "utm_content": request.utm_content,
            "ref": request.ref
        }.items() if v is not None}
        
        result = await auth_manager.google_auth(
            token=request.token,
            utm_params=params
        )
        return result
                
    except ValueError as e:
        if "Authentication timing error" in str(e):
            raise HTTPException(status_code=400, detail=str(e))
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/submit_token", response_model=TokenResponse)
async def submit_token(request: SubmitTokenRequest):
    token_manager = TokenManager()
    user_id, user_name, image = await token_manager.submit_token(request.token)
    
    if not user_id or not user_name:
        raise HTTPException(status_code=401, detail="Invalid token")
        
    return TokenResponse(
        user_id=user_id,
        user_name=user_name,
        image=image
    )