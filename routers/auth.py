from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, EmailStr
from typing import Optional, Dict
from services.auth import AuthManager
import logging
from sqlalchemy.ext.asyncio import AsyncSession
from services.psql_helpers import get_session
from sqlalchemy import select
from models.db import UserToken, User

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/auth", tags=["auth"])

class DefaultAuthRequest(BaseModel):
    email: EmailStr
    username: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    utm_source: Optional[str] = None
    utm_medium: Optional[str] = None
    utm_campaign: Optional[str] = None
    utm_term: Optional[str] = None
    utm_content: Optional[str] = None
    ref: Optional[str] = None

class GoogleAuthRequest(BaseModel):
    utm_source: Optional[str] = None
    utm_medium: Optional[str] = None
    utm_campaign: Optional[str] = None
    utm_term: Optional[str] = None
    utm_content: Optional[str] = None
    ref: Optional[str] = None
    token: str

class AuthResponse(BaseModel):
    user_id: str
    token: str
    email: str
    username: Optional[str] = None

class SubmitTokenRequest(BaseModel):
    token: str

class TokenResponse(BaseModel):
    user_id: str
    user_name: str
    email: str
    image: Optional[str] = None

@router.post("/default", response_model=AuthResponse)
async def default_auth(
    request: DefaultAuthRequest,
    session: AsyncSession = Depends(get_session)
):
    try:
        auth_manager = await AuthManager.create()
        utm_params = {k: v for k, v in {
            "utm_source": request.utm_source,
            "utm_medium": request.utm_medium,
            "utm_campaign": request.utm_campaign,
            "utm_term": request.utm_term,
            "utm_content": request.utm_content,
            "ref": request.ref
        }.items() if v is not None}
        
        result = await auth_manager.default_auth(
            email=request.email,
            username=request.username,
            first_name=request.first_name,
            last_name=request.last_name,
            utm_params=utm_params,
            session=session
        )
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Default auth failed: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

@router.post("/google", response_model=AuthResponse)
async def google_auth(
    request: GoogleAuthRequest,
    session: AsyncSession = Depends(get_session)
):
    try:
        auth_manager = await AuthManager.create()
        utm_params = {k: v for k, v in {
            "utm_source": request.utm_source,
            "utm_medium": request.utm_medium,
            "utm_campaign": request.utm_campaign,
            "utm_term": request.utm_term,
            "utm_content": request.utm_content,
            "ref": request.ref
        }.items() if v is not None}
        
        result = await auth_manager.google_auth(
            token=request.token,
            utm_params=utm_params,
            session=session
        )
        return result
                
    except ValueError as e:
        if "Authentication timing error" in str(e):
            raise HTTPException(status_code=400, detail=str(e))
        raise HTTPException(status_code=500, detail=str(e))
    

@router.post("/submit_token", response_model=TokenResponse)
async def submit_token(request: SubmitTokenRequest):
    auth_manager = await AuthManager.create()
    return await auth_manager.submit_token(request.token)