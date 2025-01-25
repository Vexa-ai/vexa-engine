from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, EmailStr
from typing import Optional
import asyncio
import logging
from app.core.logger import logger
from vexa import VexaAuth
from app.services.content.access import get_token_by_email

router = APIRouter(prefix="/auth", tags=["auth"])

class GoogleAuthRequest(BaseModel):
    utm_source: Optional[str] = None
    utm_medium: Optional[str] = None
    utm_campaign: Optional[str] = None
    utm_term: Optional[str] = None
    utm_content: Optional[str] = None
    ref: Optional[str] = None
    token: str

@router.post("/google")
async def google_auth(request: GoogleAuthRequest):
    try:
        vexa_auth = VexaAuth()
        params = {k: v for k, v in {
            "utm_source": request.utm_source,
            "utm_medium": request.utm_medium,
            "utm_campaign": request.utm_campaign,
            "utm_term": request.utm_term,
            "utm_content": request.utm_content,
            "ref": request.ref
        }.items() if v is not None}
        
        # Retry logic for token timing issues
        max_retries = 3
        base_delay = 1
        
        for attempt in range(max_retries):
            try:
                await asyncio.sleep(base_delay * (attempt + 1))
                result = await vexa_auth.google_auth(
                    token=request.token,
                    utm_params=params
                )
                return result
            except Exception as e:
                if "Token used too early" in str(e) and attempt < max_retries - 1:
                    continue
                raise
                
    except Exception as e:
        logger.error(f"Google auth failed: {str(e)}", exc_info=True)
        if "Token used too early" in str(e):
            raise HTTPException(
                status_code=400,
                detail="Authentication timing error. Please try again."
            )
        raise HTTPException(status_code=500, detail=str(e))

class TokenRequest(BaseModel):
    email: EmailStr

@router.post("/token")
async def get_token(request: TokenRequest):
    try:
        result = await get_token_by_email(request.email)
        if not result:
            raise HTTPException(status_code=404, detail="User not found")
        token, user_data = result
        return {"token": token, "user": user_data}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Token generation failed: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
