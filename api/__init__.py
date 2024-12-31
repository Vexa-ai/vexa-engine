from fastapi import APIRouter
from .threads import chat_router

api_router = APIRouter()
api_router.include_router(chat_router)

__all__ = ['api_router'] 