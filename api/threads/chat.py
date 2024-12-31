from fastapi import APIRouter, Depends, HTTPException, Header
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from datetime import datetime
from uuid import UUID
import logging

from chat import UnifiedChatManager
from thread_manager import ThreadManager
from token_manager import TokenManager
from psql_helpers import get_session

# Set up logging
logger = logging.getLogger(__name__)

router = APIRouter(prefix="/chat", tags=["chat"])
token_manager = TokenManager()
thread_manager = ThreadManager()

class MessageEdit(BaseModel):
    message_index: int
    new_content: str

async def get_current_user(authorization: str = Header(...)):
    token = authorization.split("Bearer ")[-1]
    try:
        user_id, user_name = await token_manager.check_token(token)
        if not user_id:
            raise HTTPException(status_code=401, detail="Invalid token")
        return user_id, user_name, token
    except Exception as e:
        raise HTTPException(status_code=401, detail="Invalid token")

@router.put("/messages/{thread_id}")
async def edit_message(
    thread_id: str,
    edit: MessageEdit,
    current_user: tuple = Depends(get_current_user)
):
    """Edit a message in a thread and continue the conversation from that point.
    
    Args:
        thread_id: The ID of the thread containing the message
        edit: The edit details including message index and new content
        current_user: The current authenticated user
    """
    user_id, user_name, token = current_user
    logger.info(f"Editing message in thread {thread_id} for user {user_id}")
    logger.debug(f"Edit details: {edit.dict()}")
    
    try:
        # Verify thread ownership
        thread = await thread_manager.get_thread(thread_id)
        if not thread:
            raise HTTPException(status_code=404, detail="Thread not found")
        if str(thread.user_id) != user_id:
            raise HTTPException(status_code=403, detail="Not thread owner")
            
        # Edit message
        updated_messages = await thread_manager.edit_message(
            thread_id=thread_id,
            message_index=edit.message_index,
            new_content=edit.new_content
        )
        if not updated_messages:
            raise HTTPException(status_code=400, detail="Failed to edit message")
            
        # Return updated thread state
        return {
            "thread_id": thread_id,
            "messages": [msg.dict() for msg in updated_messages]
        }
            
    except HTTPException as http_error:
        logger.error(f"HTTP error in message edit: {str(http_error)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in message edit: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) 