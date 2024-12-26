from fastapi import APIRouter, Depends, HTTPException, Header
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from datetime import datetime
from uuid import UUID
import logging

from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from psql_models import Content, UserContent, ContentType, AccessLevel
from psql_helpers import async_session, get_session
from psql_sharing import has_content_access
from psql_notes import create_note, get_notes_by_user, update_note, delete_note
from token_manager import TokenManager

# Set up logging
logger = logging.getLogger(__name__)

token_manager = TokenManager()

router = APIRouter(prefix="/notes", tags=["notes"])

class NoteCreate(BaseModel):
    text: str
    parent_id: Optional[UUID] = None

class NoteUpdate(BaseModel):
    text: str

class NoteResponse(BaseModel):
    id: UUID
    text: str
    parent_id: Optional[UUID]
    timestamp: datetime
    is_owner: bool
    access_level: str

async def get_current_user(authorization: str = Header(...)):
    token = authorization.split("Bearer ")[-1]
    try:
        user_id, user_name = await token_manager.check_token(token)
        if not user_id:
            raise HTTPException(status_code=401, detail="Invalid token")
        return user_id, user_name
    except Exception as e:
        raise HTTPException(status_code=401, detail="Invalid token")

@router.post("", response_model=NoteResponse)
async def create_new_note(
    note: NoteCreate,
    current_user: tuple = Depends(get_current_user)
):
    user_id, user_name = current_user
    logger.info(f"Creating new note for user {user_id} ({user_name})")
    logger.debug(f"Note data: {note.dict()}")
    
    try:
        async with async_session() as session:
            # If parent_id is provided, verify access
            if note.parent_id:
                logger.debug(f"Verifying access to parent content: {note.parent_id}")
                has_access = await has_content_access(session, user_id, note.parent_id)
                if not has_access:
                    logger.warning(f"User {user_id} attempted to access unauthorized parent content {note.parent_id}")
                    raise HTTPException(status_code=403, detail="No access to parent content")
                logger.debug("Parent content access verified")
            
            # Create the note
            logger.debug("Creating note in database")
            try:
                created_note = await create_note(
                    session=session,
                    user_id=user_id,
                    text=note.text,
                    parent_id=note.parent_id
                )
                logger.info(f"Note created successfully with ID: {created_note.id}")
            except Exception as db_error:
                logger.error(f"Database error while creating note: {str(db_error)}", exc_info=True)
                raise
            
            response = NoteResponse(
                id=created_note.id,
                text=created_note.text,
                parent_id=created_note.parent_id,
                timestamp=created_note.timestamp,
                is_owner=True,
                access_level=AccessLevel.OWNER.value
            )
            logger.debug(f"Returning response: {response.dict()}")
            return response
            
    except HTTPException as http_error:
        logger.error(f"HTTP error in note creation: {str(http_error)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in note creation: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("", response_model=List[NoteResponse])
async def get_notes(
    parent_id: Optional[UUID] = None,
    current_user: tuple = Depends(get_current_user)
):
    user_id, user_name = current_user
    logger.info(f"Fetching notes for user {user_id} ({user_name})")
    logger.debug(f"Request parameters: parent_id={parent_id}")
    
    try:
        async with get_session() as session:
            logger.debug("Starting database query")
            notes = await get_notes_by_user(
                session=session,
                user_id=user_id,
                parent_id=parent_id
            )
            logger.debug(f"Found {len(notes)} notes")
            
            response = [
                NoteResponse(
                    id=UUID(note["id"]),
                    text=note["text"],
                    parent_id=UUID(note["parent_id"]) if note["parent_id"] else None,
                    timestamp=note["timestamp"],
                    is_owner=note["is_owner"],
                    access_level=note["access_level"]
                )
                for note in notes
            ]
            logger.debug(f"Returning {len(response)} notes")
            return response
            
    except Exception as e:
        logger.error(f"Error fetching notes: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/{note_id}", response_model=NoteResponse)
async def update_existing_note(
    note_id: UUID,
    note: NoteUpdate,
    current_user: tuple = Depends(get_current_user),
    session: AsyncSession = Depends(get_session)
):
    user_id, _ = current_user
    
    try:
        # Verify access to note
        if not await has_content_access(session, user_id, note_id):
            raise HTTPException(status_code=403, detail="No access to note")
        
        # Update the note
        updated_note = await update_note(
            session=session,
            note_id=note_id,
            user_id=user_id,
            text=note.text
        )
        
        if not updated_note:
            raise HTTPException(status_code=404, detail="Note not found")
        
        return NoteResponse(
            id=updated_note.id,
            text=updated_note.text,
            parent_id=updated_note.parent_id,
            timestamp=updated_note.timestamp,
            is_owner=updated_note.is_owner,
            access_level=updated_note.access_level
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/{note_id}")
async def delete_existing_note(
    note_id: UUID,
    current_user: tuple = Depends(get_current_user),
    session: AsyncSession = Depends(get_session)
):
    user_id, _ = current_user
    
    try:
        # Verify access to note
        if not await has_content_access(session, user_id, note_id):
            raise HTTPException(status_code=403, detail="No access to note")
        
        # Delete the note
        success = await delete_note(
            session=session,
            note_id=note_id,
            user_id=user_id
        )
        
        if not success:
            raise HTTPException(status_code=404, detail="Note not found")
        
        return {"message": "Note deleted successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 