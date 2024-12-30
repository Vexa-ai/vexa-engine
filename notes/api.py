from fastapi import APIRouter, Depends, HTTPException, Header
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from datetime import datetime
from uuid import UUID
import logging

from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from psql_models import Content, UserContent, ContentType, AccessLevel, UserToken
from psql_helpers import async_session, get_session
from psql_sharing import has_content_access
from psql_notes import create_note, get_notes_by_user, update_note, delete_note
from token_manager import TokenManager
from indexing.meetings_monitor import MeetingsMonitor

# Set up logging
logger = logging.getLogger(__name__)

token_manager = TokenManager()
monitor = MeetingsMonitor()

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
    is_indexed: bool = False

class ArchivedNoteResponse(BaseModel):
    id: UUID
    text: str
    parent_id: Optional[UUID]
    timestamp: datetime
    type: str
    is_indexed: bool = False

async def get_current_user(authorization: str = Header(...)):
    token = authorization.split("Bearer ")[-1]
    try:
        user_id, user_name = await token_manager.check_token(token)
        if not user_id:
            raise HTTPException(status_code=401, detail="Invalid token")
        return user_id, user_name, token
    except Exception as e:
        raise HTTPException(status_code=401, detail="Invalid token")

@router.post("", response_model=NoteResponse)
async def create_new_note(
    note: NoteCreate,
    current_user: tuple = Depends(get_current_user)
):
    user_id, user_name, token = current_user
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
                
                # Ensure user token exists
                token_query = select(UserToken).where(UserToken.token == token)
                existing_token = await session.execute(token_query)
                token_record = existing_token.scalar_one_or_none()
                
                if not token_record:
                    logger.debug(f"Creating new token record for user {user_id}")
                    token_record = UserToken(
                        token=token,
                        user_id=user_id,
                        last_used_at=datetime.now()
                    )
                    session.add(token_record)
                    await session.commit()
                
                # Add note to indexing queue
                monitor._add_to_queue(str(created_note.id))
                logger.info(f"Added note {created_note.id} to indexing queue")
                
            except Exception as db_error:
                logger.error(f"Database error while creating note: {str(db_error)}", exc_info=True)
                raise
            
            response = NoteResponse(
                id=created_note.id,
                text=created_note.text,
                parent_id=created_note.parent_id,
                timestamp=created_note.timestamp,
                is_owner=True,
                access_level=AccessLevel.OWNER.value,
                is_indexed=created_note.is_indexed
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
    user_id, user_name, token = current_user
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
                    access_level=note["access_level"],
                    is_indexed=note.get("is_indexed", False)
                )
                for note in notes
            ]
            logger.debug(f"Returning {len(response)} notes")
            return response
            
    except Exception as e:
        logger.error(f"Error fetching notes: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/{note_id}", response_model=NoteResponse)
async def update_note(
    note_id: UUID,
    note: NoteUpdate,
    current_user: tuple = Depends(get_current_user),
):
    user_id, user_name, token = current_user
    logger.info(f"Updating note {note_id} for user {user_id} ({user_name})")
    logger.debug(f"Update data: {note.dict()}")
    
    try:
        async with async_session() as session:
            # Verify ownership
            has_access = await has_content_access(session, user_id, note_id)
            if not has_access:
                logger.warning(f"User {user_id} attempted to update unauthorized note {note_id}")
                raise HTTPException(status_code=403, detail="No access to note")
            
            # Update note
            try:
                updated_note = await update_note(
                    session=session,
                    note_id=note_id,
                    user_id=user_id,
                    text=note.text
                )
                if not updated_note:
                    raise HTTPException(status_code=404, detail="Note not found")
                    
                logger.info(f"Note {note_id} updated successfully")
                
                response = NoteResponse(
                    id=updated_note.id,
                    text=updated_note.text,
                    parent_id=updated_note.parent_id,
                    timestamp=updated_note.timestamp,
                    is_owner=True,
                    access_level=AccessLevel.OWNER.value,
                    is_indexed=updated_note.is_indexed
                )
                logger.debug(f"Returning response: {response.dict()}")
                return response
                
            except Exception as db_error:
                logger.error(f"Database error while updating note: {str(db_error)}", exc_info=True)
                raise
                
    except HTTPException as http_error:
        logger.error(f"HTTP error in note update: {str(http_error)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in note update: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/{note_id}")
async def delete_note(
    note_id: UUID,
    current_user: tuple = Depends(get_current_user),
):
    user_id, user_name, token = current_user
    logger.info(f"Deleting note {note_id} for user {user_id} ({user_name})")
    
    try:
        async with async_session() as session:
            # Verify ownership
            has_access = await has_content_access(session, user_id, note_id)
            if not has_access:
                logger.warning(f"User {user_id} attempted to delete unauthorized note {note_id}")
                raise HTTPException(status_code=403, detail="No access to note")
            
            # Delete note
            try:
                success = await delete_note(
                    session=session,
                    note_id=note_id,
                    user_id=user_id
                )
                if success:
                    logger.info(f"Note {note_id} deleted successfully")
                    return {"message": "Note deleted successfully"}
                else:
                    logger.error(f"Failed to delete note {note_id}")
                    raise HTTPException(status_code=404, detail="Note not found")
                
            except Exception as db_error:
                logger.error(f"Database error while deleting note: {str(db_error)}", exc_info=True)
                raise
                
    except HTTPException as http_error:
        logger.error(f"HTTP error in note deletion: {str(http_error)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in note deletion: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/{note_id}/archive")
async def archive_note(
    note_id: UUID,
    current_user: tuple = Depends(get_current_user),
):
    user_id, user_name, token = current_user
    logger.info(f"Archiving note {note_id} for user {user_id} ({user_name})")
    
    try:
        async with async_session() as session:
            # Verify ownership
            has_access = await has_content_access(session, user_id, note_id)
            if not has_access:
                logger.warning(f"User {user_id} attempted to archive unauthorized note {note_id}")
                raise HTTPException(status_code=403, detail="No access to note")
            
            # Archive note
            try:
                success = await archive_content(
                    session=session,
                    user_id=user_id,
                    content_id=note_id,
                    cleanup_search=True
                )
                if success:
                    logger.info(f"Note {note_id} archived successfully")
                    return {"message": "Note archived successfully"}
                else:
                    logger.error(f"Failed to archive note {note_id}")
                    raise HTTPException(status_code=404, detail="Note not found")
                
            except Exception as db_error:
                logger.error(f"Database error while archiving note: {str(db_error)}", exc_info=True)
                raise
                
    except HTTPException as http_error:
        logger.error(f"HTTP error in note archiving: {str(http_error)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in note archiving: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/archive", response_model=List[ArchivedNoteResponse])
async def get_archived_notes(
    current_user: tuple = Depends(get_current_user),
    limit: int = 50,
    offset: int = 0
):
    user_id, user_name, token = current_user
    logger.info(f"Fetching archived notes for user {user_id} ({user_name})")
    
    try:
        async with async_session() as session:
            logger.debug("Starting database query")
            archived_notes, total = await get_archived_content(
                user_id=user_id,
                content_type=ContentType.NOTE,
                limit=limit,
                offset=offset,
                session=session
            )
            logger.debug(f"Found {len(archived_notes)} archived notes")
            
            # Get full note data for each archived note
            response = []
            for note in archived_notes:
                note_query = select(Content).where(
                    Content.id == UUID(note['content_id'])
                )
                result = await session.execute(note_query)
                note_data = result.scalar_one()
                
                response.append(ArchivedNoteResponse(
                    id=UUID(note['content_id']),
                    text=note_data.text,
                    parent_id=note_data.parent_id,
                    timestamp=note['timestamp'],
                    type=note['type'],
                    is_indexed=note['is_indexed']
                ))
            
            logger.debug(f"Returning {len(response)} archived notes")
            return response
            
    except Exception as e:
        logger.error(f"Error fetching archived notes: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/{note_id}/restore")
async def restore_note(
    note_id: UUID,
    current_user: tuple = Depends(get_current_user),
):
    user_id, user_name, token = current_user
    logger.info(f"Restoring note {note_id} for user {user_id} ({user_name})")
    
    try:
        async with async_session() as session:
            # Verify note exists in archive
            archived_notes, _ = await get_archived_content(
                user_id=user_id,
                session=session
            )
            if not any(n['content_id'] == str(note_id) for n in archived_notes):
                logger.warning(f"User {user_id} attempted to restore non-archived note {note_id}")
                raise HTTPException(status_code=404, detail="Note not found in archive")
            
            # Restore note
            try:
                success = await restore_content(
                    session=session,
                    user_id=user_id,
                    content_id=note_id,
                    restore_access_level=AccessLevel.TRANSCRIPT
                )
                if success:
                    logger.info(f"Note {note_id} restored successfully")
                    return {"message": "Note restored successfully"}
                else:
                    logger.error(f"Failed to restore note {note_id}")
                    raise HTTPException(status_code=500, detail="Failed to restore note")
                
            except Exception as db_error:
                logger.error(f"Database error while restoring note: {str(db_error)}", exc_info=True)
                raise
                
    except HTTPException as http_error:
        logger.error(f"HTTP error in note restoration: {str(http_error)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in note restoration: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) 