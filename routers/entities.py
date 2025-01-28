from fastapi import FastAPI, HTTPException, Depends, Query, APIRouter
from typing import Optional
from psql_models import EntityType
from entity_manager import EntityManager
from routers.common import get_current_user

router = APIRouter(prefix="/entities", tags=["entities"])

@router.get("/{entity_type}")
async def get_entities(
    entity_type: str,
    offset: int = Query(0),
    limit: int = Query(20),
    current_user: tuple = Depends(get_current_user)
):
    user_id, user_name, token = current_user
    
    try:
        manager = await EntityManager.create()
        return await manager.get_entities(
            user_id=user_id,
            entity_type=entity_type,
            offset=offset,
            limit=limit
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
