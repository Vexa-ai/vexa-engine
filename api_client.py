import requests
import json
from datetime import datetime
from typing import Optional, List, Dict, Any, Generator, Union
from vexa import VexaAPI
from app.services.content.access import get_token_by_email
from enum import Enum
from uuid import UUID
import os
import httpx

API_PORT = int(os.getenv('API_PORT', '8010'))

from app.models.schema.content import (
    ContentType, EntityType, AccessLevel, ContentFilter,
    ContentListRequest, ContentCreate, ContentUpdate,
    ContentResponse, ContentIndexStatus
)

class MeetingOwnership(str, Enum):
    MY = "my"
    SHARED = "shared"
    ALL = "all"

class APIClient:
    def __init__(self, email: Optional[str] = None, base_url: str = f"http://127.0.0.1:{API_PORT}"):
        self.base_url = base_url
        self.email = email
        self.headers = {"Content-Type": "application/json"}
        self._initialized = False
        
    @classmethod
    async def create(cls, email: Optional[str] = None, base_url: str = f"http://127.0.0.1:{API_PORT}") -> 'APIClient':
        client = cls(email=email, base_url=base_url)
        if email:
            await client.set_email(email)
        return client
    
    async def set_email(self, email: str) -> None:
        self.email = email
        token = await self._get_token()
        self.headers["Authorization"] = f"Bearer {token}"
        self._initialized = True
    
    async def _get_token(self) -> str:
        if not self.email:
            raise ValueError("Email not set")
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/auth/token",
                json={"email": self.email}
            )
            if response.status_code != 200:
                raise Exception(f"Failed to get token: {response.text}")
            return response.json()["token"]
    
    async def _ensure_initialized(self):
        if not self._initialized:
            if not self.email:
                raise ValueError("Client not initialized and no email provided")
            await self.set_email(self.email)

    async def _request(self, method: str, path: str, **kwargs) -> Any:
        await self._ensure_initialized()
        url = f"{self.base_url}{path}"
        async with httpx.AsyncClient() as client:
            response = await client.request(method, url, headers=self.headers, **kwargs)
            if response.status_code >= 400:
                raise Exception(f"API error: {response.status_code} - {response.text}")
            return response.json()

    # Content Management
    async def list_contents(
        self,
        content_type: Optional[ContentType] = None,
        entity_type: Optional[str] = None,
        entity_names: Optional[List[str]] = None,
        parent_id: Optional[UUID] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        page: int = 1,
        page_size: int = 10
    ) -> Dict[str, Any]:
        params = ContentListRequest(
            content_type=content_type,
            entity_type=entity_type,
            entity_names=entity_names,
            parent_id=parent_id,
            start_date=start_date,
            end_date=end_date,
            page=page,
            page_size=page_size
        ).dict(exclude_none=True)
        
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/api/v1/contents",
                params=params,
                headers=self.headers
            )
            if response.status_code >= 400:
                raise Exception(f"Error listing contents: {response.text}")
            return response.json()

    async def create_content(
        self,
        content_type: ContentType,
        text: str,
        entities: List[Dict[str, str]],
        parent_id: Optional[UUID] = None,
    ) -> ContentResponse:
        await self._ensure_initialized()
        
        data = {
            "type": content_type.value,
            "text": text,
            "entities": [
                {
                    "name": e["name"],
                    "type": e["type"].value if isinstance(e["type"], Enum) else e["type"]
                }
                for e in entities
            ] if entities else [],
            "parent_id": str(parent_id) if parent_id else None
        }
        
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.base_url}/api/v1/contents",
                    json=data,
                    headers=self.headers
                )
                if response.status_code >= 400:
                    raise Exception(f"Error creating content: Status {response.status_code} - Response: {response.text}")
                    
                if not response.text:
                    raise Exception("Empty response received from server")
                    
                return ContentResponse(**response.json())
        except Exception as e:
            print(f"Request data: {data}")
            print(f"Headers: {self.headers}")
            print(f"Base URL: {self.base_url}")
            raise

    async def update_content(
        self,
        content_id: UUID,
        text: Optional[str] = None,
        entities: Optional[List[Dict[str, str]]] = None
    ) -> ContentResponse:
        data = ContentUpdate(
            text=text,
            entities=entities
        ).dict(exclude_none=True)
        
        async with httpx.AsyncClient() as client:
            response = await client.put(
                f"{self.base_url}/api/v1/contents/{content_id}",
                json=data,
                headers=self.headers
            )
            if response.status_code >= 400:
                raise Exception(f"Error updating content: {response.text}")
            return ContentResponse(**response.json())

    async def archive_content(
        self,
        content_id: UUID,
        archive_children: bool = False
    ) -> Dict[str, str]:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/api/v1/contents/{content_id}/archive",
                json={"archive_children": archive_children},
                headers=self.headers
            )
            if response.status_code >= 400:
                raise Exception(f"Error archiving content: {response.text}")
            return response.json()

    async def restore_content(
        self,
        content_id: UUID
    ) -> Dict[str, str]:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/api/v1/contents/{content_id}/restore",
                headers=self.headers
            )
            if response.status_code >= 400:
                raise Exception(f"Error restoring content: {response.text}")
            return response.json()

    async def index_content(
        self,
        content_id: UUID
    ) -> Dict[str, str]:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/api/v1/contents/{content_id}/index",
                headers=self.headers
            )
            if response.status_code >= 400:
                raise Exception(f"Error indexing content: {response.text}")
            return response.json()

    async def get_index_status(
        self,
        content_id: UUID
    ) -> ContentIndexStatus:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/api/v1/contents/{content_id}/index",
                headers=self.headers
            )
            if response.status_code >= 400:
                raise Exception(f"Error getting index status: {response.text}")
            return ContentIndexStatus(**response.json())

# Simple example to print API URL
if __name__ == "__main__":
    print(f"API URL: http://127.0.0.1:{API_PORT}")