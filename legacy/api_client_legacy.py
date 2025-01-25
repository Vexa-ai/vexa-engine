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
    ContentListRequest, ContentCreate, ContentUpdate
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
        entity_type: Optional[EntityType] = None,
        entity_names: Optional[List[str]] = None,
        parent_id: Optional[UUID] = None,
        date_from: Optional[datetime] = None,
        date_to: Optional[datetime] = None,
        limit: int = 20,
        offset: int = 0
    ) -> Dict[str, Any]:
        params = {
            "filter": {
                k: v for k, v in {
                    "content_type": content_type.value if content_type else None,
                    "entity_type": entity_type.value if entity_type else None,
                    "entity_names": entity_names,
                    "parent_id": str(parent_id) if parent_id else None,
                    "date_from": date_from.isoformat() if date_from else None,
                    "date_to": date_to.isoformat() if date_to else None
                }.items() if v is not None
            },
            "limit": limit,
            "offset": offset
        }
        return await self._request("GET", "/api/v1/contents", params=params)

    async def create_content(
        self,
        type: ContentType,
        text: str,
        parent_id: Optional[UUID] = None,
        entities: Optional[List[Dict[str, str]]] = None
    ) -> Dict[str, Any]:
        data = {
            "type": type.value,
            "text": text,
            "parent_id": str(parent_id) if parent_id else None,
            "entities": [
                {"name": e["name"], "type": e["type"]} 
                for e in (entities or [])
            ]
        }
        return await self._request("POST", "/api/v1/contents", json=data)

    async def get_content(self, content_id: UUID) -> Dict[str, Any]:
        return await self._request("GET", f"/api/v1/contents/{content_id}")

    async def update_content(
        self,
        content_id: UUID,
        text: Optional[str] = None,
        entities: Optional[List[Dict[str, str]]] = None
    ) -> Dict[str, Any]:
        data = {k: v for k, v in {
            "text": text,
            "entities": [
                {"name": e["name"], "type": e["type"]} 
                for e in (entities or [])
            ] if entities is not None else None
        }.items() if v is not None}
        return await self._request("PUT", f"/api/v1/contents/{content_id}", json=data)

    async def archive_content(
        self,
        content_id: UUID,
        archive_children: bool = False
    ) -> Dict[str, Any]:
        data = {"archive_children": archive_children}
        return await self._request(
            "POST", 
            f"/api/v1/contents/{content_id}/archive",
            json=data
        )

    async def restore_content(self, content_id: UUID) -> Dict[str, Any]:
        return await self._request("POST", f"/api/v1/contents/{content_id}/restore")

    async def index_content(self, content_id: UUID) -> Dict[str, Any]:
        return await self._request("POST", f"/api/v1/contents/{content_id}/index")

    async def get_index_status(self, content_id: UUID) -> Dict[str, Any]:
        return await self._request("GET", f"/api/v1/contents/{content_id}/index")

    async def get_meetings(
        self, 
        offset: int = 0, 
        limit: int = 50, 
        include_summary: bool = False, 
        ownership: MeetingOwnership = MeetingOwnership.ALL
    ) -> Dict:
        """Get meetings with ownership filter"""
        await self._ensure_initialized()
        
        response = requests.get(
            f"{self.base_url}/meetings/all", 
            headers=self.headers,
            params={
                "offset": offset, 
                "limit": limit,
                "include_summary": include_summary,
                "ownership": ownership
            }
        )
        
        if response.status_code == 401:
            raise ValueError("Unauthorized. Please check your authentication token.")
        elif response.status_code != 200:
            raise Exception(f"API request failed with status {response.status_code}: {response.text}")
            
        return response.json()

    async def get_my_meetings(self, offset: int = 0, limit: int = 50, include_summary: bool = False) -> Dict:
        """Get only meetings owned by the user"""
        return await self.get_meetings(
            offset=offset,
            limit=limit,
            include_summary=include_summary,
            ownership=MeetingOwnership.MY
        )

    async def get_shared_meetings(self, offset: int = 0, limit: int = 50, include_summary: bool = False) -> Dict:
        """Get only meetings shared with the user"""
        return await self.get_meetings(
            offset=offset,
            limit=limit,
            include_summary=include_summary,
            ownership=MeetingOwnership.SHARED
        )

    def submit_token(self, token: str) -> Dict:
        response = requests.post(f"{self.base_url}/submit_token", json={"token": token})
        result = response.json()
        print(f"User ID: {result.get('user_id')}\nUser Name: {result.get('user_name')}\nImage URL: {result.get('image')}")
        return result

    def chat(
        self, 
        query: str, 
        meeting_id: Optional[str] = None,
        entities: Optional[List[str]] = None,
        thread_id: Optional[str] = None, 
        model: str = "gpt-4o-mini", 
        temperature: float = 0.7
    ) -> Dict:
        """Unified chat method that supports both general and meeting-specific queries"""
        response = requests.post(
            f"{self.base_url}/chat", 
            headers=self.headers, 
            json={
                "query": query,
                "meeting_id": meeting_id,
                "entities": entities,
                "thread_id": thread_id,
                "model": model,
                "temperature": temperature
            },
            stream=True
        )
        
        final_response = None
        for line in response.iter_lines():
            if line:
                try:
                    data = json.loads(line.decode('utf-8').replace('data: ', ''))
                    if data.get('type') == 'stream':
                        print(data.get('content', ''), end='', flush=True)
                    elif data.get('type') == 'done':
                        break
                    else:
                        final_response = data
                except json.JSONDecodeError:
                    continue
        return final_response or {"error": "No response received"}

    async def get_meeting_details(self, meeting_id: str) -> Dict:
        await self._ensure_initialized()
        return requests.get(f"{self.base_url}/meeting/{meeting_id}/details", headers=self.headers).json()

    def create_share_link(
        self, 
        access_level: str = "search",
        meeting_ids: Optional[List[str]] = None,
        target_email: Optional[str] = None,
        expiration_hours: Optional[int] = 24
    ) -> Dict:
        """Create a share link for specific meetings or general access"""
        return requests.post(
            f"{self.base_url}/share-links", 
            headers=self.headers, 
            json={
                "access_level": access_level,
                "meeting_ids": meeting_ids,
                "target_email": target_email,
                "expiration_hours": expiration_hours
            }
        ).json()

    def accept_share_link(self, token: str, accepting_email: str) -> Dict:
        return requests.post(f"{self.base_url}/share-links/accept", headers=self.headers,
            json={"token": token, "accepting_email": accepting_email}).json()

    def start_indexing(self, num_meetings: int = 200) -> Dict:
        return requests.post(f"{self.base_url}/start_indexing", headers=self.headers,
            json={"num_meetings": num_meetings}).json()

    def get_indexing_status(self) -> Dict:
        return requests.get(f"{self.base_url}/indexing_status", headers=self.headers).json()

    def get_threads(self, meeting_id: Optional[str] = None) -> Dict:
        if not self._initialized:
            raise ValueError("Client not initialized. Please call set_email first.")
        
        if meeting_id:
            try:
                UUID(meeting_id)  # Validate UUID format
                response = requests.get(f"{self.base_url}/threads/{meeting_id}", headers=self.headers)
            except ValueError:
                raise ValueError("Invalid meeting ID format - must be a valid UUID")
        else:
            response = requests.get(f"{self.base_url}/threads/all", headers=self.headers)
        
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"API request failed with status {response.status_code}: {response.text}")

    def delete_thread(self, thread_id: str) -> Dict:
        return requests.delete(f"{self.base_url}/thread/{thread_id}", headers=self.headers).json()

    def get_meetings_by_speakers(self, speakers: List[str], limit: int = 50, offset: int = 0) -> Dict:
        return requests.post(f"{self.base_url}/meetings/by-speakers", headers=self.headers,
            json={"speakers": speakers, "limit": limit, "offset": offset}).json()

    def revoke_meeting_share(self, meeting_id: str, token: str) -> Dict:
        """Revoke share access for a specific meeting"""
        return requests.post(
            f"{self.base_url}/meetings/{meeting_id}/revoke-share/{token}",
            headers=self.headers
        ).json()

    def get_global_threads(self) -> Dict:
        """Get threads that are not associated with any content or entities"""
        if not self._initialized:
            raise ValueError("Client not initialized. Please call set_email first.")
        
        response = requests.get(f"{self.base_url}/threads/global", headers=self.headers)
        
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"API request failed with status {response.status_code}: {response.text}")

    def get_threads_by_entities(self, entity_names: List[str]) -> Dict:
        """Get threads associated with exact set of entities"""
        if not self._initialized:
            raise ValueError("Client not initialized. Please call set_email first.")
        
        response = requests.post(
            f"{self.base_url}/threads/by-entities",
            headers=self.headers,
            json={"entity_names": entity_names}
        )
        
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"API request failed with status {response.status_code}: {response.text}")

    async def get_meeting_threads(self, meeting_id: str) -> Dict:
        """Get threads associated with a specific meeting
        
        Args:
            meeting_id (str): UUID of the meeting
            
        Returns:
            Dict: List of threads associated with the meeting
            
        Raises:
            ValueError: If client not initialized or invalid meeting ID format
            Exception: If API request fails
        """
        if not self._initialized:
            raise ValueError("Client not initialized. Please call set_email first.")
        
        try:
            # Validate UUID format
            UUID(meeting_id)
            await self._ensure_initialized()
            
            response = requests.get(
                f"{self.base_url}/threads/meeting/{meeting_id}", 
                headers=self.headers
            )
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                raise ValueError("Meeting not found")
            elif response.status_code == 403:
                raise ValueError("Access denied to meeting")
            else:
                raise Exception(f"API request failed with status {response.status_code}: {response.text}")
            
        except ValueError as e:
            if "invalid literal for UUID" in str(e):
                raise ValueError("Invalid meeting ID format - must be a valid UUID")
            raise

    async def get_contents(
        self,
        offset: int = 0,
        limit: Optional[int] = None,
        parent_id: Optional[Union[str, UUID]] = None,
        filter: Optional[ContentFilter] = None
    ) -> Dict:
        """Get contents with optional filtering
        
        Args:
            offset (int): Pagination offset
            limit (Optional[int]): Maximum number of contents to return
            parent_id (Optional[Union[str, UUID]]): Filter by parent content ID
            filter (Optional[ContentFilter]): Filter by entity type and names
            
        Returns:
            Dict: {
                "total": int,
                "contents": List[Dict] containing content items with their entities
            }
        """
        await self._ensure_initialized()
        
        # Build query parameters
        params = {"offset": offset}
        if limit is not None:
            params["limit"] = limit
        if parent_id is not None:
            # Convert to string if UUID
            params["parent_id"] = str(parent_id) if isinstance(parent_id, UUID) else parent_id
        if filter is not None:
            params["filter"] = json.dumps(filter.to_dict())
        
        response = requests.get(
            f"{self.base_url}/contents",
            headers=self.headers,
            params=params
        )
        
        if response.status_code == 401:
            raise ValueError("Unauthorized. Please check your authentication token.")
        elif response.status_code != 200:
            raise Exception(f"API request failed with status {response.status_code}: {response.text}")
        
        return response.json()

    async def get_root_contents(
        self,
        offset: int = 0,
        limit: Optional[int] = None
    ) -> Dict:
        """Get root contents (those without parent)"""
        return await self.get_contents(offset=offset, limit=limit)

    async def get_child_contents(
        self,
        parent_id: Union[str, UUID],
        offset: int = 0,
        limit: Optional[int] = None
    ) -> Dict:
        """Get child contents for a specific parent"""
        return await self.get_contents(
            parent_id=parent_id,
            offset=offset,
            limit=limit
        )

    async def get_contents_by_entity(
        self,
        entity_type: str,
        entity_names: List[str],
        offset: int = 0,
        limit: Optional[int] = None
    ) -> Dict:
        """Get contents filtered by entity type and names"""
        filter = ContentFilter(entity_type=entity_type, entity_names=entity_names)
        return await self.get_contents(
            filter=filter,
            offset=offset,
            limit=limit
        )

async def complete_workflow():
    """Example workflow demonstrating API client usage"""
    print(f"API URL: http://127.0.0.1:{API_PORT}")

if __name__ == "__main__":
    import asyncio
    asyncio.run(complete_workflow())