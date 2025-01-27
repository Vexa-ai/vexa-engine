import requests
import json
from datetime import datetime
from typing import Optional, List, Dict, Any, Generator, AsyncGenerator
from vexa import VexaAPI
from psql_access import get_token_by_email
from enum import Enum
from uuid import UUID
from psql_models import ContentType
import aiohttp
import logging

import os
API_PORT = os.getenv('API_PORT')

logger = logging.getLogger(__name__)

class MeetingOwnership(str, Enum):
    MY = "my"
    SHARED = "shared"
    ALL = "all"

class APIError(Exception):
    """Custom exception for API errors"""
    pass

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
        """Set email and initialize authorization headers"""
        self.email = email
        token, _ = await get_token_by_email(email)
        if not token:
            raise ValueError(f"No token found for email: {email}")
        self.token = token
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
            "X-User-Email": email
        }
        self._initialized = True

    async def _ensure_initialized(self):
        """Ensure client is initialized with proper headers"""
        if not self._initialized:
            if not self.email:
                raise ValueError("Email not set. Please call set_email first.")
            await self.set_email(self.email)

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

    async def chat(
        self,
        query: str,
        content_id: Optional[UUID] = None,
        entity: Optional[str] = None,
        content_ids: Optional[List[UUID]] = None,
        entities: Optional[List[str]] = None,
        thread_id: Optional[str] = None,
        model: Optional[str] = None,
        temperature: Optional[float] = None,
        meta: Optional[dict] = None
    ) -> AsyncGenerator[Dict[str, Any], None]:
        logger.debug("Starting chat method")
        
        # Validate ID combinations
        if content_id and entity:
            logger.error("Cannot specify both content_id and entity")
            raise ValueError("Cannot specify both content_id and entity")
        if content_ids and entities:
            logger.error("Cannot specify both content_ids and entities")
            raise ValueError("Cannot specify both content_ids and entities")
        if (content_id and content_ids) or (entity and entities):
            logger.error("Cannot specify both single and multiple values of same type")
            raise ValueError("Cannot specify both single and multiple values of the same type")

        # Prepare payload
        payload = {
            "query": query,
            "content_id": str(content_id) if content_id else None,
            "entity": entity,
            "content_ids": [str(cid) for cid in content_ids] if content_ids else None,
            "entities": entities,
            "thread_id": thread_id,
            "model": model,
            "temperature": temperature,
            "meta": meta
        }
        logger.debug(f"Prepared payload: {payload}")

        async with aiohttp.ClientSession() as session:
            logger.debug(f"Making POST request to {self.base_url}/chat")
            try:
                async with session.post(f"{self.base_url}/chat", json=payload, headers=self.headers) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        logger.error(f"Chat request failed with status {response.status}: {error_text}")
                        raise APIError(f"Chat request failed: {error_text}")

                    logger.debug("Starting to process response stream")
                    buffer = ""
                    
                    # Read the full response
                    response_data = await response.read()
                    if not response_data:
                        return
                        
                    # Process response in chunks
                    chunks = response_data.split(b'\n\n')
                    for chunk in chunks:
                        if not chunk:
                            continue
                            
                        chunk_data = chunk.decode('utf-8').strip()
                        if chunk_data.startswith('data: '):
                            try:
                                data = json.loads(chunk_data[6:])
                                logger.debug(f"Parsed JSON data: {data}")
                                
                                if 'error' in data:
                                    logger.error(f"Error in response: {data['error']}")
                                    yield data
                                    return
                                elif 'chunk' in data:
                                    logger.debug(f"Yielding chunk: {data['chunk']}")
                                    yield {'chunk': data['chunk']}
                                else:
                                    logger.debug(f"Yielding final data: {data}")
                                    yield data
                            except json.JSONDecodeError as e:
                                logger.error(f"JSON decode error: {e}")
                                continue
                                    
            except aiohttp.ClientError as e:
                logger.error(f"Network error during chat: {str(e)}")
                raise APIError(f"Network error: {str(e)}")
            except Exception as e:
                logger.error(f"Unexpected error during chat: {str(e)}")
                raise APIError(f"Unexpected error: {str(e)}")

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

    async def get_threads(
        self,
        content_id: Optional[UUID] = None,
        entity: Optional[str] = None,
        content_ids: Optional[List[UUID]] = None,
        entities: Optional[List[str]] = None
    ) -> Dict:
        """Get threads for content or entity
        
        Args:
            content_id: Single content ID for direct thread mapping
            entity: Single entity name for direct thread mapping
            content_ids: Multiple content IDs for metadata search
            entities: Multiple entity names for metadata search
        """
        if not self._initialized:
            raise ValueError("Client not initialized. Please call set_email first.")

        if content_id and entity:
            raise ValueError("Cannot specify both content_id and entity for thread mapping")
            
        if content_ids and entities:
            raise ValueError("Cannot specify both content_ids and entities for search")
            
        if (content_id and content_ids) or (entity and entities):
            raise ValueError("Cannot specify both single and multiple values of the same type")
        
        params = {}
        if content_id:
            params['content_id'] = str(content_id)
        if entity:
            params['entity'] = entity
        if content_ids:
            params['content_ids'] = [str(cid) for cid in content_ids]
        if entities:
            params['entities'] = entities
        
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{self.base_url}/threads",
                headers=self.headers,
                params=params
            ) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    raise Exception(f"API request failed with status {response.status}: {await response.text()}")

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
        content_type: Optional[str] = None,
        filters: Optional[List[Dict[str, List[str]]]] = None,
        parent_id: Optional[str] = None,
        offset: int = 0,
        limit: int = 20,
        ownership: MeetingOwnership = MeetingOwnership.ALL
    ) -> Dict:
        """Get contents with optional filters
        
        Args:
            content_type: Optional type filter (meeting, note, etc)
            filters: List of filter dicts with format {"type": str, "values": List[str]}
            parent_id: Optional parent content ID
            offset: Pagination offset
            limit: Pagination limit
            ownership: Filter by ownership (MY, SHARED, ALL)
        """
        await self._ensure_initialized()
        
        params = {
            "offset": offset,
            "limit": limit,
            "ownership": ownership
        }
        
        if content_type:
            params["content_type"] = content_type
            
        if parent_id:
            params["parent_id"] = parent_id
            
        if filters:
            params["filters"] = json.dumps(filters)
        
        response = requests.get(
            f"{self.base_url}/contents/all",
            headers=self.headers,
            params=params
        )
        
        if response.status_code == 401:
            raise ValueError("Unauthorized. Please check your authentication token.")
        elif response.status_code != 200:
            raise Exception(f"API request failed with status {response.status_code}: {response.text}")
            
        return response.json()

    async def add_content(
        self,
        body: str,
        content_type: str,
        parent_id: Optional[UUID] = None,
        entities: Optional[List[Dict[str, str]]] = None
    ) -> Dict[str, Any]:
        """Add new content with optional parent and entities
        
        Args:
            body: Content text
            content_type: Type of content (e.g. 'note', 'meeting')
            parent_id: Optional UUID of parent content
            entities: Optional list of entities with type and name
            
        Returns:
            Dict with content_id, timestamp, type, and parent_id
        """
        if not self._initialized:
            raise ValueError("Client not initialized. Please call set_email first.")
            
        # Convert string content type to enum - keep original case
        try:
            content_type_enum = ContentType(content_type)
        except ValueError:
            raise ValueError(f"Invalid content type: {content_type}. Must be one of: {[t.value for t in ContentType]}")
            
        data = {
            "body": body,
            "type": content_type_enum.value,
            "parent_id": str(parent_id) if parent_id else None,
            "entities": entities or [],
            "access_level": "owner"  # Set owner access level for created content
        }
        
        response = requests.post(
            f"{self.base_url}/api/content",
            headers=self.headers,
            json=data
        )
        
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"API request failed with status {response.status_code}: {response.text}")

    async def modify_content(
        self,
        content_id: UUID,
        body: str,
        entities: Optional[List[Dict[str, str]]] = None
    ) -> Dict[str, Any]:
        """Modify existing content with new text and entities
        
        Args:
            content_id: UUID of content to modify
            body: New content text
            entities: Optional list of entities with type and name
            
        Returns:
            Dict with content_id, timestamp, type, and parent_id
        """
        if not self._initialized:
            raise ValueError("Client not initialized. Please call set_email first.")
            
        data = {
            "content_id": str(content_id),
            "body": body,
            "entities": entities or []
        }
        
        response = requests.post(
            f"{self.base_url}/api/content/modify",
            headers=self.headers,
            json=data
        )
        
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"API request failed with status {response.status_code}: {response.text}")
            
    async def archive_content(self, content_id: UUID) -> bool:
        """Archive content
        
        Args:
            content_id: UUID of content to archive
            
        Returns:
            True if successful
        """
        if not self._initialized:
            raise ValueError("Client not initialized. Please call set_email first.")
            
        data = {"content_id": str(content_id)}
        
        response = requests.post(
            f"{self.base_url}/api/content/archive",
            headers=self.headers,
            json=data
        )
        
        if response.status_code == 200:
            return response.json()["success"]
        else:
            raise Exception(f"API request failed with status {response.status_code}: {response.text}")
            
    async def restore_content(self, content_id: UUID) -> bool:
        """Restore archived content
        
        Args:
            content_id: UUID of content to restore
            
        Returns:
            True if successful
        """
        if not self._initialized:
            raise ValueError("Client not initialized. Please call set_email first.")
            
        data = {"content_id": str(content_id)}
        
        response = requests.post(
            f"{self.base_url}/api/content/restore",
            headers=self.headers,
            json=data
        )
        
        if response.status_code == 200:
            return response.json()["success"]
        else:
            raise Exception(f"API request failed with status {response.status_code}: {response.text}")

    async def get_content(self, content_id: UUID) -> Dict[str, Any]:
        """Get content by ID including children and entities
        
        Args:
            content_id: UUID of content to get
            
        Returns:
            Dict with content details including text, entities, and children
            
        Raises:
            ValueError: If content not found or no access
        """
        if not self._initialized:
            raise ValueError("Client not initialized. Please call set_email first.")
            
        response = requests.get(
            f"{self.base_url}/api/content/{content_id}",
            headers=self.headers
        )
        
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            raise ValueError("Content not found or no access")
        else:
            raise Exception(f"API request failed with status {response.status_code}: {response.text}")

    async def get_entities(
        self,
        entity_type: str,
        offset: int = 0,
        limit: int = 20
    ) -> Dict[str, Any]:
        """Get entities of a specific type
        
        Args:
            entity_type: Type of entities to get (e.g. 'tag', 'speaker')
            offset: Pagination offset
            limit: Pagination limit
            
        Returns:
            Dict with total count and list of entities
            
        Raises:
            ValueError: If invalid entity type
        """
        if not self._initialized:
            raise ValueError("Client not initialized. Please call set_email first.")
            
        response = requests.get(
            f"{self.base_url}/api/entities/{entity_type}",
            headers=self.headers,
            params={
                "offset": offset,
                "limit": limit
            }
        )
        
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 400:
            raise ValueError(f"Invalid entity type: {entity_type}")
        else:
            raise Exception(f"API request failed with status {response.status_code}: {response.text}")