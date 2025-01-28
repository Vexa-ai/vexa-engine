from psql_access import get_token_by_email
import requests
import json
from datetime import datetime
from typing import Optional, List, Dict, Any, AsyncGenerator
from uuid import UUID
import aiohttp
import logging
from google_client import GoogleClient, GoogleError
from enum import Enum
import os

API_PORT = os.getenv('API_PORT', '8010')
logger = logging.getLogger(__name__)

class APIError(Exception):
    pass

class UUIDEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, UUID):
            return str(obj)
        return super().default(obj)

class APIClient:
    def __init__(self, email: Optional[str] = None, base_url: str = f"http://127.0.0.1:{API_PORT}"):
        self.base_url = base_url
        self.email = email
        self.headers = {"Content-Type": "application/json"}
        self._initialized = False
        self.google_client = GoogleClient()

    @classmethod
    async def create(cls, email: Optional[str] = None, base_url: str = f"http://127.0.0.1:{API_PORT}") -> 'APIClient':
        client = cls(email=email, base_url=base_url)
        if email:
            await client.set_email(email)
        return client

    async def set_email(self, email: str) -> None:
        self.email = email
        token, _ = await get_token_by_email(email)
        if not token:
            raise ValueError(f"No token found for email: {email}")
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
            "X-User-Email": email
        }
        self._initialized = True

    # Auth Router Methods
    async def google_auth(self, token: str, utm_params: Optional[Dict[str, str]] = None) -> Dict:
        try:
            user_info = await self.google_client.get_user_info(token)
            data = {
                "token": token,
                "email": user_info.email,
                "name": user_info.name,
                "picture": user_info.picture
            }
            if utm_params:
                data.update(utm_params)
            response = requests.post(f"{self.base_url}/auth/google", json=data)
            if response.status_code != 200:
                raise APIError(f"Google auth failed: {response.text}")
        return response.json()
        except GoogleError as e:
            raise APIError(f"Google token verification failed: {str(e)}")
        except Exception as e:
            raise APIError(f"Google auth failed: {str(e)}")

    async def submit_token(self, token: str) -> Dict:
        response = requests.post(f"{self.base_url}/submit_token", json={"token": token})
        if response.status_code != 200:
            raise APIError(f"Token submission failed: {response.text}")
            return response.json()

    # Contents Router Methods
    async def get_contents(self, content_type: Optional[str] = None, filters: Optional[List[Dict]] = None, 
                          offset: int = 0, limit: int = 20, ownership: str = "all") -> Dict:
        if not self._initialized:
            raise ValueError("Client not initialized. Please call set_email first.")
        params = {
            "content_type": content_type,
            "filters": json.dumps(filters) if filters else None,
            "offset": offset,
            "limit": limit,
            "ownership": ownership
        }
        response = requests.get(f"{self.base_url}/contents/all", headers=self.headers, params=params)
        if response.status_code != 200:
            raise APIError(f"Failed to get contents: {response.text}")
        return response.json()

    async def get_content(self, content_id: UUID) -> Dict[str, Any]:
        if not self._initialized:
            raise ValueError("Client not initialized. Please call set_email first.")
        response = requests.get(f"{self.base_url}/contents/{content_id}", headers=self.headers)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            raise ValueError("Content not found or no access")
        else:
            raise APIError(f"Failed to get content: {response.text}")

    async def add_content(self, body: str, content_type: str, entities: Optional[List[Dict[str, str]]] = None) -> Dict[str, str]:
        if not self._initialized:
            raise ValueError("Client not initialized. Please call set_email first.")
        data = {
            "type": content_type,
            "text": body,
            "entities": entities
        }
        response = requests.post(f"{self.base_url}/contents", headers=self.headers, json=data)
        if response.status_code != 200:
            raise APIError(f"Failed to add content: {response.text}")
            return response.json()

    async def modify_content(self, content_id: UUID, body: str, entities: Optional[List[Dict[str, str]]] = None) -> Dict[str, bool]:
        if not self._initialized:
            raise ValueError("Client not initialized. Please call set_email first.")
        data = {
            "text": body,
            "entities": entities
        }
        response = requests.put(f"{self.base_url}/contents/{content_id}", headers=self.headers, json=data)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            raise ValueError("Content not found or no access")
        else:
            raise APIError(f"Failed to modify content: {response.text}")

    async def delete_content(self, content_id: UUID) -> Dict[str, bool]:
        if not self._initialized:
            raise ValueError("Client not initialized. Please call set_email first.")
        response = requests.delete(f"{self.base_url}/contents/{content_id}", headers=self.headers)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            raise ValueError("Content not found or no access")
        else:
            raise APIError(f"Failed to delete content: {response.text}")

    async def archive_content(self, content_id: UUID) -> Dict[str, bool]:
        if not self._initialized:
            raise ValueError("Client not initialized. Please call set_email first.")
        response = requests.post(f"{self.base_url}/contents/{content_id}/archive", headers=self.headers)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            raise ValueError("Content not found or no access")
        else:
            raise APIError(f"Failed to archive content: {response.text}")

    async def restore_content(self, content_id: UUID) -> Dict[str, bool]:
        if not self._initialized:
            raise ValueError("Client not initialized. Please call set_email first.")
        response = requests.post(f"{self.base_url}/contents/{content_id}/restore", headers=self.headers)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            raise ValueError("Content not found or not archived")
        else:
            raise APIError(f"Failed to restore content: {response.text}")

    async def create_share_link(self, access_level: str, meeting_ids: List[UUID], target_email: Optional[str] = None, 
                              expiration_hours: Optional[int] = None) -> Dict[str, str]:
        if not self._initialized:
            raise ValueError("Client not initialized. Please call set_email first.")
        data = {
            "access_level": access_level,
            "meeting_ids": meeting_ids,
            "target_email": target_email,
            "expiration_hours": expiration_hours
        }
        headers = {**self.headers, "Content-Type": "application/json"}
        response = requests.post(
            f"{self.base_url}/contents/share-links",
            headers=headers,
            data=json.dumps(data, cls=UUIDEncoder)
        )
        if response.status_code != 200:
            raise APIError(f"Failed to create share link: {response.text}")
        return response.json()

    async def accept_share_link(self, token: str, accepting_email: str) -> Dict[str, str]:
        if not self._initialized:
            raise ValueError("Client not initialized. Please call set_email first.")
        data = {
            "token": token,
            "accepting_email": accepting_email
        }
        response = requests.post(f"{self.base_url}/contents/share-links/accept", headers=self.headers, json=data)
        if response.status_code != 200:
            raise APIError(f"Failed to accept share link: {response.text}")
        return response.json()

    async def index_content(self, content_id: UUID) -> Dict:
        if not self._initialized:
            raise ValueError("Client not initialized. Please call set_email first.")
        response = requests.post(f"{self.base_url}/contents/{content_id}/index", headers=self.headers)
        if response.status_code != 200:
            raise APIError(f"Failed to index content: {response.text}")
        return response.json()

    # Entities Router Methods
    async def get_entities(self, entity_type: str, offset: int = 0, limit: int = 20) -> Dict:
        if not self._initialized:
            raise ValueError("Client not initialized. Please call set_email first.")
        params = {
            "offset": offset,
            "limit": limit
        }
        response = requests.get(f"{self.base_url}/entities/{entity_type}", headers=self.headers, params=params)
        if response.status_code != 200:
            raise APIError(f"Failed to get entities: {response.text}")
        return response.json()

    # Chat Router Methods
    async def chat(self, query: str, content_id: Optional[UUID] = None, entity: Optional[str] = None,
                  content_ids: Optional[List[UUID]] = None, entities: Optional[List[str]] = None,
                  thread_id: Optional[str] = None, model: Optional[str] = None,
                  temperature: Optional[float] = None, meta: Optional[dict] = None) -> AsyncGenerator[Dict[str, Any], None]:
        if not self._initialized:
            raise ValueError("Client not initialized. Please call set_email first.")
        if content_id and entity:
            raise ValueError("Cannot specify both content_id and entity")
        if content_ids and entities:
            raise ValueError("Cannot specify both content_ids and entities")
        if (content_id and content_ids) or (entity and entities):
            raise ValueError("Cannot specify both single and multiple values of same type")

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

        async with aiohttp.ClientSession() as session:
            async with session.post(f"{self.base_url}/chat", json=payload, headers=self.headers) as response:
                if response.status != 200:
                    error_text = await response.text()
                    raise APIError(f"Chat request failed: {error_text}")

                async for line in response.content:
                    line = line.decode('utf-8').strip()
                    if line.startswith('data: '):
                        try:
                            data = json.loads(line[6:])
                            yield data
                        except json.JSONDecodeError:
                            continue

    async def edit_chat_message(self, thread_id: str, message_index: int, new_content: str,
                              model: Optional[str] = None, temperature: Optional[float] = None) -> AsyncGenerator[Dict[str, Any], None]:
        if not self._initialized:
            raise ValueError("Client not initialized. Please call set_email first.")

        payload = {
            "thread_id": thread_id,
            "message_index": message_index,
            "new_content": new_content,
            "model": model,
            "temperature": temperature
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(f"{self.base_url}/chat/edit", json=payload, headers=self.headers) as response:
                if response.status != 200:
                    error_text = await response.text()
                    raise APIError(f"Chat edit request failed: {error_text}")

                async for line in response.content:
                    line = line.decode('utf-8').strip()
                    if line.startswith('data: '):
                        try:
                            data = json.loads(line[6:])
                            yield data
                        except json.JSONDecodeError:
                            continue 