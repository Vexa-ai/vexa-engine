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
         #   "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
         #   "X-User-Email": email
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
        response = requests.post(f"{self.base_url}/auth/submit_token", json={"token": token})
        if response.status_code != 200:
            raise APIError(f"Token submission failed: {response.text}")
        return response.json()

    # Contents Router Methods
    async def get_contents(self, content_type: Optional[str] = None, filters: Optional[List[Dict]] = None, 
                          offset: int = 0, limit: int = 20, ownership: str = "all", only_archived: bool = False) -> Dict:
        if not self._initialized:
            raise ValueError("Client not initialized. Please call set_email first.")
        params = {
            "content_type": content_type,
            "filters": json.dumps(filters) if filters else None,
            "offset": offset,
            "limit": limit,
            "ownership": ownership,
            "only_archived": only_archived
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
        
        # Convert entities to proper format
        formatted_entities = None
        if entities:
            formatted_entities = []
            for entity in entities:
                if "type" not in entity:
                    raise APIError("Entity type is required")
                if "name" not in entity:
                    raise APIError("Entity name is required")
                formatted_entities.append({
                    "name": entity["name"],
                    "type": entity["type"]
                })
        
        data = {
            "type": content_type,
            "text": body,
            "entities": formatted_entities
        }
        response = requests.post(f"{self.base_url}/contents", headers=self.headers, json=data)
        if response.status_code != 200:
            raise APIError(f"Failed to add content: {response.text}")
        return response.json()

    async def modify_content(self, content_id: UUID, body: str, entities: Optional[List[Dict[str, str]]] = None) -> Dict[str, bool]:
        if not self._initialized:
            raise ValueError("Client not initialized. Please call set_email first.")
        
        # Convert entities to proper format
        formatted_entities = None
        if entities:
            formatted_entities = []
            for entity in entities:
                if "type" not in entity:
                    raise APIError("Entity type is required")
                if "name" not in entity:
                    raise APIError("Entity name is required")
                formatted_entities.append({
                    "name": entity["name"],
                    "type": entity["type"]
                })
        
        data = {
            "text": body,
            "entities": formatted_entities
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

    async def create_share_link(self, access_level: str, content_ids: List[UUID], target_email: Optional[str] = None, 
                              expiration_hours: Optional[int] = None) -> Dict[str, str]:
        if not self._initialized:
            raise ValueError("Client not initialized. Please call set_email first.")
        data = {
            "access_level": access_level,
            "content_ids": content_ids,
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
    async def chat(self, query: str, content_id: Optional[UUID] = None, entity_id: Optional[int] = None,
                  content_ids: Optional[List[UUID]] = None, entity_ids: Optional[List[int]] = None,
                  thread_id: Optional[str] = None, model: Optional[str] = None,
                  temperature: Optional[float] = None, meta: Optional[dict] = None) -> AsyncGenerator[Dict[str, Any], None]:
        if not self._initialized:
            raise ValueError("Client not initialized. Please call set_email first.")
        if content_id and entity_id:
            raise ValueError("Cannot specify both content_id and entity_id")
        if content_ids and entity_ids:
            raise ValueError("Cannot specify both content_ids and entity_ids")
        if (content_id and content_ids) or (entity_id and entity_ids):
            raise ValueError("Cannot specify both single and multiple values of same type")

        payload = {
            "query": query,
            "content_id": str(content_id) if content_id else None,
            "entity_id": entity_id,
            "content_ids": [str(cid) for cid in content_ids] if content_ids else None,
            "entity_ids": entity_ids,
            "thread_id": thread_id,
            "model": model,
            "temperature": temperature,
            "meta": meta
        }
        payload = {k: v for k, v in payload.items() if v is not None}

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(f"{self.base_url}/chat/chat", json=payload, headers=self.headers) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        raise APIError(f"Chat request failed: {error_text}")

                    try:
                        got_data = False
                        async for line in response.content:
                            got_data = True
                            try:
                                line = line.decode('utf-8').strip()
                                if line.startswith('data: '):
                                    try:
                                        data = json.loads(line[6:])
                                        yield data
                                    except json.JSONDecodeError:
                                        continue
                            except UnicodeDecodeError:
                                continue
                        if not got_data:
                            raise APIError("No data received from stream")
                    except (aiohttp.ClientPayloadError, aiohttp.ClientError, TypeError) as e:
                        if isinstance(e, TypeError) and 'does not implement __anext__' not in str(e):
                            raise
                        raise APIError(f"Chat streaming failed: {str(e)}")
        except Exception as e:
            if isinstance(e, APIError):
                raise
            raise APIError(f"Chat request failed: {str(e)}")

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
        payload = {k: v for k, v in payload.items() if v is not None}

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(f"{self.base_url}/chat/edit", json=payload, headers=self.headers) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        raise APIError(f"Chat edit request failed: {error_text}")

                    try:
                        got_data = False
                        async for line in response.content:
                            got_data = True
                            try:
                                line = line.decode('utf-8').strip()
                                if line.startswith('data: '):
                                    try:
                                        data = json.loads(line[6:])
                                        yield data
                                    except json.JSONDecodeError:
                                        continue
                            except UnicodeDecodeError:
                                continue
                    except (aiohttp.ClientPayloadError, aiohttp.ClientError, TypeError) as e:
                        if isinstance(e, TypeError) and 'does not implement __anext__' not in str(e):
                            raise
                        raise APIError(f"Chat edit streaming failed: {str(e)}")
        except Exception as e:
            if isinstance(e, APIError):
                raise
            raise APIError(f"Chat edit request failed: {str(e)}")

    # Thread Router Methods
    async def get_threads(self, content_id: Optional[UUID] = None, entity_id: Optional[int] = None,
                         only_archived: bool = False, limit: int = 50, offset: int = 0,
                         start_date: Optional[datetime] = None, end_date: Optional[datetime] = None) -> Dict:
        if not self._initialized:
            raise ValueError("Client not initialized. Please call set_email first.")
        params = {
            "content_id": str(content_id) if content_id else None,
            "entity_id": entity_id,
            "only_archived": only_archived,
            "limit": limit,
            "offset": offset,
            "start_date": start_date.isoformat() if start_date else None,
            "end_date": end_date.isoformat() if end_date else None
        }
        params = {k: v for k, v in params.items() if v is not None}
        response = requests.get(f"{self.base_url}/threads", headers=self.headers, params=params)
        if response.status_code != 200:
            raise APIError(f"Failed to get threads: {response.text}")
        return response.json()

    async def get_thread(self, thread_id: str) -> Dict:
        if not self._initialized:
            raise ValueError("Client not initialized. Please call set_email first.")
        response = requests.get(f"{self.base_url}/threads/{thread_id}", headers=self.headers)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            raise ValueError("Thread not found")
        else:
            raise APIError(f"Failed to get thread: {response.text}")

    async def archive_thread(self, thread_id: str) -> Dict[str, bool]:
        if not self._initialized:
            raise ValueError("Client not initialized. Please call set_email first.")
        response = requests.post(f"{self.base_url}/threads/{thread_id}/archive", headers=self.headers)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            raise ValueError("Thread not found")
        else:
            raise APIError(f"Failed to archive thread: {response.text}")

    async def restore_thread(self, thread_id: str) -> Dict[str, bool]:
        if not self._initialized:
            raise ValueError("Client not initialized. Please call set_email first.")
        response = requests.post(f"{self.base_url}/threads/{thread_id}/restore", headers=self.headers)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            raise ValueError("Thread not found")
        else:
            raise APIError(f"Failed to restore thread: {response.text}")

    async def rename_thread(self, thread_id: str, thread_name: str) -> Dict[str, bool]:
        if not self._initialized:
            raise ValueError("Client not initialized. Please call set_email first.")
        data = {"thread_name": thread_name}
        response = requests.put(f"{self.base_url}/threads/{thread_id}/rename", headers=self.headers, json=data)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            raise ValueError("Thread not found")
        else:
            raise APIError(f"Failed to rename thread: {response.text}")

    # Prompts Router Methods
    async def create_prompt(self, prompt: str, prompt_type: str, alias: Optional[str] = None) -> Dict[str, int]:
        if not self._initialized:
            raise ValueError("Client not initialized. Please call set_email first.")
        data = {
            "prompt": prompt,
            "type": prompt_type,
            "alias": alias
        }
        response = requests.post(f"{self.base_url}/prompts", headers=self.headers, json=data)
        if response.status_code != 200:
            raise APIError(f"Failed to create prompt: {response.text}")
        return response.json()

    async def create_global_prompt(self, prompt: str, prompt_type: str, alias: Optional[str] = None) -> Dict[str, int]:
        if not self._initialized:
            raise ValueError("Client not initialized. Please call set_email first.")
        data = {
            "prompt": prompt,
            "type": prompt_type,
            "alias": alias
        }
        response = requests.post(f"{self.base_url}/prompts/global", headers=self.headers, json=data)
        if response.status_code != 200:
            raise APIError(f"Failed to create global prompt: {response.text}")
        return response.json()

    async def get_prompts(self, prompt_type: Optional[str] = None) -> List[Dict]:
        if not self._initialized:
            raise ValueError("Client not initialized. Please call set_email first.")
        params = {"prompt_type": prompt_type} if prompt_type else {}
        response = requests.get(f"{self.base_url}/prompts", headers=self.headers, params=params)
        if response.status_code != 200:
            raise APIError(f"Failed to get prompts: {response.text}")
        return response.json()

    async def update_prompt(self, prompt_id: int, prompt: Optional[str] = None, 
                          prompt_type: Optional[str] = None, alias: Optional[str] = None) -> Dict[str, bool]:
        if not self._initialized:
            raise ValueError("Client not initialized. Please call set_email first.")
        data = {
            "prompt": prompt,
            "type": prompt_type,
            "alias": alias
        }
        data = {k: v for k, v in data.items() if v is not None}
        response = requests.put(f"{self.base_url}/prompts/{prompt_id}", headers=self.headers, json=data)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            raise ValueError("Prompt not found")
        else:
            raise APIError(f"Failed to update prompt: {response.text}")

    async def delete_prompt(self, prompt_id: int) -> Dict[str, bool]:
        if not self._initialized:
            raise ValueError("Client not initialized. Please call set_email first.")
        response = requests.delete(f"{self.base_url}/prompts/{prompt_id}", headers=self.headers)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            raise ValueError("Prompt not found")
        else:
            raise APIError(f"Failed to delete prompt: {response.text}") 