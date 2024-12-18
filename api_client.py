import requests
import json
from datetime import datetime
from typing import Optional, List, Dict, Any, Generator
from vexa import VexaAPI
from psql_access import get_token_by_email
from enum import Enum

from uuid import UUID

import os
API_PORT = os.getenv('API_PORT')

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

    def chat(
        self, 
        query: str, 
        meeting_ids: Optional[List[str]] = None,
        speakers: Optional[List[str]] = None,
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
                "meeting_ids": meeting_ids,
                "speakers": speakers,
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