import requests
import json
from datetime import datetime
from typing import Optional, List, Dict, Any, Generator
from vexa import VexaAPI
from psql_helpers import get_token_by_email

class APIClient:
    def __init__(self, email: Optional[str] = None, base_url: str = "http://localhost:8765"):
        self.base_url = base_url
        self.email = email
        self.headers = {"Content-Type": "application/json"}
        self._initialized = False

    @classmethod
    async def create(cls, email: Optional[str] = None, base_url: str = "http://localhost:8765") -> 'APIClient':
        client = cls(base_url=base_url)
        if email:
            await client.set_email(email)
        return client

    async def set_email(self, email: str) -> None:
        self.email = email
        token, _ = await get_token_by_email(email)
        self.token = token
        self.headers["Authorization"] = f"Bearer {token}"
        self.headers["X-User-Email"] = email
        self._initialized = True

    async def _ensure_initialized(self):
        if not self._initialized and self.email:
            await self.set_email(self.email)

    async def get_meetings(self, offset: int = 0, limit: int = 50) -> Dict:
        await self._ensure_initialized()
        return requests.get(f"{self.base_url}/meetings/all", headers=self.headers, 
            params={"offset": offset, "limit": limit}).json()

    def submit_token(self, token: str) -> Dict:
        response = requests.post(f"{self.base_url}/submit_token", json={"token": token})
        result = response.json()
        print(f"User ID: {result.get('user_id')}\nUser Name: {result.get('user_name')}\nImage URL: {result.get('image')}")
        return result

    def chat(self, query: str, thread_id: Optional[str] = None, model: str = "gpt-4o-mini", temperature: float = 0.7) -> Dict:
        response = requests.post(f"{self.base_url}/chat", headers=self.headers, json={
            "query": query, "thread_id": thread_id, "model": model, "temperature": temperature}, stream=True)
        final_response = None
        for line in response.iter_lines():
            if line:
                try:
                    data = json.loads(line.decode('utf-8').replace('data: ', ''))
                    if data.get('type') == 'stream': print(data.get('content', ''), end='', flush=True)
                    elif data.get('type') == 'done': break
                    else: final_response = data
                except json.JSONDecodeError: continue
        return final_response or {"error": "No response received"}

    def global_search(self, query: str, limit: int = 200, min_score: float = 0.4) -> Dict:
        response = requests.post(f"{self.base_url}/search/global", headers=self.headers,
            json={"query": query, "limit": limit, "min_score": min_score})
        if response.status_code != 200:
            raise Exception(f"Search failed: {response.text}")
        return response.json()

    def search_transcripts(self, query: str, meeting_ids: Optional[List[str]] = None, min_score: float = 0.8) -> Dict:
        return requests.post(f"{self.base_url}/search/transcripts", headers=self.headers,
            json={"query": query, "meeting_ids": meeting_ids, "min_score": min_score}).json()

    async def get_meeting_details(self, meeting_id: str) -> Dict:
        await self._ensure_initialized()
        return requests.get(f"{self.base_url}/meeting/{meeting_id}/details", headers=self.headers).json()

    def create_share_link(self, target_email: str, access_level: str = "READ", expiration_hours: int = 24, include_existing: bool = True) -> Dict:
        return requests.post(f"{self.base_url}/share-links", headers=self.headers, json={
            "access_level": access_level, "target_email": target_email,
            "expiration_hours": expiration_hours, "include_existing_meetings": include_existing}).json()

    def accept_share_link(self, token: str, accepting_email: str) -> Dict:
        return requests.post(f"{self.base_url}/share-links/accept", headers=self.headers,
            json={"token": token, "accepting_email": accepting_email}).json()

    def start_indexing(self, num_meetings: int = 200) -> Dict:
        return requests.post(f"{self.base_url}/start_indexing", headers=self.headers,
            json={"num_meetings": num_meetings}).json()

    def get_indexing_status(self) -> Dict:
        return requests.get(f"{self.base_url}/indexing_status", headers=self.headers).json()

    def get_threads(self, meeting_id: Optional[str] = None) -> Dict:
        if meeting_id: 
            return requests.get(f"{self.base_url}/threads/{meeting_id}", headers=self.headers).json()
        return requests.get(f"{self.base_url}/threads", headers=self.headers).json()

    def delete_thread(self, thread_id: str) -> Dict:
        return requests.delete(f"{self.base_url}/thread/{thread_id}", headers=self.headers).json()

    def chat_meeting(self, query: str, meeting_ids: List[str], thread_id: Optional[str] = None, 
                    model: str = "gpt-4o-mini", temperature: float = 0.7) -> Dict:
        response = requests.post(f"{self.base_url}/chat/meeting", headers=self.headers, json={
            "query": query, "meeting_ids": meeting_ids, "thread_id": thread_id,
            "model": model, "temperature": temperature}, stream=True)
        final_response = None
        for line in response.iter_lines():
            if line:
                try:
                    data = json.loads(line.decode('utf-8').replace('data: ', ''))
                    if data.get('type') == 'stream': print(data.get('content', ''), end='', flush=True)
                    elif data.get('type') == 'done': break
                    else: final_response = data
                except json.JSONDecodeError: continue
        return final_response or {"error": "No response received"}

    def get_meetings_by_speakers(self, speakers: List[str], limit: int = 50, offset: int = 0) -> Dict:
        return requests.post(f"{self.base_url}/meetings/by-speakers", headers=self.headers,
            json={"speakers": speakers, "limit": limit, "offset": offset}).json()

    def chat_meeting_summary(self, query: str, meeting_ids: List[str], include_discussion_points: bool = True,
                           thread_id: Optional[str] = None, model: str = "gpt-4o-mini", temperature: float = 0.7) -> Dict:
        response = requests.post(f"{self.base_url}/chat/meeting/summary", headers=self.headers, json={
            "query": query, "meeting_ids": meeting_ids, "include_discussion_points": include_discussion_points,
            "thread_id": thread_id, "model": model, "temperature": temperature}, stream=True)
        final_response = None
        for line in response.iter_lines():
            if line:
                try:
                    data = json.loads(line.decode('utf-8').replace('data: ', ''))
                    if data.get('type') == 'stream': print(data.get('content', ''), end='', flush=True)
                    elif data.get('type') == 'done': break
                    else: final_response = data
                except json.JSONDecodeError: continue
        return final_response or {"error": "No response received"} 