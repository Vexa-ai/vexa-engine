import requests
import json
from datetime import datetime
from typing import Optional, List, Dict, Any, Generator
from vexa import VexaAPI

BASE_URL = "http://localhost:8765"
vexa = VexaAPI()
headers = {"Authorization": f"Bearer {vexa.token}", "Content-Type": "application/json"}

def submit_token(token: str) -> Dict:
    response = requests.post(f"{BASE_URL}/submit_token", json={"token": token})
    result = response.json()
    print(f"User ID: {result.get('user_id')}\nUser Name: {result.get('user_name')}\nImage URL: {result.get('image')}")
    return result

def chat(query: str, thread_id: Optional[str] = None, model: str = "gpt-4o-mini", temperature: float = 0.7) -> Dict:
    response = requests.post(f"{BASE_URL}/chat", headers=headers, json={
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

def global_search(query: str, limit: int = 200, min_score: float = 0.4) -> Dict:
    """
    Global search across all accessible meetings and their content.
    Returns ranked results with relevance scores in meetings list format.
    
    Args:
        query: Search query string
        limit: Maximum number of results (default: 200)
        min_score: Minimum relevance score threshold (default: 0.4)
        
    Returns:
        Dict with format:
        {
            "total": int,
            "meetings": [
                {
                    "meeting_id": str,
                    "meeting_name": str,
                    "timestamp": str,
                    "is_indexed": bool,
                    "meeting_summary": str,
                    "relevance_score": float,
                    "speakers": List[str]
                },
                ...
            ]
        }
    """
    response = requests.post(
        f"{BASE_URL}/search/global", 
        headers=headers,
        json={
            "query": query,
            "limit": limit,
            "min_score": min_score
        }
    )
    
    if response.status_code != 200:
        raise Exception(f"Search failed: {response.text}")
        
    return response.json()

def search_transcripts(query: str, meeting_ids: Optional[List[str]] = None, min_score: float = 0.8) -> Dict:
    return requests.post(f"{BASE_URL}/search/transcripts", headers=headers,
        json={"query": query, "meeting_ids": meeting_ids, "min_score": min_score}).json()

def get_meetings(offset: int = 0, limit: int = 50) -> Dict:
    return requests.get(f"{BASE_URL}/meetings/all", headers=headers, 
        params={"offset": offset, "limit": limit}).json()

def get_meeting_details(meeting_id: str) -> Dict:
    return requests.get(f"{BASE_URL}/meeting/{meeting_id}/details", headers=headers).json()

def create_share_link(target_email: str, access_level: str = "READ", expiration_hours: int = 24, include_existing: bool = True) -> Dict:
    return requests.post(f"{BASE_URL}/share-links", headers=headers, json={
        "access_level": access_level, "target_email": target_email,
        "expiration_hours": expiration_hours, "include_existing_meetings": include_existing}).json()

def accept_share_link(token: str, accepting_email: str) -> Dict:
    return requests.post(f"{BASE_URL}/share-links/accept", headers=headers,
        json={"token": token, "accepting_email": accepting_email}).json()

def start_indexing(num_meetings: int = 200) -> Dict:
    return requests.post(f"{BASE_URL}/start_indexing", headers=headers,
        json={"num_meetings": num_meetings}).json()

def get_indexing_status() -> Dict:
    return requests.get(f"{BASE_URL}/indexing_status", headers=headers).json()

def get_threads(meeting_id: Optional[str] = None) -> Dict:
    if meeting_id: return requests.get(f"{BASE_URL}/threads/{meeting_id}", headers=headers).json()
    return requests.get(f"{BASE_URL}/threads", headers=headers).json()

def delete_thread(thread_id: str) -> Dict:
    return requests.delete(f"{BASE_URL}/thread/{thread_id}", headers=headers).json()

def chat_meeting(query: str, meeting_ids: List[str], thread_id: Optional[str] = None, 
                model: str = "gpt-4o-mini", temperature: float = 0.7) -> Dict:
    response = requests.post(f"{BASE_URL}/chat/meeting", headers=headers, json={
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

def get_meetings_by_speakers(speakers: List[str], limit: int = 50, offset: int = 0) -> Dict:
    return requests.post(f"{BASE_URL}/meetings/by-speakers", headers=headers,
        json={"speakers": speakers, "limit": limit, "offset": offset}).json()

def chat_meeting_summary(query: str, meeting_ids: List[str], include_discussion_points: bool = True,
                        thread_id: Optional[str] = None, model: str = "gpt-4o-mini", temperature: float = 0.7) -> Dict:
    response = requests.post(f"{BASE_URL}/chat/meeting/summary", headers=headers, json={
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