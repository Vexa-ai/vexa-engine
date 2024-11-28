import httpx
from dotenv import load_dotenv
import os
from datetime import datetime
import pandas as pd
import asyncio
from google_client import GoogleClient, GoogleError
from typing import Optional
from pydantic import BaseModel
from urllib.parse import urlparse, parse_qs
from psql_models import User, UserToken, async_session, UTMParams
from pytz import timezone
from models import VexaAPIError
import uuid
from sqlalchemy import select
from typing import List, Tuple
from uuid import UUID


VEXA_API_URL = os.getenv('VEXA_API_URL', 'http://127.0.0.1:8001')
API_URL = os.getenv('API_URL', 'http://127.0.0.1:8765')

load_dotenv()

class SessionSpeakerStats(BaseModel):
    meeting_session_id: UUID
    start_timestamp: datetime
    updated_timestamp: datetime
    user_id: UUID
    last_finish: Optional[datetime]
    speakers: List[str]

class VexaAPI:
    def __init__(self, token=os.getenv('VEXA_TOKEN')):
        self.token = token
        print(f"Vexa token: {self.token}")
        self.base_url = VEXA_API_URL
        self.user_info = None
        self.user_id = None
        self.user_name = None


    async def get_meetings(self, offset=None, limit=None, include_total=False):
        url = f"{self.base_url}/api/v1/calls/all"
        params = {
            "token": self.token
        }
        
        # Only add offset and limit to params if they are not None
        if offset is not None:
            params['offset'] = offset
        if limit is not None:
            params['limit'] = limit
        
        print(f"Request URL: {url}")
        print(f"Request Params: {params}")
        
        async with httpx.AsyncClient() as client:
            response = await client.get(url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            return (data['calls'], data.get('total')) if include_total else data['calls']
        else:
            error_message = f"Failed to retrieve data. Status code: {response.status_code}"
            if response.content:
                try:
                    error_details = response.json()
                    error_message += f"\nError details: {error_details}"
                except ValueError:
                    error_message += f"\nError details: {response.text}"
            raise VexaAPIError(error_message)

    async def get_transcription_(self, meeting_id=None, meeting_session_id=None, last_msg_timestamp=None, offset=None, limit=None):

            url = f"{self.base_url}/api/v1/transcription"
            
            params = {
                "meeting_id": meeting_id,
                "meeting_session_id": meeting_session_id,
                "token": self.token
            }
            async with httpx.AsyncClient() as client:
                response = await client.get(url, params=params)
            
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Failed to retrieve data. Status code: {response.status_code}")
                return None

    @staticmethod
    def format_timestamp(timestamp, start_time):
        dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
        time_diff = dt - start_dt
        minutes, seconds = divmod(time_diff.seconds, 60)
        return f"{minutes:02d}:{seconds:02d}", (minutes, seconds)

    async def get_transcription(self, meeting_id=None, meeting_session_id=None, raw=False, use_index=False):
        transcript = await self.get_transcription_(meeting_id=meeting_id, meeting_session_id=meeting_session_id)
        
        if raw:
            return transcript
        
        if not transcript:
            return
        
        # Replace 'TBD' with empty string in transcript data
        for entry in transcript:
            if entry['speaker'] == 'TBD':
                entry['speaker'] = ''
        
        start_time = transcript[0]['timestamp']
        start_datetime = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
        speakers = list(set(entry['speaker'] for entry in transcript if entry['speaker']))
        
        speaker_initials = {}
        for speaker in speakers:
            initials = ''.join([word[0].upper() for word in speaker.split()])
            if initials in speaker_initials.values():
                last_name = speaker.split()[-1]
                initials += last_name[:2].upper()
            speaker_initials[speaker] = initials
        
        df = pd.DataFrame(transcript)
        df['speaker'] = df['speaker'].replace('TBD', '')
        df['formatted_time'], df['time_tuple'] = zip(*df['timestamp'].apply(lambda x: self.format_timestamp(x, start_time)))
        df['initials'] = df['speaker'].map(speaker_initials)
        
        df = df.reset_index()
        df['index'] = df.index
        
        formatted_output = f"Meeting Metadata:\n"
        formatted_output += f"Start Date and Time: {start_datetime.strftime('%Y-%m-%d %H:%M:%S')}\n"
        formatted_output += "Speakers (Always  reference the names EXACTLY as provided with NO changes letter by letter):\n"
        for speaker, initials in speaker_initials.items():
            formatted_output += f"  {initials}: {speaker}\n"
        formatted_output += "\nMeeting Transcript:\n\n"
        
        for _, row in df.iterrows():
            time_or_index = row['index'] if use_index else row['formatted_time']
            formatted_output += f"{row['initials']} ({time_or_index}): {row['content']}\n"
        
        df['chunk_number'] = assign_chunk_numbers(df)
        return df, formatted_output, start_datetime, speakers,transcript
    
    async def get_user_info(self):
        url = f"{self.base_url}/api/v1/users/me"
        params = {
            "token": self.token
        }
        
        async with httpx.AsyncClient() as client:
            response = await client.get(url, params=params)
        
        if response.status_code == 200:
            self.user_info = response.json()
            self.user_id = self.user_info.get('id')
            self.user_name = self.user_info.get('username')
            print("User information retrieved successfully.")
            return self.user_info
        else:
            error_message = f"Failed to retrieve user information. Status code: {response.status_code}"
            if response.content:
                try:
                    error_details = response.json()
                    error_message += f"\nError details: {error_details}"
                except ValueError:
                    error_message += f"\nError details: {response.text}"
            raise VexaAPIError(error_message)

    async def get_meeting_speakers(self, meeting_session_id: UUID) -> dict:
        url = f"{self.base_url}/api/v1/calls/{meeting_session_id}/speakers"
        params = {"token": self.token}
        
        async with httpx.AsyncClient() as client:
            response = await client.get(url, params=params)
            
        if response.status_code == 200:
            return response.json()
        else:
            error_message = f"Failed to retrieve speakers. Status code: {response.status_code}"
            if response.content:
                try:
                    error_details = response.json()
                    error_message += f"\nError details: {error_details}"
                except ValueError:
                    error_message += f"\nError details: {response.text}"
            raise VexaAPIError(error_message)

def assign_chunk_numbers(df, min_chars=300, max_chars=1000):
    cumsum = df['content'].apply(len).cumsum()
    chunk_num = 0
    chunk_start = 0
    chunk_numbers = []
    last_speaker = None

    for i, (total_chars, speaker) in enumerate(zip(cumsum, df['speaker'])):
        current_chunk_size = total_chars - chunk_start
        
        if (current_chunk_size >= min_chars and (speaker != last_speaker or current_chunk_size > max_chars)) or i == len(cumsum) - 1:
            chunk_numbers.extend([chunk_num] * (i - len(chunk_numbers) + 1))
            chunk_num += 1
            chunk_start = total_chars
            last_speaker = speaker

    return chunk_numbers

class VexaAPIError(Exception):
    """Custom exception for Vexa API errors"""
    pass

import requests
import os
from dotenv import load_dotenv
from urllib.parse import urlparse, parse_qs

#auth_url = f"{self.vexa_api_url}/api/v1/auth/default"
#/api/v1/tools/meetings/active
class VexaAuth:
    def __init__(self):
        self.base_url = API_URL
        self.vexa_api_url = VEXA_API_URL
        self.service_token = os.getenv('VEXA_SERVICE_TOKEN')
        self.google_client = GoogleClient()

    async def get_active_meetings(self) -> list:
        url = f"{self.vexa_api_url}/api/v1/tools/meetings/active"
        try:
            async with httpx.AsyncClient(timeout=30.0, verify=False) as client:
                response = await client.get(
                    url, 
                    headers={"Authorization": f"Bearer {self.service_token}"},
                )
                response.raise_for_status()
                return response.json()
        except httpx.ConnectError as e:
            raise VexaAPIError(f"Connection failed to {url}: {str(e)}")
        except httpx.HTTPStatusError as e:
            raise VexaAPIError(f"HTTP error occurred: {e.response.status_code} - {e.response.text}")
        except Exception as e:
            raise VexaAPIError(f"Unexpected error: {str(e)}")

    async def get_user_token(self, email: str = None, user_id: str = None) -> str:
        if not email and not user_id:
            raise VexaAPIError("Either email or user_id must be provided")
        if email and user_id:
            raise VexaAPIError("Provide either email or user_id, not both")
        
        url = f"{self.vexa_api_url}/api/v1/tools/user/token/"
        url += f"id/{user_id}" if user_id else f"{email}"
        params = {"service_token": self.service_token}
        
        try:
            async with httpx.AsyncClient(timeout=30.0, verify=False) as client:
                response = await client.get(url, params=params)
                response.raise_for_status()
                data = response.json()
                return data["token"]
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                identifier = user_id if user_id else email
                raise VexaAPIError(f"User with {'user_id' if user_id else 'email'} {identifier} not found")
            raise VexaAPIError(f"HTTP error occurred: {e.response.status_code} - {e.response.text}")
        except Exception as e:
            raise VexaAPIError(f"Failed to get user token: {str(e)}")

    async def get_default_auth_token(self, email: str, **kwargs) -> str:
        url = f"{self.vexa_api_url}/api/v1/auth/default"
        headers = {"Authorization": f"Bearer {self.service_token}"}
        
        # Build payload from kwargs, excluding None values
        payload = {
            "email": email,
            **{k: v for k, v in kwargs.items() if v is not None}
        }
            
        try:
            async with httpx.AsyncClient(timeout=30.0, verify=False) as client:
                response = await client.post(url, json=payload, headers=headers)
                response.raise_for_status()
                return response.json()["link"]
        except httpx.HTTPStatusError as e:
            raise VexaAPIError(f"HTTP error occurred: {e.response.status_code} - {e.response.text}")
        except Exception as e:
            raise VexaAPIError(f"Failed to get default auth link: {str(e)}")

    async def google_auth(self, token: str, utm_params: dict = None) -> dict:
        try:
            user_info = await self.google_client.get_user_info(token)
            
            async with async_session() as session:
                try:
                    existing_user = await session.execute(
                        select(User).where(User.email == user_info.email)
                    )
                    user = existing_user.scalar_one_or_none()
                    
                    if user:
                        # Update existing user info if needed
                        if (user.username != user_info.name or 
                            user.first_name != user_info.given_name or 
                            user.last_name != user_info.family_name or 
                            user.image != user_info.picture):
                            
                            user.username = user_info.name
                            user.first_name = user_info.given_name
                            user.last_name = user_info.family_name
                            user.image = user_info.picture
                            user.updated_timestamp = datetime.now(timezone('utc'))
                    else:
                        # Create new user
                        user = User(
                            id=uuid.uuid4(),
                            email=user_info.email,
                            username=user_info.name,
                            first_name=user_info.given_name,
                            last_name=user_info.family_name,
                            image=user_info.picture,
                            created_timestamp=datetime.now(timezone('utc')),
                            updated_timestamp=datetime.now(timezone('utc'))
                        )
                        session.add(user)

                    # Handle UTM params
                    if utm_params:
                        utm = UTMParams(
                            id=uuid.uuid4(),
                            user_id=user.id,
                            **{k: v for k, v in utm_params.items() if v is not None},
                            created_at=datetime.now(timezone('utc'))
                        )
                        session.add(utm)

                    await session.flush()

                    # Get auth token
                    auth_link = await self.get_default_auth_token(
                        email=user_info.email,
                        username=user_info.name,
                        first_name=user_info.given_name,
                        last_name=user_info.family_name,
                        image=user_info.picture,
                        **utm_params if utm_params else {}
                    )
                    
                    parsed_url = urlparse(auth_link)
                    query_params = parse_qs(parsed_url.query)
                    vexa_token = query_params.get('__vexa_token', [''])[0]

                    # Check if token already exists
                    existing_token = await session.execute(
                        select(UserToken).where(UserToken.token == vexa_token)
                    )
                    token_obj = existing_token.scalar_one_or_none()
                    
                    user_name = f"{user_info.given_name or ''} {user_info.family_name or ''}".strip()
                    
                    if token_obj:
                        # Update existing token
                        token_obj.user_id = user.id
                        token_obj.user_name = user_name
                        token_obj.last_used_at = datetime.now(timezone('utc'))
                    else:
                        # Create new token
                        token_obj = UserToken(
                            token=vexa_token,
                            user_id=user.id,
                            created_at=datetime.now(timezone('utc')),
                            last_used_at=datetime.now(timezone('utc'))
                        )
                        session.add(token_obj)
                    
                    await session.commit()
                    
                    return {
                        "user_id": str(user.id),
                        "user_name": user_name,
                        "image": user.image,
                        "token": vexa_token
                    }
                    
                except Exception as db_error:
                    await session.rollback()
                    raise VexaAPIError(f"Database operation failed: {str(db_error)}")
                
        except GoogleError as e:
            raise VexaAPIError(f"Google authentication failed: {str(e)}")
        except httpx.HTTPStatusError as e:
            raise VexaAPIError(f"HTTP error occurred: {e.response.status_code} - {e.response.text}")
        except Exception as e:
            raise VexaAPIError(f"Authentication failed: {str(e)}")
        
 


    async def get_speech_stats(self, after_time: Optional[datetime] = None) -> List[SessionSpeakerStats]:
        url = f"{self.vexa_api_url}/api/v1/tools/sessions/stats"
        
        try:
            async with httpx.AsyncClient(timeout=30.0, verify=False) as client:
                params = {}
                if after_time:
                    params["after_time"] = after_time.isoformat()
                    
                response = await client.get(
                    url,
                    params=params,
                    headers={"Authorization": f"Bearer {self.service_token}"}
                )
                response.raise_for_status()
                # Parse response data into SessionSpeakerStats objects
                return [SessionSpeakerStats(**item) for item in response.json()]
        except httpx.HTTPStatusError as e:
            raise VexaAPIError(f"HTTP error occurred: {e.response.status_code} - {e.response.text}")
        except Exception as e:
            raise VexaAPIError(f"Failed to get speech stats: {str(e)}")

