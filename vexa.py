import httpx
from dotenv import load_dotenv
import os
from datetime import datetime
import pandas as pd
import asyncio

load_dotenv()

class VexaAPI:
    def __init__(self, token=os.getenv('VEXA_TOKEN')):
        self.token = token
        self.base_url = "http://127.0.0.1:8001/api/v1"
        self.user_info = None
        self.user_id = None
        self.user_name = None


    async def get_meetings(self, offset=None, limit=None):
        url = f"{self.base_url}/calls/all"
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
            return response.json()['calls']
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

            url = f"{self.base_url}/transcription"
            
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
        
        start_time = transcript[0]['timestamp']
        start_datetime = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
        speakers = list(set(entry['speaker'] for entry in transcript if entry['speaker'] != 'TBD'))
        
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
        formatted_output += "Speakers:\n"
        for speaker, initials in speaker_initials.items():
            formatted_output += f"  {initials}: {speaker}\n"
        formatted_output += "\nMeeting Transcript:\n\n"
        
        for _, row in df.iterrows():
            time_or_index = row['index'] if use_index else row['formatted_time']
            formatted_output += f"{row['initials']} ({time_or_index}): {row['content']}\n"
        
        df['chunk_number'] = assign_chunk_numbers(df)
        return df, formatted_output, start_datetime, speakers
    
    async def get_user_info(self):
        url = f"{self.base_url}/users/me"
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
