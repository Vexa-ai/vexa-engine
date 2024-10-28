import requests
import json

# Base URL for the API
BASE_URL = "http://127.0.0.1:8765"

# Store the token after authentication
TOKEN = "5faa86ba0dc74b14a78c60c8aa20a014"
HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}

# Submit token
def submit_token():
    response = requests.post(
        f"{BASE_URL}/submit_token",
        json={"token": TOKEN}
    )
    print("Submit Token:", response.json())

# Get threads
def get_threads():
    response = requests.get(
        f"{BASE_URL}/threads",
        headers=HEADERS
    )
    print("Threads:", response.json())

# Start a new chat
def start_chat(query,thread_id=None):
    data = {
        "query": query,
        "thread_id": thread_id,  # For a new thread
        "model": "gpt-4o-mini",  # Optional
        "temperature": 0.7  # Optional
    }
    
    # Using Server-Sent Events (SSE)
    response = requests.post(
        f"{BASE_URL}/chat",
        headers=HEADERS,
        json=data,
        stream=True
    )
    
    for line in response.iter_lines():
        if line:
            data = json.loads(line.decode('utf-8').replace('data: ', ''))
            print(data)

# Get meeting timestamps
def get_meeting_timestamps():
    response = requests.get(
        f"{BASE_URL}/user/meetings/timestamps",
        headers=HEADERS
    )
    print("Meeting Timestamps:", response.json())

# Start indexing
def start_indexing():
    data = {
        "num_meetings": 100  # Optional, defaults to 200
    }
    response = requests.post(
        f"{BASE_URL}/start_indexing",
        headers=HEADERS,
        json=data
    )
    print("Indexing Started:", response.json())

# Check indexing status
def check_indexing_status():
    response = requests.get(
        f"{BASE_URL}/indexing_status",
        headers=HEADERS
    )
    print("Indexing Status:", response.json())

# Get number of processed meetings
def get_meetings_processed():
    response = requests.get(
        f"{BASE_URL}/meetings_processed",
        headers=HEADERS
    )
    print("Meetings Processed:", response.json())

# Delete a thread
def delete_thread(thread_id):
    response = requests.delete(
        f"{BASE_URL}/thread/{thread_id}",
        headers=HEADERS
    )
    print("Thread Deleted:", response.json())

# Remove all user data
def remove_user_data():
    response = requests.delete(
        f"{BASE_URL}/user/data",
        headers=HEADERS
    )
    print("User Data Removed:", response.json())
    
def test_indexing_flow():
    # Start indexing
    start_indexing()
    
    # Check status immediately after starting
    check_indexing_status()
    
    # Check number of processed meetings
    get_meetings_processed()