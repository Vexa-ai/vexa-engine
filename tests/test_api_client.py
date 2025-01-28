import pytest
import pytest_asyncio
from uuid import UUID
from api_client import APIClient
from psql_models import ContentType, AccessLevel, EntityType, User, UserToken
from unittest.mock import patch, AsyncMock
from google_client import UserInfo
from aiohttp import ClientSession
import asyncio
from typing import List, Tuple

async def async_iter(items):
    for item in items:
        yield item

@pytest_asyncio.fixture
async def api_client():
    client = APIClient(email="test@example.com")
    client._initialized = True
    client.headers = {"Content-Type": "application/json", "Authorization": "Bearer test_token"}
    return client

@pytest_asyncio.fixture
async def setup_test_users() -> List[Tuple[User, UserToken]]:
    users = [
        (User(email="owner@example.com"), UserToken(token="owner_token")),
        (User(email="accepter@example.com"), UserToken(token="accepter_token")),
        (User(email="other@example.com"), UserToken(token="other_token"))
    ]
    return users

@pytest.mark.asyncio
async def test_google_auth(api_client):
    with patch('requests.post') as mock_post, \
         patch('api_client.GoogleClient.get_user_info') as mock_get_user_info:
        
        # Mock responses
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = {
            "token": "test_token",
            "email": "test@example.com"
        }
        
        mock_get_user_info.return_value.email = "test@example.com"
        mock_get_user_info.return_value.name = "Test User"
        mock_get_user_info.return_value.picture = "test_picture_url"

        # Test successful auth
        result = await api_client.google_auth("test_token")
        assert "token" in result
        assert result["token"] == "test_token"
        assert "email" in result
        assert result["email"] == "test@example.com"

@pytest.mark.asyncio
async def test_contents_crud(api_client):
    with patch('requests.post') as mock_post, \
         patch('requests.get') as mock_get, \
         patch('requests.put') as mock_put, \
         patch('requests.delete') as mock_delete:

        # Mock responses
        test_content_id = "550e8400-e29b-41d4-a716-446655440000"
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.side_effect = [
            {"content_id": test_content_id},  # For add_content
            {"success": True},  # For archive_content
            {"success": True}   # For restore_content
        ]

        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {
            "text": "Test content",
            "entities": [{"name": "Test Entity", "type": EntityType.TAG.value}]
        }

        mock_put.return_value.status_code = 200
        mock_put.return_value.json.return_value = {"success": True}

        mock_delete.return_value.status_code = 200
        mock_delete.return_value.json.return_value = {"success": True}

        # Test creating content
        content = await api_client.add_content(
            body="Test content",
            content_type=ContentType.NOTE.value,
            entities=[{"name": "Test Entity", "type": EntityType.TAG.value}]
        )
        assert "content_id" in content
        content_id = UUID(content["content_id"])

        # Test getting content
        content_details = await api_client.get_content(content_id)
        assert "text" in content_details
        assert content_details["text"] == "Test content"
        assert "entities" in content_details
        assert len(content_details["entities"]) == 1
        assert content_details["entities"][0]["name"] == "Test Entity"

        # Test modifying content
        modified = await api_client.modify_content(
            content_id=content_id,
            body="Modified content",
            entities=[{"name": "Modified Entity", "type": EntityType.TAG.value}]
        )
        assert "success" in modified
        assert modified["success"] is True

        # Test archiving content
        archived = await api_client.archive_content(content_id)
        assert "success" in archived
        assert archived["success"] is True

        # Test restoring content
        restored = await api_client.restore_content(content_id)
        assert "success" in restored
        assert restored["success"] is True

        # Test deleting content
        deleted = await api_client.delete_content(content_id)
        assert "success" in deleted
        assert deleted["success"] is True

@pytest.mark.asyncio
async def test_share_links(api_client, setup_test_users):
    with patch('requests.post') as mock_post:
        users = setup_test_users
        owner, owner_token = users[0]
        accepter, accepter_token = users[1]

        # Mock responses
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.side_effect = [
            {"content_id": "550e8400-e29b-41d4-a716-446655440000"},  # For add_content
            {"token": "test_share_token"},  # For create_share_link
            {"success": True}  # For accept_share_link
        ]

        # Create content to share
        content = await api_client.add_content(
            body="Content to share",
            content_type=ContentType.NOTE.value
        )
        content_id = UUID(content["content_id"])

        # Create share link
        share_link = await api_client.create_share_link(
            access_level="read",
            meeting_ids=[content_id],
            target_email=accepter.email
        )
        assert "token" in share_link
        assert share_link["token"] == "test_share_token"

        # Accept share link
        accepted = await api_client.accept_share_link(
            token=share_link["token"],
            accepting_email=accepter.email
        )
        assert "success" in accepted
        assert accepted["success"] is True

@pytest.mark.asyncio
async def test_entities(api_client):
    with patch('requests.get') as mock_get:
        # Mock response
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {
            "entities": [
                {"name": "Test Entity 1", "type": EntityType.TAG.value},
                {"name": "Test Entity 2", "type": EntityType.TAG.value}
            ],
            "total": 2
        }

        # Test getting entities
        result = await api_client.get_entities(EntityType.TAG.value)
        assert "entities" in result
        assert len(result["entities"]) == 2
        assert result["entities"][0]["name"] == "Test Entity 1"
        assert result["entities"][1]["name"] == "Test Entity 2"

@pytest.mark.asyncio
async def test_chat(api_client):
    with patch('aiohttp.ClientSession.post') as mock_post, \
         patch('requests.post') as mock_requests_post:

        # Mock responses for content creation
        mock_requests_post.return_value.status_code = 200
        mock_requests_post.return_value.json.return_value = {"content_id": "550e8400-e29b-41d4-a716-446655440000"}

        # Create content for chat
        content = await api_client.add_content(
            body="Content for chat",
            content_type=ContentType.NOTE.value
        )
        content_id = UUID(content["content_id"])

        # Mock streaming response for chat
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_post.return_value.__aenter__.return_value = mock_response

        # Prepare chat response data
        chat_responses = [
            b'data: {"chunk": "Test response chunk"}\n\n',
            b'data: {"thread_id": "test-thread-id"}\n\n'
        ]

        # Set up the content attribute to yield our test responses
        mock_response.content.iter_any = AsyncMock(return_value=async_iter(chat_responses))

        # Test chat with content - thread is created automatically
        thread_id = None
        async for response in api_client.chat(
            query="Summarize this content",
            content_id=content_id
        ):
            if "thread_id" in response:
                thread_id = response["thread_id"]
                assert thread_id == "test-thread-id"
            else:
                assert "chunk" in response
                assert response["chunk"] == "Test response chunk" 