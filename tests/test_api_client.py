import pytest
import pytest_asyncio
from uuid import UUID
from api_client import APIClient, APIError
from psql_models import ContentType, AccessLevel, EntityType, User, UserToken
from unittest.mock import patch, AsyncMock, MagicMock
from google_client import UserInfo
from aiohttp import ClientSession, ClientPayloadError
from aiohttp.http_parser import TransferEncodingError
import asyncio
from typing import List, Tuple

async def async_iter(items):
    for item in items:
        yield item

class MockStreamResponse:
    def __init__(self, responses):
        self.responses = responses
        self.status = 200

    async def __aiter__(self):
        for response in self.responses:
            yield response

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
        assert content_details["entities"][0]["type"] == EntityType.TAG.value

        # Test modifying content with entities
        modified = await api_client.modify_content(
            content_id=content_id,
            body="Modified content",
            entities=[{"name": "Modified Entity", "type": EntityType.SPEAKER.value}]
        )
        assert "success" in modified
        assert modified["success"] is True

        # Verify modified content
        mock_get.return_value.json.return_value = {
            "text": "Modified content",
            "entities": [{"name": "Modified Entity", "type": EntityType.SPEAKER.value}]
        }
        modified_content = await api_client.get_content(content_id)
        assert modified_content["text"] == "Modified content"
        assert len(modified_content["entities"]) == 1
        assert modified_content["entities"][0]["name"] == "Modified Entity"
        assert modified_content["entities"][0]["type"] == EntityType.SPEAKER.value

        # Test invalid content type
        with pytest.raises(APIError, match="Invalid content type"):
            mock_post.return_value.status_code = 400
            mock_post.return_value.text = "Invalid content type"
            await api_client.add_content(
                body="Test content",
                content_type="invalid_type",
                entities=[]
            )

        # Test invalid entity type
        with pytest.raises(APIError, match="Invalid entity type"):
            mock_post.return_value.status_code = 400
            mock_post.return_value.text = "Invalid entity type"
            await api_client.add_content(
                body="Test content",
                content_type=ContentType.NOTE.value,
                entities=[{"name": "Test Entity", "type": "invalid_type"}]
            )

        # Test missing entity name
        with pytest.raises(APIError, match="Entity name is required"):
            mock_post.return_value.status_code = 400
            mock_post.return_value.text = "Entity name is required"
            await api_client.add_content(
                body="Test content",
                content_type=ContentType.NOTE.value,
                entities=[{"type": EntityType.SPEAKER.value}]
            )

        # Test modifying content with invalid entity type
        with pytest.raises(APIError, match="Invalid entity type"):
            mock_put.return_value.status_code = 400
            mock_put.return_value.text = "Invalid entity type"
            await api_client.modify_content(
                content_id=content_id,
                body="Modified content",
                entities=[{"name": "Test Entity", "type": "invalid_type"}]
            )

        # Test modifying content with missing entity name
        with pytest.raises(APIError, match="Entity name is required"):
            mock_put.return_value.status_code = 400
            mock_put.return_value.text = "Entity name is required"
            await api_client.modify_content(
                content_id=content_id,
                body="Modified content",
                entities=[{"type": EntityType.SPEAKER.value}]
            )

        # Reset mock for archive operation
        mock_post.reset_mock()
        mock_post.return_value = MagicMock()
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = {"success": True}

        # Test archiving content
        archived = await api_client.archive_content(content_id)
        assert "success" in archived
        assert archived["success"] is True

        # Reset mock for restore operation
        mock_post.reset_mock()
        mock_post.return_value = MagicMock()
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = {"success": True}

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
            access_level=AccessLevel.TRANSCRIPT.value,
            content_ids=[content_id],
            target_email=accepter.email,
            expiration_hours=24
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
    with patch('aiohttp.ClientSession.post') as mock_post:
        # Test successful chat with chunks and metadata
        mock_response = AsyncMock()
        mock_response.status = 200
        chat_responses = [
            b'data: {"chunk": "Test response chunk 1"}\n\n',
            b'data: {"chunk": "Test response chunk 2"}\n\n',
            b'data: {"thread_id": "test-thread-id", "output": "Complete response", "linked_output": "Complete response with context", "service_content": {"context": "test context"}}\n\n'
        ]
        mock_response.content = MockStreamResponse(chat_responses)
        mock_post.return_value.__aenter__.return_value = mock_response

        responses = []
        async for response in api_client.chat(
            query="Summarize this content",
            content_id=UUID("550e8400-e29b-41d4-a716-446655440000"),
            model="gpt-4o-mini",
            temperature=0.7
        ):
            responses.append(response)

        assert len(responses) == 3
        assert responses[0]["chunk"] == "Test response chunk 1"
        assert responses[1]["chunk"] == "Test response chunk 2"
        assert responses[2]["thread_id"] == "test-thread-id"
        assert responses[2]["output"] == "Complete response"
        assert responses[2]["linked_output"] == "Complete response with context"
        assert "service_content" in responses[2]
        assert responses[2]["service_content"]["context"] == "test context"

        # Test error in response
        error_response = b'data: {"error": "Test error message"}\n\n'
        mock_response.content = MockStreamResponse([error_response])
        
        responses = []
        async for response in api_client.chat(
            query="Summarize this content",
            content_id=UUID("550e8400-e29b-41d4-a716-446655440000")
        ):
            responses.append(response)
            
        assert len(responses) == 1
        assert "error" in responses[0]
        assert responses[0]["error"] == "Test error message"

        # Test HTTP error
        mock_response.status = 400
        mock_response.text = AsyncMock(return_value="Bad request")
        
        with pytest.raises(APIError, match="Chat request failed: Bad request"):
            async for _ in api_client.chat(
                query="Summarize this content",
                content_id=UUID("550e8400-e29b-41d4-a716-446655440000")
            ):
                pass

        # Test TransferEncodingError
        async def error_generator():
            raise ClientPayloadError("Response payload is not completed", TransferEncodingError("Not enough data for satisfy transfer length header."))
            yield

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.content = error_generator()
        mock_post.return_value.__aenter__.return_value = mock_response

        with pytest.raises(APIError, match="Chat streaming failed"):
            async for _ in api_client.chat(
                query="Summarize this content",
                content_id=UUID("550e8400-e29b-41d4-a716-446655440000")
            ):
                pass

        # Test invalid input combinations
        with pytest.raises(ValueError, match="Cannot specify both content_id and entity_id"):
            async for _ in api_client.chat(
                query="test",
                content_id=UUID("550e8400-e29b-41d4-a716-446655440000"),
                entity_id=123
            ):
                pass

        with pytest.raises(ValueError, match="Cannot specify both content_ids and entity_ids"):
            async for _ in api_client.chat(
                query="test",
                content_ids=[UUID("550e8400-e29b-41d4-a716-446655440000")],
                entity_ids=[123]
            ):
                pass

@pytest.mark.asyncio
async def test_threads(api_client):
    with patch('requests.get') as mock_get, \
         patch('requests.post') as mock_post, \
         patch('requests.put') as mock_put:
        
        # Mock responses
        test_thread_id = "test-thread-123"
        test_thread = {
            "id": test_thread_id,
            "name": "Test Thread",
            "user_id": "test-user",
            "created_at": "2024-01-28T12:00:00Z",
            "messages": []
        }
        
        # Mock get_threads response
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {
            "threads": [test_thread],
            "total": 1
        }
        
        # Test get_threads
        threads = await api_client.get_threads(
            limit=10,
            offset=0,
            only_archived=False
        )
        assert "threads" in threads
        assert len(threads["threads"]) == 1
        assert threads["threads"][0]["id"] == test_thread_id
        
        # Mock get_thread response
        mock_get.return_value.json.return_value = test_thread
        
        # Test get_thread
        thread = await api_client.get_thread(test_thread_id)
        assert thread["id"] == test_thread_id
        assert thread["name"] == "Test Thread"
        
        # Mock archive_thread response
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = {"success": True}
        
        # Test archive_thread
        result = await api_client.archive_thread(test_thread_id)
        assert result["success"] is True
        
        # Test restore_thread
        result = await api_client.restore_thread(test_thread_id)
        assert result["success"] is True
        
        # Mock rename_thread response
        mock_put.return_value.status_code = 200
        mock_put.return_value.json.return_value = {"success": True}
        
        # Test rename_thread
        result = await api_client.rename_thread(test_thread_id, "New Thread Name")
        assert result["success"] is True
        
        # Test error cases
        mock_get.return_value.status_code = 404
        with pytest.raises(ValueError, match="Thread not found"):
            await api_client.get_thread("non-existent-thread")
            
        mock_post.return_value.status_code = 404
        with pytest.raises(ValueError, match="Thread not found"):
            await api_client.archive_thread("non-existent-thread")
            
        mock_put.return_value.status_code = 404
        with pytest.raises(ValueError, match="Thread not found"):
            await api_client.rename_thread("non-existent-thread", "New Name")

@pytest.mark.asyncio
async def test_edit_chat_message(api_client):
    with patch('aiohttp.ClientSession.post') as mock_post:
        # Mock streaming response
        mock_response = AsyncMock()
        mock_response.status = 200
        chat_responses = [
            b'data: {"chunk": "Edited chunk 1"}\n\n',
            b'data: {"chunk": "Edited chunk 2"}\n\n',
            b'data: {"thread_id": "test-thread-id", "output": "Complete edited response", "service_content": {}}\n\n'
        ]
        mock_response.content = MockStreamResponse(chat_responses)
        mock_post.return_value.__aenter__.return_value = mock_response

        responses = []
        async for response in api_client.edit_chat_message(
            thread_id="test-thread-id",
            message_index=1,
            new_content="Updated message",
            model="gpt-4o-mini",
            temperature=0.7
        ):
            responses.append(response)

        assert len(responses) == 3
        assert responses[0]["chunk"] == "Edited chunk 1"
        assert responses[1]["chunk"] == "Edited chunk 2"
        assert responses[2]["thread_id"] == "test-thread-id"
        assert responses[2]["output"] == "Complete edited response"
        assert "service_content" in responses[2]

        # Test error handling for TransferEncodingError
        async def error_generator():
            raise ClientPayloadError("Response payload is not completed", TransferEncodingError("Not enough data for satisfy transfer length header."))
            yield

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.content = error_generator()
        mock_post.return_value.__aenter__.return_value = mock_response

        with pytest.raises(APIError) as exc_info:
            async for response in api_client.edit_chat_message(
                thread_id="test-thread-id",
                message_index=1,
                new_content="Updated message",
                model="gpt-4o-mini",
                temperature=0.7
            ):
                pass
        assert "Chat edit streaming failed" in str(exc_info.value)

        # Test invalid response format
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.content = MockStreamResponse([b'invalid data format\n\n'])
        mock_post.return_value.__aenter__.return_value = mock_response
        
        responses = []
        async for response in api_client.edit_chat_message(
            thread_id="test-thread-id",
            message_index=1,
            new_content="Updated message",
            model="gpt-4o-mini",
            temperature=0.7
        ):
            responses.append(response)
        assert len(responses) == 0  # Invalid data should be skipped

        # Test empty response
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.content = MockStreamResponse([])
        mock_post.return_value.__aenter__.return_value = mock_response
        
        responses = []
        async for response in api_client.edit_chat_message(
            thread_id="test-thread-id",
            message_index=1,
            new_content="Updated message",
            model="gpt-4o-mini",
            temperature=0.7
        ):
            responses.append(response)
        assert len(responses) == 0

@pytest.mark.asyncio
async def test_get_archived_contents(api_client):
    with patch('requests.get') as mock_get, \
         patch('requests.post') as mock_post:
        
        # Mock responses
        test_content_id = "550e8400-e29b-41d4-a716-446655440000"
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = {"success": True}
        
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {
            "total": 1,
            "contents": [{
                "content_id": test_content_id,
                "type": ContentType.NOTE.value,
                "text": "Test content",
                "timestamp": "2024-01-01T00:00:00Z",
                "is_indexed": False,
                "is_owner": True,
                "access_level": AccessLevel.REMOVED.value,
                "entities": []
            }]
        }
        
        # Test getting archived contents
        contents = await api_client.get_contents(
            only_archived=True,
            limit=20,
            offset=0
        )
        
        assert "contents" in contents
        assert len(contents["contents"]) == 1
        assert contents["contents"][0]["content_id"] == test_content_id
        assert contents["contents"][0]["access_level"] == AccessLevel.REMOVED.value

        # Test getting non-archived contents
        mock_get.return_value.json.return_value = {
            "total": 0,
            "contents": []
        }
        
        contents = await api_client.get_contents(
            only_archived=False,
            limit=20,
            offset=0
        )
        
        assert "contents" in contents
        assert len(contents["contents"]) == 0 