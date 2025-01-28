import pytest
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4
from typing import List
from core import Msg
from thread_manager import ThreadManager, SearchAssistantThread
from psql_models import Thread, async_session
from sqlalchemy import select

@pytest.mark.asyncio
async def test_upsert_thread_create(setup_test_users):
    users = setup_test_users
    test_user, test_token = users[0]
    
    manager = await ThreadManager.create()
    messages = [Msg(role="user", content="test message")]
    
    # Test creating new thread
    thread_id = await manager.upsert_thread(
        user_id=str(test_user.id),
        messages=messages,
        thread_name="Test Thread"
    )
    
    assert thread_id is not None
    
    # Verify in database
    async with async_session() as session:
        result = await session.execute(
            select(Thread).where(Thread.thread_id == thread_id)
        )
        thread = result.scalar_one()
        assert thread.thread_name == "Test Thread"
        assert thread.user_id == test_user.id
        assert not thread.is_archived

@pytest.mark.asyncio
async def test_upsert_thread_update(setup_test_users):
    users = setup_test_users
    test_user, test_token = users[0]
    
    manager = await ThreadManager.create()
    messages = [Msg(role="user", content="test message")]
    
    # Create initial thread
    thread_id = await manager.upsert_thread(
        user_id=str(test_user.id),
        messages=messages,
        thread_name="Initial Name"
    )
    
    # Update thread
    new_messages = messages + [Msg(role="assistant", content="response")]
    updated_thread_id = await manager.upsert_thread(
        user_id=str(test_user.id),
        messages=new_messages,
        thread_id=thread_id,
        thread_name="Updated Name"
    )
    
    assert updated_thread_id == thread_id
    
    # Verify updates in database
    thread = await manager.get_thread(thread_id)
    assert thread is not None
    assert len(thread.messages) == 2
    assert thread.thread_name == "Updated Name"

@pytest.mark.asyncio
async def test_get_user_threads_basic(setup_test_users):
    users = setup_test_users
    test_user, test_token = users[0]
    
    manager = await ThreadManager.create()
    messages = [Msg(role="user", content="test message")]
    
    # Create multiple threads
    threads = []
    for i in range(3):
        thread_id = await manager.upsert_thread(
            user_id=str(test_user.id),
            messages=messages,
            thread_name=f"Thread {i}"
        )
        threads.append(thread_id)
    
    # Get user threads
    result = await manager.get_user_threads(str(test_user.id))
    assert result["total"] >= 3
    assert len(result["threads"]) >= 3
    assert all(t["thread_id"] in threads for t in result["threads"][:3])

@pytest.mark.asyncio
async def test_get_user_threads_pagination(setup_test_users):
    users = setup_test_users
    test_user, test_token = users[0]
    
    manager = await ThreadManager.create()
    messages = [Msg(role="user", content="test message")]
    
    # Create multiple threads
    for i in range(5):
        await manager.upsert_thread(
            user_id=str(test_user.id),
            messages=messages,
            thread_name=f"Thread {i}"
        )
    
    # Test pagination
    page1 = await manager.get_user_threads(str(test_user.id), limit=2, offset=0)
    page2 = await manager.get_user_threads(str(test_user.id), limit=2, offset=2)
    
    assert len(page1["threads"]) == 2
    assert len(page2["threads"]) == 2
    assert page1["threads"][0]["thread_id"] != page2["threads"][0]["thread_id"]

@pytest.mark.asyncio
async def test_get_user_threads_date_filter(setup_test_users):
    users = setup_test_users
    test_user, test_token = users[0]
    
    manager = await ThreadManager.create()
    messages = [Msg(role="user", content="test message")]
    
    # Create thread
    thread_id = await manager.upsert_thread(
        user_id=str(test_user.id),
        messages=messages,
        thread_name="Test Thread"
    )
    
    now = datetime.now(timezone.utc)
    start_date = now - timedelta(hours=1)
    end_date = now + timedelta(hours=1)
    
    # Test date filtering
    result = await manager.get_user_threads(
        str(test_user.id),
        start_date=start_date,
        end_date=end_date
    )
    assert result["total"] >= 1
    assert any(t["thread_id"] == thread_id for t in result["threads"])
    
    # Test future date (should return empty)
    future_start = now + timedelta(days=1)
    future_end = now + timedelta(days=2)
    empty_result = await manager.get_user_threads(
        str(test_user.id),
        start_date=future_start,
        end_date=future_end
    )
    assert empty_result["total"] == 0
    assert len(empty_result["threads"]) == 0

@pytest.mark.asyncio
async def test_archive_unarchive_thread(setup_test_users):
    users = setup_test_users
    test_user, test_token = users[0]
    
    manager = await ThreadManager.create()
    messages = [Msg(role="user", content="test message")]
    
    # Create thread
    thread_id = await manager.upsert_thread(
        user_id=str(test_user.id),
        messages=messages,
        thread_name="Test Thread"
    )
    
    # Archive thread
    success = await manager.archive_thread(thread_id)
    assert success is True
    
    # Verify thread is archived
    archived_threads = await manager.get_user_threads(
        str(test_user.id),
        only_archived=True
    )
    assert any(t["thread_id"] == thread_id for t in archived_threads["threads"])
    
    # Verify thread not in regular threads
    active_threads = await manager.get_user_threads(str(test_user.id))
    assert not any(t["thread_id"] == thread_id for t in active_threads["threads"])
    
    # Unarchive thread
    success = await manager.unarchive_thread(thread_id)
    assert success is True
    
    # Verify thread is active again
    active_threads = await manager.get_user_threads(str(test_user.id))
    assert any(t["thread_id"] == thread_id for t in active_threads["threads"])

@pytest.mark.asyncio
async def test_rename_thread(setup_test_users):
    users = setup_test_users
    test_user, test_token = users[0]
    
    manager = await ThreadManager.create()
    messages = [Msg(role="user", content="test message")]
    
    # Create thread
    thread_id = await manager.upsert_thread(
        user_id=str(test_user.id),
        messages=messages,
        thread_name="Initial Name"
    )
    
    # Rename thread
    success = await manager.rename_thread(thread_id, "New Name")
    assert success is True
    
    # Verify new name
    thread = await manager.get_thread(thread_id)
    assert thread is not None
    assert thread.thread_name == "New Name"
    
    # Test long name truncation
    long_name = "x" * 200
    success = await manager.rename_thread(thread_id, long_name)
    assert success is True
    
    thread = await manager.get_thread(thread_id)
    assert thread is not None
    assert len(thread.thread_name) == 125
    assert thread.thread_name == long_name[:125]

@pytest.mark.asyncio
async def test_get_messages_by_thread_id(setup_test_users):
    users = setup_test_users
    test_user, test_token = users[0]
    
    manager = await ThreadManager.create()
    messages = [
        Msg(role="user", content="test message 1"),
        Msg(role="assistant", content="response 1")
    ]
    
    # Create thread
    thread_id = await manager.upsert_thread(
        user_id=str(test_user.id),
        messages=messages,
        thread_name="Test Thread"
    )
    
    # Get messages
    thread_messages = await manager.get_messages_by_thread_id(thread_id)
    assert thread_messages is not None
    assert len(thread_messages) == 2
    assert thread_messages[0].content == "test message 1"
    assert thread_messages[1].content == "response 1"
    
    # Test nonexistent thread
    invalid_messages = await manager.get_messages_by_thread_id(str(uuid4()))
    assert invalid_messages is None 