import pytest
import pytest_asyncio
import uuid
from datetime import datetime, timedelta, timezone
from token_manager import TokenManager
from psql_models import User, UserToken, UserContent, async_session
from sqlalchemy import select, delete
from unittest.mock import patch, MagicMock

@pytest_asyncio.fixture(autouse=True)
async def cleanup_database():
    """Clean up database before and after each test"""
    async with async_session() as session:
        # Clean up in reverse order of dependencies
        await session.execute(delete(UserToken))
        await session.execute(delete(UserContent))
        await session.execute(delete(User))
        await session.commit()
        
    yield
    
    async with async_session() as session:
        # Clean up again after test
        await session.execute(delete(UserToken))
        await session.execute(delete(UserContent))
        await session.execute(delete(User))
        await session.commit()

@pytest.mark.asyncio
async def test_submit_token_new_user():
    token_manager = TokenManager()
    test_token = str(uuid.uuid4())
    test_user_id = str(uuid.uuid4())
    
    mock_user_info = {
        "id": test_user_id,
        "email": "test1@example.com",
        "username": "testuser",
        "first_name": "Test",
        "last_name": "User",
        "image": "test_image.jpg"
    }
    
    with patch('token_manager.VexaAPI') as mock_vexa:
        mock_instance = MagicMock()
        async def mock_get_user_info():
            return mock_user_info
        mock_instance.get_user_info = mock_get_user_info
        mock_vexa.return_value = mock_instance
        
        user_id, user_name, image = await token_manager.submit_token(test_token)
        
        assert user_id == test_user_id
        assert user_name == "Test User"
        assert image == "test_image.jpg"
        
        # Verify database records
        async with async_session() as session:
            user_result = await session.execute(
                select(User).where(User.id == uuid.UUID(test_user_id))
            )
            user = user_result.scalar_one()
            assert user.email == "test1@example.com"
            
            token_result = await session.execute(
                select(UserToken).where(UserToken.token == test_token)
            )
            token_record = token_result.scalar_one()
            assert token_record.user_id == uuid.UUID(test_user_id)

@pytest.mark.asyncio
async def test_submit_token_existing_token():
    token_manager = TokenManager()
    test_token = str(uuid.uuid4())
    test_user = User(
        id=uuid.uuid4(),
        email="test2@example.com",
        first_name="Existing",
        last_name="User"
    )
    
    async with async_session() as session:
        session.add(test_user)
        await session.flush()
        
        token_record = UserToken(
            token=test_token,
            user_id=test_user.id,
            last_used_at=datetime.now(timezone.utc)
        )
        session.add(token_record)
        await session.commit()
    
    user_id, user_name, image = await token_manager.submit_token(test_token)
    
    assert user_id == str(test_user.id)
    assert user_name == "Existing User"

@pytest.mark.asyncio
async def test_check_token():
    token_manager = TokenManager()
    test_token = str(uuid.uuid4())
    test_user = User(
        id=uuid.uuid4(),
        email="test3@example.com",
        first_name="Test",
        last_name="User"
    )
    
    async with async_session() as session:
        session.add(test_user)
        await session.flush()
        
        token_record = UserToken(
            token=test_token,
            user_id=test_user.id,
            last_used_at=datetime.now(timezone.utc)
        )
        session.add(token_record)
        await session.commit()
    
    user_id, user_name = await token_manager.check_token(test_token)
    
    assert user_id == str(test_user.id)
    assert user_name == "Test User"
    
    # Test invalid token
    invalid_user_id, invalid_user_name = await token_manager.check_token("invalid_token")
    assert invalid_user_id is None
    assert invalid_user_name is None

@pytest.mark.asyncio
async def test_get_user_tokens():
    token_manager = TokenManager()
    test_user = User(
        id=uuid.uuid4(),
        email="test4@example.com",
        first_name="Test",
        last_name="User"
    )
    test_tokens = [str(uuid.uuid4()) for _ in range(3)]
    
    async with async_session() as session:
        session.add(test_user)
        await session.flush()
        
        for token in test_tokens:
            token_record = UserToken(
                token=token,
                user_id=test_user.id,
                last_used_at=datetime.now(timezone.utc)
            )
            session.add(token_record)
        await session.commit()
    
    user_tokens = await token_manager.get_user_tokens(test_user.id)
    assert len(user_tokens) == 3
    assert all(token in test_tokens for token in user_tokens)

@pytest.mark.asyncio
async def test_revoke_token():
    token_manager = TokenManager()
    test_token = str(uuid.uuid4())
    test_user = User(
        id=uuid.uuid4(),
        email="test5@example.com",
        first_name="Test",
        last_name="User"
    )
    
    async with async_session() as session:
        session.add(test_user)
        await session.flush()
        
        token_record = UserToken(
            token=test_token,
            user_id=test_user.id,
            last_used_at=datetime.now(timezone.utc)
        )
        session.add(token_record)
        await session.commit()
    
    # Test successful revocation
    success = await token_manager.revoke_token(test_token)
    assert success is True
    
    # Verify token is removed
    async with async_session() as session:
        result = await session.execute(
            select(UserToken).where(UserToken.token == test_token)
        )
        assert result.first() is None
    
    # Test revoking non-existent token
    success = await token_manager.revoke_token("non_existent_token")
    assert success is False

@pytest.mark.asyncio
async def test_revoke_all_user_tokens():
    token_manager = TokenManager()
    test_user = User(
        id=uuid.uuid4(),
        email="test6@example.com",
        first_name="Test",
        last_name="User"
    )
    test_tokens = [str(uuid.uuid4()) for _ in range(3)]
    
    async with async_session() as session:
        session.add(test_user)
        await session.flush()
        
        for token in test_tokens:
            token_record = UserToken(
                token=token,
                user_id=test_user.id,
                last_used_at=datetime.now(timezone.utc)
            )
            session.add(token_record)
        await session.commit()
    
    # Test revoking all tokens
    revoked_count = await token_manager.revoke_all_user_tokens(test_user.id)
    assert revoked_count == 3
    
    # Verify all tokens are removed
    async with async_session() as session:
        result = await session.execute(
            select(UserToken).where(UserToken.user_id == test_user.id)
        )
        assert len(result.scalars().all()) == 0

@pytest.mark.asyncio
async def test_get_user_id():
    token_manager = TokenManager()
    test_token = str(uuid.uuid4())
    test_user = User(
        id=uuid.uuid4(),
        email="test7@example.com",
        first_name="Test",
        last_name="User"
    )
    
    async with async_session() as session:
        session.add(test_user)
        await session.flush()
        
        token_record = UserToken(
            token=test_token,
            user_id=test_user.id,
            last_used_at=datetime.now(timezone.utc)
        )
        session.add(token_record)
        await session.commit()
    
    # Test valid token
    user_id = await token_manager.get_user_id(test_token)
    assert user_id == test_user.id
    
    # Test invalid token
    with pytest.raises(ValueError, match="Invalid token"):
        await token_manager.get_user_id("invalid_token") 