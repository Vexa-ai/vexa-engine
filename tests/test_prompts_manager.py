import pytest
import pytest_asyncio
from uuid import uuid4, UUID
from prompts_manager import PromptsManager
from psql_models import PromptTypeEnum, User
from psql_helpers import get_session
from sqlalchemy import text

@pytest_asyncio.fixture(autouse=True)
async def clean_database():
    async with get_session() as session:
        # Clean both tables to ensure fresh state
        await session.execute(text("TRUNCATE TABLE prompts RESTART IDENTITY CASCADE"))
        await session.execute(text("TRUNCATE TABLE users RESTART IDENTITY CASCADE"))
        await session.commit()

# Helper function to insert a dummy user
async def insert_dummy_user(user_id: str):
    async with get_session() as session:
        user = User(id=UUID(user_id), email=f"test_{user_id}@example.com", username="test")
        session.add(user)
        await session.commit()

@pytest.mark.asyncio
async def test_create_and_get_prompts():
    manager = await PromptsManager.create()
    user_id = str(uuid4())
    await insert_dummy_user(user_id)
    
    # Create global prompt using enum value
    global_id = await manager.create_global_prompt("test global prompt", PromptTypeEnum.SPEAKER.value)
    assert global_id is not None
    
    # Create user prompt using enum value
    user_prompt_id = await manager.create_prompt("test user prompt", PromptTypeEnum.SPEAKER.value, user_id)
    assert user_prompt_id is not None
    
    # Get all prompts
    prompts = await manager.get_prompts(user_id)
    assert len(prompts) == 2
    assert any(p.user_id is None for p in prompts)
    assert any(p.user_id == UUID(user_id) for p in prompts)
    
    # Get filtered by type
    speaker_prompts = await manager.get_prompts(user_id, PromptTypeEnum.SPEAKER)
    assert len(speaker_prompts) == 2
    note_prompts = await manager.get_prompts(user_id, PromptTypeEnum.NOTE)
    assert len(note_prompts) == 0

@pytest.mark.asyncio
async def test_get_global_prompts():
    manager = await PromptsManager.create()
    user_id = str(uuid4())
    await insert_dummy_user(user_id)
    global_id1 = await manager.create_global_prompt("test global prompt 1", PromptTypeEnum.SPEAKER.value)
    global_id2 = await manager.create_global_prompt("test global prompt 2", PromptTypeEnum.NOTE.value)
    user_prompt_id = await manager.create_prompt("test user prompt", PromptTypeEnum.SPEAKER.value, user_id)
    global_prompts = await manager.get_global_prompts()
    assert len(global_prompts) == 2
    assert all(p.user_id is None for p in global_prompts)
    speaker_globals = await manager.get_global_prompts(PromptTypeEnum.SPEAKER)
    assert len(speaker_globals) == 1
    assert speaker_globals[0].id == global_id1

@pytest.mark.asyncio
async def test_update_prompt():
    manager = await PromptsManager.create()
    prompt_id = await manager.create_global_prompt("test prompt", PromptTypeEnum.SPEAKER.value, "test_alias")
    assert await manager.update_prompt(prompt_id, "updated text", PromptTypeEnum.NOTE.value, "new_alias")
    prompts = await manager.get_global_prompts()
    assert len(prompts) == 1
    assert prompts[0].prompt == "updated text"
    assert prompts[0].type == PromptTypeEnum.NOTE.value
    assert prompts[0].alias == "new_alias"

@pytest.mark.asyncio
async def test_delete_prompt():
    manager = await PromptsManager.create()
    prompt_id = await manager.create_global_prompt("test prompt", PromptTypeEnum.SPEAKER.value)
    assert await manager.delete_prompt(prompt_id)
    prompts = await manager.get_global_prompts()
    assert len(prompts) == 0

@pytest.mark.asyncio
async def test_invalid_prompt_type():
    manager = await PromptsManager.create()
    with pytest.raises(ValueError) as exc_info:
        await manager.create_prompt("test prompt", "invalid_type")
    assert "Invalid prompt type" in str(exc_info.value)
    assert all(t.value in str(exc_info.value) for t in PromptTypeEnum)

@pytest.mark.asyncio
async def test_create_prompt_with_alias():
    manager = await PromptsManager.create()
    user_id = str(uuid4())
    alias = "test_alias"
    await insert_dummy_user(user_id)
    
    # Create prompt with alias using enum value
    prompt_id = await manager.create_prompt("test prompt with alias", PromptTypeEnum.NOTE.value, user_id, alias)
    assert prompt_id is not None
    
    # Verify prompt was created with correct alias
    prompts = await manager.get_prompts(user_id)
    assert len(prompts) == 1
    assert prompts[0].alias == alias 