import pytest
import pytest_asyncio
import asyncio
from uuid import UUID
from api_client import APIClient
from app.models.schema.content import (
    ContentType, EntityType, AccessLevel, ContentFilter,
    ContentListRequest, ContentCreate, ContentUpdate
)

TEST_EMAIL = "test@example.com"  # Replace with a real test user email

@pytest_asyncio.fixture
async def api_client():
    client = await APIClient.create(email=TEST_EMAIL)
    yield client

@pytest.mark.asyncio
async def test_content_lifecycle(api_client):
    # 1. Create a document
    doc = await api_client.create_content(
        type=ContentType.DOCUMENT,
        text="Test document",
        entities=[{"name": "test", "type": EntityType.TAG}]
    )
    assert doc["type"] == ContentType.DOCUMENT.value
    doc_id = UUID(doc["content_id"])
    
    # 2. Create a child note
    note = await api_client.create_content(
        type=ContentType.NOTE,
        text="Test note",
        parent_id=doc_id,
        entities=[{"name": "test", "type": EntityType.TAG}]
    )
    assert note["type"] == ContentType.NOTE.value
    note_id = UUID(note["content_id"])
    
    # 3. List contents and verify
    contents = await api_client.get_contents()
    assert len(contents["contents"]) > 0
    assert any(c["content_id"] == str(doc_id) for c in contents["contents"])
    
    # 4. Get child contents
    children = await api_client.get_child_contents(doc_id)
    assert len(children["contents"]) > 0
    assert any(c["content_id"] == str(note_id) for c in children["contents"])
    
    # 5. Update document
    updated = await api_client.update_content(
        doc_id,
        text="Updated test document",
        entities=[
            {"name": "test", "type": EntityType.TAG},
            {"name": "updated", "type": EntityType.TAG}
        ]
    )
    assert "Updated test document" in updated["text"]
    
    # 6. Archive document (should archive child note too)
    await api_client.archive_content(doc_id, archive_children=True)
    
    # 7. Verify archival
    contents = await api_client.get_contents()
    assert not any(c["content_id"] == str(doc_id) for c in contents["contents"])
    assert not any(c["content_id"] == str(note_id) for c in contents["contents"])
    
    # 8. Restore document
    await api_client.restore_content(doc_id)
    
    # 9. Index document
    await api_client.index_content(doc_id)
    
    # 10. Check index status
    status = await api_client.get_index_status(doc_id)
    assert "status" in status

@pytest.mark.asyncio
async def test_content_filtering(api_client):
    # Create test contents with different entities
    doc1 = await api_client.create_content(
        type=ContentType.DOCUMENT,
        text="Test document 1",
        entities=[
            {"name": "tag1", "type": EntityType.TAG},
            {"name": "speaker1", "type": EntityType.SPEAKER}
        ]
    )
    
    doc2 = await api_client.create_content(
        type=ContentType.DOCUMENT,
        text="Test document 2",
        entities=[
            {"name": "tag2", "type": EntityType.TAG},
            {"name": "speaker1", "type": EntityType.SPEAKER}
        ]
    )
    
    # Test filtering by entity type
    speaker_contents = await api_client.get_contents_by_entity(
        entity_type=EntityType.SPEAKER,
        entity_names=["speaker1"]
    )
    assert len(speaker_contents["contents"]) >= 2
    
    # Test filtering by content type
    docs = await api_client.get_contents(
        filter=ContentFilter(content_type=ContentType.DOCUMENT)
    )
    assert len(docs["contents"]) >= 2
    
    # Test combined filtering
    filtered = await api_client.get_contents(
        filter=ContentFilter(
            content_type=ContentType.DOCUMENT,
            entity_type=EntityType.TAG,
            entity_names=["tag1"]
        )
    )
    assert len(filtered["contents"]) >= 1
    assert any(c["content_id"] == doc1["content_id"] for c in filtered["contents"])
    assert not any(c["content_id"] == doc2["content_id"] for c in filtered["contents"])

@pytest.mark.asyncio
async def test_root_and_child_contents(api_client):
    # Create root document
    root = await api_client.create_content(
        type=ContentType.DOCUMENT,
        text="Root document"
    )
    root_id = UUID(root["content_id"])
    
    # Create children
    children = []
    for i in range(3):
        child = await api_client.create_content(
            type=ContentType.NOTE,
            text=f"Child note {i}",
            parent_id=root_id
        )
        children.append(child)
    
    # Get root contents
    roots = await api_client.get_root_contents()
    assert any(c["content_id"] == str(root_id) for c in roots["contents"])
    
    # Get children
    child_contents = await api_client.get_child_contents(root_id)
    assert len(child_contents["contents"]) >= 3
    for child in children:
        assert any(c["content_id"] == child["content_id"] for c in child_contents["contents"])

if __name__ == "__main__":
    asyncio.run(pytest.main([__file__, "-v"])) 