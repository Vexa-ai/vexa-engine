import pytest
import pytest_asyncio
from datetime import datetime, timezone, timedelta
from uuid import UUID, uuid4
from transcript_manager import TranscriptManager, generate_deterministic_uuid
from psql_models import Content, Transcript, TranscriptAccess, AccessLevel, ContentType, ExternalIDType, UserContent, ContentAccess
from psql_helpers import get_session, async_session
from sqlalchemy import select, delete, and_, func
from vexa import VexaAPI
import json
import os
from unittest.mock import AsyncMock, patch

@pytest_asyncio.fixture(autouse=True)
async def cleanup_transcripts():
    """Clean up any existing test transcripts"""
    async with async_session() as session:
        await session.execute(delete(TranscriptAccess))
        await session.execute(delete(Transcript))
        await session.commit()

@pytest.fixture
def legacy_segments():
    return [
        {
            'content': ' Субтитры сделал DimaTorzok Hi guys!',
            'html_content': None,
            'html_content_short': None,
            'keywords': [],
            'start_timestamp': '2025-02-13 15:32:22.950000+00:00',
            'end_timestamp': '2025-02-13 15:32:42.170000+00:00',
            'confidence': 0.9,
            'segment_id': 123,
            'words': [['Hi', 0.0, 0.5], ['guys', 0.5, 1.0]],
            'speaker': 'Speaker 1',
            'server_timestamp': '2025-02-13 15:32:42.170000+00:00',
            'transcription_timestamp': '2025-02-13 15:32:42.170000+00:00'
        },
        {
            'content': 'Welcome to Ken, and hopefully he will join us.',
            'html_content': None,
            'html_content_short': None,
            'keywords': [],
            'start_timestamp': '2025-02-13 15:32:42.170000+00:00',
            'end_timestamp': '2025-02-13 15:33:02.170000+00:00',
            'confidence': 0.85,
            'segment_id': 124,
            'words': [['Welcome', 0.0, 0.5], ['to', 0.5, 0.7], ['Ken', 0.7, 1.0]],
            'speaker': 'Speaker 2',
            'server_timestamp': '2025-02-13 15:33:02.170000+00:00',
            'transcription_timestamp': '2025-02-13 15:33:02.170000+00:00'
        }
    ]

@pytest.fixture
def upstream_segments():
    return [
        {
            'content': ' я думал ты что-то про ну типа как хранить там и так далее но окей да... что конечно наворочили',
            'start_timestamp': '2025-02-13 15:32:22.950000+00:00',
            'end_timestamp': '2025-02-13 15:32:42.170000+00:00',
            'confidence': 0.85,
            'segment_id': 123,
            'meeting_id': 'the-zdjv-byg',
            'words': [['я', 0.0, 0.5], ['думал', 0.5, 1.0], ['ты', 1.0, 1.5]],
            'speaker': 'Speaker 1',
            'server_timestamp': '2025-02-13 15:32:42.170000+00:00',
            'transcription_timestamp': '2025-02-13 15:32:42.170000+00:00',
            'present_user_ids': ['user1'],
            'partially_present_user_ids': []
        },
        {
            'content': ' а то тебе скоро векса начнет советовать психологов хоть с кем-нибудь поговорить',
            'start_timestamp': '2025-02-13 15:32:42.170000+00:00',
            'end_timestamp': '2025-02-13 15:33:02.170000+00:00',
            'confidence': 0.9,
            'segment_id': 124,
            'meeting_id': 'the-zdjv-byg',
            'words': [['а', 0.0, 0.5], ['то', 0.5, 1.0], ['тебе', 1.0, 1.5]],
            'speaker': 'Speaker 2',
            'server_timestamp': '2025-02-13 15:33:02.170000+00:00',
            'transcription_timestamp': '2025-02-13 15:33:02.170000+00:00',
            'present_user_ids': ['user1', 'user2'],
            'partially_present_user_ids': []
        }
    ]

@pytest.fixture
def invalid_segment():
    return {
        'content': None,
        'start_timestamp': '2025-02-13 15:32:22.950000+00:00',
        'end_timestamp': '2025-02-13 15:32:42.170000+00:00',
        'confidence': -1,
        'segment_id': 123,
        'meeting_id': 'test-meeting',
        'words': None,
        'speaker': None,
        'server_timestamp': '2025-02-13 15:32:42.170000+00:00',
        'transcription_timestamp': '2025-02-13 15:32:42.170000+00:00'
    }

@pytest_asyncio.fixture
async def test_content(setup_test_users):
    users, _ = setup_test_users[0]
    async with get_session() as session:
        content = Content(
            id=uuid4(),
            type=ContentType.MEETING,
            timestamp=datetime.now(timezone.utc)
        )
        session.add(content)
        await session.commit()
        return content

@pytest.fixture
def mock_vexa_api():
    """Mock VexaAPI for testing"""
    async def mock_get_transcription(*args, **kwargs):
        return [{
            "content": "Mocked Vexa API response",
            "timestamp": "2025-02-13T15:32:16.430000+00:00",
            "speaker": "Mocked Speaker",
            "confidence": 0.9,
            "segment_id": 1,
            "words": [["mocked", 0.0, 0.5], ["response", 0.5, 1.0]]
        }]
    
    with patch('vexa.VexaAPI') as mock:
        mock.return_value.get_transcription_ = AsyncMock(side_effect=mock_get_transcription)
        yield mock.return_value

@pytest.fixture
def mock_segment():
    return {
        "content": "Test content",
        "start_timestamp": "2025-02-13 15:32:16.430000+00:00",
        "end_timestamp": "2025-02-13 15:32:22.950000+00:00",
        "speaker": "Test Speaker",
        "confidence": 0.95,
        "segment_id": 1,
        "meeting_id": "test-meeting",
        "words": [["test", 0.0, 0.5], ["content", 0.5, 1.0]],
        "server_timestamp": "2025-02-13 15:32:16+00:00",
        "transcription_timestamp": "2025-02-18 15:03:21.243843+00:00",
        "present_user_ids": ["00000000-0000-4000-a000-000000000001"],
        "partially_present_user_ids": []
    }

@pytest.fixture
def mock_vexa_segment():
    return {
        'content': 'This is a test segment from Vexa API',
        'start_timestamp': '2025-02-13 15:32:22.950000+00:00',
        'end_timestamp': '2025-02-13 15:32:42.170000+00:00',
        'confidence': 0.95,
        'segment_id': 123,
        'meeting_id': 'test-meeting',
        'words': [['This', 0.0, 0.5], ['is', 0.5, 1.0], ['a', 1.0, 1.5]],
        'speaker': 'Speaker 1',
        'server_timestamp': '2025-02-13 15:32:42.170000+00:00',
        'transcription_timestamp': '2025-02-13 15:32:42.170000+00:00',
        'present_user_ids': ['user1', 'user2'],
        'partially_present_user_ids': []
    }

@pytest.fixture
def mock_segments_from_file():
    return [
        {
            'content': 'First segment from file',
            'start_timestamp': '2025-02-13 15:32:22.950000+00:00',
            'end_timestamp': '2025-02-13 15:32:42.170000+00:00',
            'confidence': 0.9,
            'segment_id': 123,
            'meeting_id': 'test-meeting',
            'words': [['First', 0.0, 0.5], ['segment', 0.5, 1.0], ['from', 1.0, 1.5]],
            'speaker': 'Speaker 1',
            'server_timestamp': '2025-02-13 15:32:42.170000+00:00',
            'transcription_timestamp': '2025-02-13 15:32:42.170000+00:00',
            'present_user_ids': ['user1'],
            'partially_present_user_ids': []
        },
        {
            'content': 'Second segment from file',
            'start_timestamp': '2025-02-13 15:32:42.170000+00:00',
            'end_timestamp': '2025-02-13 15:33:02.170000+00:00',
            'confidence': 0.85,
            'segment_id': 124,
            'meeting_id': 'test-meeting',
            'words': [['Second', 0.0, 0.5], ['segment', 0.5, 1.0], ['from', 1.0, 1.5]],
            'speaker': 'Speaker 2',
            'server_timestamp': '2025-02-13 15:33:02.170000+00:00',
            'transcription_timestamp': '2025-02-13 15:33:02.170000+00:00',
            'present_user_ids': ['user1', 'user2'],
            'partially_present_user_ids': []
        }
    ]

class MockTranscript:
    def __init__(self, id, content_id, text_content, start_timestamp, end_timestamp, confidence, original_segment_id, word_timing_data, segment_metadata):
        self.id = id
        self.content_id = content_id
        self.text_content = text_content
        self.start_timestamp = start_timestamp
        self.end_timestamp = end_timestamp
        self.confidence = confidence
        self.original_segment_id = original_segment_id
        self.word_timing_data = word_timing_data
        self.segment_metadata = segment_metadata

def get_mock_session():
    class MockSession:
        def __init__(self):
            self.content = {}
            self.user_content = {}
            self.transcripts = {}
            self.transcript_access = {}
            
        async def __aenter__(self):
            return self
            
        async def __aexit__(self, exc_type, exc_val, exc_tb):
            pass
            
        def add(self, obj):
            if isinstance(obj, Content):
                self.content[obj.id] = obj
                # Create UserContent entry automatically
                if hasattr(obj, '_user_id'):  # Set by _get_or_create_content
                    user_content = UserContent(
                        content_id=obj.id,
                        user_id=obj._user_id,
                        access_level=AccessLevel.OWNER.value,
                        is_owner=True,
                        created_by=obj._user_id
                    )
                    self.user_content[obj.id] = user_content
            elif isinstance(obj, UserContent):
                self.user_content[obj.content_id] = obj
            elif isinstance(obj, Transcript):
                if not obj.id:
                    obj.id = uuid4()
                self.transcripts[obj.id] = obj
            elif isinstance(obj, TranscriptAccess):
                self.transcript_access[obj.transcript_id] = obj
                
        def add_all(self, objs):
            for obj in objs:
                self.add(obj)
                
        async def execute(self, query):
            class Result:
                def __init__(self, data=None):
                    self.data = data
                    
                def scalar_one_or_none(self):
                    return self.data[0] if isinstance(self.data, list) and self.data else self.data
                    
                def scalars(self):
                    class All:
                        def __init__(self, data):
                            self.data = data if isinstance(data, list) else [data] if data else []
                        def all(self):
                            return self.data
                    return All(self.data)
                    
                def all(self):
                    return self.data if isinstance(self.data, list) else []
                    
                def mappings(self):
                    return self.data if isinstance(self.data, list) else []
            
            # Extract query conditions
            content_id = None
            user_id = None
            transcript_id = None
            access_level = None
            timestamp_filter = None

            for criterion in query._where_criteria:
                if hasattr(criterion, 'left') and hasattr(criterion, 'right'):
                    left_str = str(criterion.left).lower()
                    if "content.id" in left_str or "content_id" in left_str:
                        content_id = criterion.right.value
                    elif "user_id" in left_str:
                        user_id = criterion.right.value
                    elif "transcript.id" in left_str:
                        transcript_id = criterion.right.value
                    elif "access_level" in left_str:
                        access_level = criterion.right.value
                    elif "start_timestamp" in left_str and hasattr(criterion, 'operator'):
                        if '>' in str(criterion.operator):
                            timestamp_filter = criterion.right.value

            # Handle Content queries with joins
            if "FROM content" in str(query):
                if content_id in self.content:
                    content = self.content[content_id]
                    if user_id:
                        # Check UserContent access
                        user_content = self.user_content.get(content_id)
                        if user_content and user_content.user_id == user_id:
                            if access_level:
                                if user_content.access_level == access_level:
                                    return Result(content)
                                return Result(None)
                            return Result(content)
                        return Result(None)
                    return Result(content)
                return Result(None)

            # Handle Transcript queries
            if "FROM transcript" in str(query):
                if transcript_id:
                    # Single transcript query
                    transcript = self.transcripts.get(transcript_id)
                    if not transcript:
                        return Result(None)
                        
                    if "join transcript_access" in str(query).lower():
                        # Check TranscriptAccess
                        access = self.transcript_access.get(transcript_id)
                        if access and access.user_id == user_id:
                            if access_level and access.access_level != access_level:
                                return Result(None)
                            return Result(transcript)
                        return Result(None)
                    return Result(transcript)
                else:
                    # List of transcripts query
                    results = []
                    for transcript in self.transcripts.values():
                        if content_id and transcript.content_id == content_id:
                            if timestamp_filter and transcript.start_timestamp <= timestamp_filter:
                                continue
                            results.append(transcript)
                    
                    # Apply ordering if specified
                    if hasattr(query, '_order_by_clauses') and query._order_by_clauses:
                        results.sort(key=lambda x: x.start_timestamp)
                    
                    # Apply limit if specified
                    if hasattr(query, '_limit_clause') and query._limit_clause is not None:
                        limit = query._limit_clause.value
                        results = results[:limit]
                    
                    return Result(results)
            
            return Result(None)
            
        async def commit(self):
            pass
            
        async def rollback(self):
            pass
    
    return MockSession()

@pytest.fixture
def mock_session():
    return get_mock_session()

@pytest.fixture
def mock_segments():
    return [
        {
            "content": "Test content 1",
            "start_timestamp": "2025-02-13T15:32:42.170000+00:00",
            "end_timestamp": "2025-02-13T15:32:45.170000+00:00",
            "confidence": 0.95,
            "segment_id": 1,
            "words": [["test", 0.0, 1.0]],
            "speaker": "Speaker 1",
            "html_content": "<p>Test content 1</p>",
            "html_content_short": "Test content 1",
            "keywords": []
        }
    ]

@pytest.mark.asyncio
async def test_generate_deterministic_uuid():
    external_id = "meeting-123"
    external_id_type = ExternalIDType.GOOGLE_MEET.value
    uuid1 = generate_deterministic_uuid(external_id, external_id_type)
    uuid2 = generate_deterministic_uuid(external_id, external_id_type)
    assert uuid1 == uuid2
    assert isinstance(uuid1, UUID)

@pytest.mark.asyncio
async def test_get_or_create_content(mock_session):
    manager = TranscriptManager()
    external_id = "meeting-123"
    external_id_type = ExternalIDType.GOOGLE_MEET.value
    user_id = UUID('12345678-1234-5678-1234-567812345678')
    
    # Test content creation
    content = await manager._get_or_create_content(external_id, external_id_type, user_id, mock_session)
    assert content.external_id == external_id
    assert content.external_id_type == external_id_type
    assert content.type == ContentType.MEETING.value
    
    # Test content retrieval
    content2 = await manager._get_or_create_content(external_id, external_id_type, user_id, mock_session)
    assert content.id == content2.id

@pytest.mark.asyncio
async def test_ingest_transcript_segments(mock_session, mock_segments):
    manager = TranscriptManager()
    external_id = "meeting-123"
    external_id_type = ExternalIDType.GOOGLE_MEET.value
    user_id = UUID('12345678-1234-5678-1234-567812345678')
    
    # Test successful ingestion
    transcripts = await manager.ingest_transcript_segments(external_id, external_id_type, mock_segments, user_id, mock_session)
    assert len(transcripts) == 1
    assert isinstance(transcripts[0].id, UUID)
    assert transcripts[0].content_id == generate_deterministic_uuid(external_id, external_id_type)
    assert transcripts[0].original_segment_id == mock_segments[0]["segment_id"]
    assert transcripts[0].text_content == mock_segments[0]["content"]
    
    # Test duplicate ingestion
    transcripts2 = await manager.ingest_transcript_segments(external_id, external_id_type, mock_segments, user_id, mock_session)
    assert len(transcripts2) == 1
    assert transcripts2[0].id == transcripts[0].id

@pytest.fixture
async def setup_test_content(setup_test_users):
    user, _ = setup_test_users[0]
    external_id = f"meeting-{uuid4()}"
    external_id_type = ExternalIDType.GOOGLE_MEET.value
    content_id = generate_deterministic_uuid(external_id, external_id_type)

    # Create mock content first
    content = Content(
        id=content_id,
        external_id=external_id,
        external_id_type=external_id_type,
        type=ContentType.MEETING.value,
        text="Test meeting",
        timestamp=datetime.now(timezone.utc),
        last_update=datetime.now(timezone.utc)
    )

    # Create user content separately
    user_content = UserContent(
        content_id=content_id,
        user_id=user.id,
        access_level=AccessLevel.OWNER.value,
        is_owner=True,
        created_by=user.id
    )

    async with get_session() as session:
        session.add(content)
        session.add(user_content)
        await session.commit()

    return user, content_id, external_id, external_id_type

@pytest.mark.asyncio
async def test_get_transcript_segments(setup_test_users):
    user, _ = setup_test_users[0]
    manager = TranscriptManager()
    external_id = f"meeting-{uuid4()}"  # Use unique ID
    external_id_type = ExternalIDType.GOOGLE_MEET.value
    content_id = generate_deterministic_uuid(external_id, external_id_type)

    # Create mock content first
    content = Content(
        id=content_id,
        external_id=external_id,
        external_id_type=external_id_type,
        type=ContentType.MEETING.value,
        text="Test meeting",
        timestamp=datetime.now(timezone.utc),
        last_update=datetime.now(timezone.utc)
    )

    # Create user content separately
    user_content = UserContent(
        content_id=content_id,
        user_id=user.id,
        access_level=AccessLevel.OWNER.value,
        is_owner=True,
        created_by=user.id
    )

    async with get_session() as session:
        session.add(content)
        session.add(user_content)
        await session.commit()

    # Create segments
    segments = [
        {
            "content": "Test content 1",
            "start_timestamp": "2025-02-13T15:32:42.170000+00:00",
            "confidence": 0.95,
            "segment_id": 1,
            "words": [["test", 0.0, 1.0]],
            "speaker": "Speaker 1"
        }
    ]

    # Test ingestion
    transcripts = await manager.ingest_transcript_segments(
        external_id=external_id,
        external_id_type=external_id_type,
        segments=segments,
        user_id=user.id
    )

    assert len(transcripts) == 1
    assert transcripts[0].original_segment_id == segments[0]["segment_id"]
    assert transcripts[0].text_content == segments[0]["content"]

@pytest.mark.asyncio
async def test_get_transcript_segments_with_timestamp(setup_test_users):
    user, _ = setup_test_users[0]
    manager = TranscriptManager()
    external_id = f"meeting-{uuid4()}"  # Use unique ID
    external_id_type = ExternalIDType.GOOGLE_MEET.value
    content_id = generate_deterministic_uuid(external_id, external_id_type)

    # Create mock content first
    content = Content(
        id=content_id,
        external_id=external_id,
        external_id_type=external_id_type,
        type=ContentType.MEETING.value,
        text="Test meeting",
        timestamp=datetime.now(timezone.utc),
        last_update=datetime.now(timezone.utc)
    )

    # Create user content separately
    user_content = UserContent(
        content_id=content_id,
        user_id=user.id,
        access_level=AccessLevel.OWNER.value,
        is_owner=True,
        created_by=user.id
    )

    async with get_session() as session:
        session.add(content)
        session.add(user_content)
        await session.commit()

    # Create segments with different timestamps
    segments = [
        {
            "content": "Test content 1",
            "start_timestamp": "2025-02-13T15:32:42.170000+00:00",
            "confidence": 0.95,
            "segment_id": 1,
            "words": [["test", 0.0, 1.0]],
            "speaker": "Speaker 1"
        },
        {
            "content": "Test content 2",
            "start_timestamp": "2025-02-13T15:33:42.170000+00:00",
            "confidence": 0.95,
            "segment_id": 2,
            "words": [["test", 0.0, 1.0]],
            "speaker": "Speaker 2"
        }
    ]

    # Ingest segments
    await manager.ingest_transcript_segments(
        external_id=external_id,
        external_id_type=external_id_type,
        segments=segments,
        user_id=user.id
    )

    # Test with timestamp filter
    last_msg_timestamp = datetime.fromisoformat("2025-02-13T15:33:00.000000+00:00")
    filtered_segments = await manager.get_transcript_segments(
        external_id=external_id,
        external_id_type=external_id_type,
        user_id=user.id,
        last_msg_timestamp=last_msg_timestamp
    )

    assert len(filtered_segments) == 1
    assert filtered_segments[0]["segment_id"] == segments[1]["segment_id"]

@pytest.mark.asyncio
async def test_get_transcript_segments_limit(setup_test_users):
    user, _ = setup_test_users[0]
    manager = TranscriptManager()
    external_id = f"meeting-{uuid4()}"  # Use unique ID
    external_id_type = ExternalIDType.GOOGLE_MEET.value
    content_id = generate_deterministic_uuid(external_id, external_id_type)

    # Create mock content first
    content = Content(
        id=content_id,
        external_id=external_id,
        external_id_type=external_id_type,
        type=ContentType.MEETING.value,
        text="Test meeting",
        timestamp=datetime.now(timezone.utc),
        last_update=datetime.now(timezone.utc)
    )

    # Create user content separately
    user_content = UserContent(
        content_id=content_id,
        user_id=user.id,
        access_level=AccessLevel.OWNER.value,
        is_owner=True,
        created_by=user.id
    )

    async with get_session() as session:
        session.add(content)
        session.add(user_content)
        await session.commit()

    # Create many segments
    segments = []
    base_time = datetime(2025, 2, 13, 15, 32, 42, tzinfo=timezone.utc)
    for i in range(150):  # More than default limit
        segments.append({
            "content": f"Test content {i}",
            "start_timestamp": (base_time + timedelta(seconds=i)).isoformat(),
            "confidence": 0.95,
            "segment_id": i,
            "words": [["test", 0.0, 1.0]],
            "speaker": f"Speaker {i}"
        })

    # Ingest segments
    await manager.ingest_transcript_segments(
        external_id=external_id,
        external_id_type=external_id_type,
        segments=segments,
        user_id=user.id
    )

    # Test default limit
    results = await manager.get_transcript_segments(
        external_id=external_id,
        external_id_type=external_id_type,
        user_id=user.id
    )

    assert len(results) == 100  # Default limit
    # Verify ordering
    for i in range(len(results) - 1):
        assert results[i]["timestamp"] < results[i + 1]["timestamp"] 