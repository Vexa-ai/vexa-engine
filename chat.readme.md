# Chat System Documentation

## Overview
The chat system provides a unified interface for conversing with content through different access patterns: direct content access, entity-based access, and search-based access. It supports both single-item and multi-item contexts, with automatic fallback to semantic search when content exceeds token limits.

## Core Components

### 1. Content Resolution
Located in `content_resolver.py`, handles merging content from different sources:
```python
async def resolve_content_ids(
    session: AsyncSession,
    content_id: Optional[UUID] = None,
    entity_id: Optional[int] = None,
    content_ids: Optional[List[UUID]] = None,
    entity_ids: Optional[List[int]] = None
) -> List[UUID]
```
- Resolves content IDs from multiple sources
- Uses content_entity_association table for entity relationships
- Deduplicates content IDs
- Returns list of unique content IDs

### 2. Thread Model
Located in `psql_models.py`, stores chat threads with:
- Direct mapping via content_id OR entity_id (mutually exclusive)
- Search scope stored in meta field (content_ids and entity_ids)
- Relationships to User, Content, Entity, and Prompt
- JSONB meta field for flexible metadata storage

### 3. Search Engines

#### QdrantSearchEngine
Semantic search with:
- Content-based filtering
- Voyage embeddings
- Cosine similarity ranking

#### ElasticsearchBM25
Text-based search with:
- Content-based filtering
- BM25 ranking
- English analyzer

#### Hybrid Search
Combines both engines:
- Merges results based on content_id
- Maintains score-based ranking
- Deduplicates by content_id
- Limits results to k items

### 4. Context Providers

#### ContentContextProvider
Direct content access:
- Fetches full content
- Handles different content types
- Automatic token limiting
- Fallback to UnifiedContextProvider

#### UnifiedContextProvider
Search-based access:
- Uses hybrid search
- Content ID mapping for references
- Formats context with timestamps
- Handles empty results

### 5. Chat Manager

#### Base ChatManager
Common chat functionality:
- Thread management
- Message history
- Context integration
- Model interaction

#### UnifiedChatManager
Main chat interface:
- Content resolution
- Access validation
- Context provider selection
- Meta storage
- Output linking

## Flow Diagrams

### Chat Flow
```
1. Input Parameters
   ├── content_id/entity_id (direct mapping)
   └── content_ids/entity_ids (search scope)

2. Content Resolution
   ├── Resolve direct mappings
   ├── Resolve entity relationships
   └── Deduplicate content IDs

3. Access Validation
   ├── Get accessible content
   ├── Filter resolved content
   └── Validate access rights

4. Context Provider Selection
   ├── Check content size
   ├── Select appropriate provider
   └── Fallback if needed

5. Chat Processing
   ├── Get context
   ├── Process messages
   └── Generate response

6. Thread Management
   ├── Store thread mapping
   ├── Update meta
   └── Return response
```

### Search Flow
```
1. Query Input
   ├── User query
   └── Content filters

2. Parallel Search
   ├── Semantic (Qdrant)
   └── Text (Elasticsearch)

3. Result Merging
   ├── Combine results
   ├── Deduplicate
   └── Sort by score

4. Context Formation
   ├── Format timestamps
   ├── Add references
   └── Structure content
```

## Usage Examples

### Golobal Content Chat
```python
await chat_manager.chat(
    user_id=user_id,
    query="What was discussed?",
)

### Direct Content Chat
```python
await chat_manager.chat(
    user_id=user_id,
    query="What was discussed?",
    content_id=some_uuid
)
```

### Entity-Based Chat
```python
await chat_manager.chat(
    user_id=user_id,
    query="What was discussed?",
    entity_id=123
)
```

### Search Scope Chat
```python
await chat_manager.chat(
    user_id=user_id,
    query="Find in these contents",
    content_ids=[uuid1, uuid2],
    entity_ids=[123, 456]
)
```

## Error Handling

### Access Errors
- No access to content
- Mixed access rights
- Invalid content IDs

### Validation Errors
- Mutex constraint violations
- Invalid parameter combinations
- Token limit exceeded

### Search Errors
- No results found
- Search engine failures
- Invalid filters

## Performance Considerations

### Content Resolution
- Efficient entity relationship queries
- Content ID deduplication
- Batch access validation

### Search Optimization
- Content-based filtering
- Result deduplication
- Score-based sorting

### Context Management
- Token limiting
- Automatic fallback
- Context truncation

## Security Considerations

### Access Control
- User-based access validation
- Content-level permissions
- Entity-level permissions

### Data Protection
- No direct ID exposures
- Validated content access
- Secure thread storage

### Input Validation
- Parameter validation
- Content validation
- Query validation

## Implementation Details

### Content Resolution Logic
```python
# 1. Collect all content IDs
all_content_ids = set(content_ids or [])
if content_id:
    all_content_ids.add(content_id)

# 2. Get content from entities
if entity_ids or entity_id:
    ids_to_query = set(entity_ids or [])
    if entity_id:
        ids_to_query.add(entity_id)
    
    # Query association table
    entity_content_ids = await get_entity_content_ids(session, ids_to_query)
    all_content_ids.update(entity_content_ids)

# 3. Validate access
accessible_ids = await validate_access(session, user_id, all_content_ids)
```

### Thread Storage Logic
```python
# 1. Store thread mapping
thread = Thread(
    content_id=content_id,  # Direct mapping
    entity_id=entity_id,    # OR entity mapping
    meta={                  # Search scope
        'content_ids': [str(cid) for cid in content_ids],
        'entity_ids': entity_ids
    }
)

# 2. Validate constraints
assert not (thread.content_id and thread.entity_id), "Cannot have both mappings"

# 3. Store thread
session.add(thread)
await session.commit()
```

### Search Integration Logic
```python
# 1. Get semantic results
semantic_results = await qdrant.search(
    query=query,
    content_ids=accessible_content_ids
)

# 2. Get text results
text_results = await elastic.search(
    query=query,
    content_ids=accessible_content_ids
)

# 3. Merge and deduplicate
merged_results = merge_results(semantic_results, text_results)
```

## Migration Notes

### Database Changes
1. Added entity_id to Thread model
2. Updated constraints
3. Added meta indices
4. Removed old speaker fields

### API Changes
1. Updated chat endpoint parameters
2. Modified response format
3. Added new error types
4. Updated validation logic

### Search Changes
1. Removed speaker filtering
2. Updated content filtering
3. Modified result format
4. Updated merge logic 