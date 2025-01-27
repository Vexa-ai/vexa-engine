# TODO Tests for Chat System

## Content Resolution Tests (`test_content_resolver.py`)

### Basic Resolution
- [ ] Test single content_id resolution
- [ ] Test single entity_id resolution and content fetching
- [ ] Test multiple content_ids resolution
- [ ] Test multiple entity_ids resolution
- [ ] Test mixed content_id and entity_ids resolution

### Edge Cases
- [ ] Test empty/None inputs handling
- [ ] Test non-existent entity_ids
- [ ] Test duplicate IDs deduplication
- [ ] Test invalid UUID formats
- [ ] Test large number of entity_ids

## Thread Model Tests (`test_thread_model.py`)

### Constraints
- [ ] Test content_id and entity_id mutex constraint
- [ ] Test meta JSONB field with content_ids storage
- [ ] Test meta JSONB field with entity_ids storage
- [ ] Test Entity relationship
- [ ] Test index effectiveness

### Metadata
- [ ] Test meta field updates
- [ ] Test meta field querying
- [ ] Test meta field indexing performance

## Search Engine Tests

### QdrantSearch (`test_qdrant_search.py`)
- [ ] Test content_ids filtering
- [ ] Test search without filters
- [ ] Test invalid content_ids handling
- [ ] Test result format compliance
- [ ] Test search ranking

### ElasticsearchBM25 (`test_es_search.py`)
- [ ] Test content_ids filtering
- [ ] Test search without filters
- [ ] Test invalid content_ids handling
- [ ] Test result format compliance
- [ ] Test search ranking

### Hybrid Search (`test_hybrid_search.py`)
- [ ] Test result merging from both engines
- [ ] Test content_id based deduplication
- [ ] Test score-based sorting
- [ ] Test k-limit enforcement
- [ ] Test empty results handling

## Chat Manager Tests (`test_chat_manager.py`)

### Content Resolution
- [ ] Test different input combinations
- [ ] Test access validation
- [ ] Test token limit handling
- [ ] Test fallback mechanisms

### Meta Storage
- [ ] Test content_ids in meta
- [ ] Test entity_ids in meta
- [ ] Test meta updates in threads
- [ ] Test meta querying

### Context Providers
- [ ] Test ContentContextProvider selection
- [ ] Test UnifiedContextProvider fallback
- [ ] Test token limit handling
- [ ] Test context merging

### Thread Management
- [ ] Test thread creation with content_id
- [ ] Test thread creation with entity_id
- [ ] Test thread meta updates
- [ ] Test thread continuation

### Error Handling
- [ ] Test invalid access scenarios
- [ ] Test invalid model selection
- [ ] Test invalid parameter combinations
- [ ] Test error message clarity

## Integration Tests (`test_integration.py`)

### Chat Flow
- [ ] Test content_id based chat flow
- [ ] Test entity_id based chat flow
- [ ] Test search scope chat flow
- [ ] Test thread continuation
- [ ] Test context linking

### Access Control
- [ ] Test accessible content chat
- [ ] Test inaccessible content chat
- [ ] Test mixed access content chat
- [ ] Test access inheritance

### Performance
- [ ] Test content resolution performance
- [ ] Test search performance with filters
- [ ] Test chat response time
- [ ] Test memory usage

## End-to-End Tests (`test_e2e.py`)

### Complete Flows
- [ ] Test complete chat with content_id
- [ ] Test complete chat with entity_id
- [ ] Test complete chat with search
- [ ] Test thread management
- [ ] Test error recovery

### Real-World Scenarios
- [ ] Test multiple concurrent chats
- [ ] Test long conversations
- [ ] Test large content sets
- [ ] Test complex queries

## Test Infrastructure Needed

### Setup
- [ ] Configure pytest-asyncio
- [ ] Configure pytest-mock
- [ ] Setup Factory Boy
- [ ] Configure pytest-cov

### Fixtures
- [ ] Database fixtures
- [ ] Search engine fixtures
- [ ] Chat manager fixtures
- [ ] Content fixtures

## Test Data Requirements

### Content Data
- [ ] Sample content of different types
- [ ] Content with various lengths
- [ ] Content with special characters
- [ ] Content with embedded references

### Entity Data
- [ ] Different entity types
- [ ] Entity relationships
- [ ] Entity hierarchies
- [ ] Entity metadata

### User Data
- [ ] Different access levels
- [ ] User preferences
- [ ] User history
- [ ] User relationships

### Thread Data
- [ ] Different thread types
- [ ] Thread history
- [ ] Thread metadata
- [ ] Thread relationships 