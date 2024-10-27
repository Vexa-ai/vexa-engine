VectorSearchEngine Algorithm Documentation
Overview
VectorSearchEngine is a semantic search system that combines vector embeddings with weighted field matching to find relevant content across structured text data. It uses the SentenceTransformer model for text vectorization and FAISS for efficient similarity search.
Core Components
1. Vector Embedding
The system uses SentenceTransformer (default: 'all-MiniLM-L6-v2') to convert text into dense vector representations (embeddings). It supports both CPU and GPU processing and handles data in batches for memory efficiency.
2. Index Structure
The engine maintains separate FAISS indices for each field type:
topic_name (High-priority field)
summary (Medium-priority field)
details (Low-priority field)
3. Weighting System
Field weights determine the importance of matches in different sections:
topic_name: 1.0 (Full importance)
summary: 0.7 (70% importance)
details: 0.5 (50% importance)
Search Process
1. Query Processing
When a user submits a search query, the system:
Converts the query text to a vector embedding
Initiates similarity search across all field indices
Calculates weighted scores for matches
2. Similarity Calculation
For each field, the system:
1. Converts the query to a vector
Finds similar vectors using FAISS
Calculates similarity scores (score = 1 / (1 + distance))
Applies field weight (weighted_score = score × field_weight)
3. Score Combination
The final score is calculated as:
(Sum of weighted field scores / total weights) + exact match boost
4. Exact Match Boost
Additional score added when exact text matches are found
Boost is weighted by field importance
Default boost value is 0.3 × field_weight
Example Flow
For a search query "machine learning projects":
Vector Creation
The query is converted into a 384-dimensional vector representing its semantic meaning
Per-Field Scoring
Topic Match: (0.85 similarity × 1.0 weight) = 0.85
Summary Match: (0.75 similarity × 0.7 weight) = 0.525
Details Match: (0.60 similarity × 0.5 weight) = 0.30
Combined Score
Base Score = (0.85 + 0.525 + 0.30) / (1.0 + 0.7 + 0.5)
Add Exact Match Boost if found
Results in Final Similarity Score
Features
Batch Processing: Efficiently handles large datasets
Configurable Weights: Allows customization of field importance
Minimum Similarity: Filters out low-relevance results
Speaker Filtering: Can exclude specified speakers
Search Hints: Provides semantic and exact match suggestions
Meeting Grouping: Aggregates results by meeting
Performance Considerations
FAISS Implementation: Enables efficient similarity search
Batch Processing: Manages memory effectively
GPU Acceleration: Utilizes when available
Separate Indices: Maintains flexibility in querying
Memory Management: Includes cleanup for GPU operations
Conclusion
The VectorSearchEngine algorithm effectively balances semantic understanding with exact matching, providing relevant search results across structured content while respecting the relative importance of different text fields. Its weighted approach ensures that matches in more important fields (like titles) are prioritized over matches in less important fields (like detailed content), while still considering all available information.
The combination of vector-based semantic search with traditional exact matching, along with the configurable weighting system, makes it highly adaptable to different types of content and search requirements. The batched processing and efficient indexing ensure good performance even with large datasets.




TASK:
1 fullry replicate the alglrythm using qdrant instead of faiss
self.qdrant_client = AsyncQdrantClient("localhost", port=6333)


2 access data and sync from postgress like


async with async_session() as session:
    # Original query remains the same
    query = select(DiscussionPoint, Meeting, Speaker).join(
        Meeting, DiscussionPoint.meeting_id == Meeting.meeting_id
    ).join(
        Speaker, DiscussionPoint.speaker_id == Speaker.id
    )
    
    result = await session.execute(query)
    rows = result.fetchall()
    
    # Create a dictionary to store all speakers per meeting
    meeting_speakers = {}
    for dp, meeting, speaker in rows:
        if meeting.meeting_id not in meeting_speakers:
            meeting_speakers[meeting.meeting_id] = set()
        meeting_speakers[meeting.meeting_id].add(speaker.name)
    
    # Convert the result to a list of dictionaries with other speakers
    data = []
    for dp, meeting, speaker in rows:
        # Get all speakers except the current one
        other_speakers = list(meeting_speakers[meeting.meeting_id] - {speaker.name})
        
        data.append({
            # Original fields remain the same
            'summary_index': dp.summary_index,
            'summary': dp.summary,
            'details': dp.details,
            'referenced_text': dp.referenced_text,
            'topic_name': dp.topic_name,
            'topic_type': dp.topic_type,
            'meeting_id': meeting.meeting_id,
            'meeting_timestamp': meeting.timestamp,
            'speaker_name': speaker.name,
            # Add new field
            'other_speakers': other_speakers
        })




stick to 127.0.0.1 instead localhost!!!!