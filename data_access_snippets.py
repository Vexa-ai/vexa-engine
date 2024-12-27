# %% [markdown]
# # Qdrant Data Access Snippets
# Inspect the latest chunks stored in Qdrant vector database

# %% [markdown]
# ## Setup and Imports

# %%
from qdrant_client import QdrantClient
from datetime import datetime, timedelta
import pandas as pd
pd.set_option('display.max_colwidth', None)

# %% [markdown]
# ## Connect to Qdrant

# %%
def get_qdrant_client():
    """Get Qdrant client with default settings"""
    return QdrantClient(
        host="localhost",  # Update if running in different environment
        port=6333
    )

# %% [markdown]
# ## Query Recent Chunks

# %%
def get_recent_chunks(collection_name="content", limit=10, hours_ago=24):
    """
    Retrieve the most recent chunks from Qdrant.
    
    Args:
        collection_name (str): Name of the Qdrant collection
        limit (int): Maximum number of chunks to retrieve
        hours_ago (int): Look back period in hours
    
    Returns:
        pd.DataFrame: DataFrame containing chunk data
    """
    client = get_qdrant_client()
    
    # Calculate timestamp threshold
    time_threshold = (datetime.utcnow() - timedelta(hours=hours_ago)).isoformat()
    
    # Search with filtering by timestamp
    search_result = client.scroll(
        collection_name=collection_name,
        scroll_filter={"must": [
            {"key": "timestamp", "range": {"gte": time_threshold}}
        ]},
        limit=limit,
        with_payload=True,
        with_vectors=True
    )[0]  # scroll returns (points, next_page_offset)
    
    # Extract relevant fields from payload
    chunks_data = []
    for point in search_result:
        payload = point.payload
        chunks_data.append({
            'id': point.id,
            'timestamp': payload.get('timestamp'),
            'meeting_id': payload.get('meeting_id'),
            'content': payload.get('content'),
            'contextualized_content': payload.get('contextualized_content'),
            'chunk_index': payload.get('chunk_index'),
            'topic': payload.get('topic'),
            'speaker': payload.get('speaker'),
            'speakers': payload.get('speakers'),
            'vector_dim': len(point.vector) if point.vector else None
        })
    
    # Convert to DataFrame
    df = pd.DataFrame(chunks_data)
    
    # Sort by timestamp
    if not df.empty:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp', ascending=False)
    
    return df

# %% [markdown]
# ## Example Usage

# %%
def display_recent_chunks(hours=24, limit=5):
    """Display recent chunks in a readable format"""
    df = get_recent_chunks(hours_ago=hours, limit=limit)
    
    if df.empty:
        print(f"No chunks found in the last {hours} hours")
        return
    
    print(f"Found {len(df)} recent chunks:\n")
    
    for _, row in df.iterrows():
        print("=" * 80)
        print(f"Chunk ID: {row['id']}")
        print(f"Timestamp: {row['timestamp']}")
        print(f"Meeting/Note ID: {row['meeting_id']}")
        print(f"Speaker: {row['speaker']}")
        print(f"Topic: {row['topic']}")
        print(f"\nContent:")
        print(row['content'])
        print(f"\nContextualized Content:")
        print(row['contextualized_content'])
        print(f"\nVector Dimension: {row['vector_dim']}")
        print("=" * 80)
        print()

# %% [markdown]
# ## Run the Analysis
# Uncomment and run the following cells to inspect chunks:

# %%
# Display chunks from last 24 hours
# display_recent_chunks(hours=24, limit=5)

# %%
# Display chunks from last week
# display_recent_chunks(hours=24*7, limit=10)

# %% [markdown]
# ## Advanced Queries

# %%
def analyze_chunk_statistics(hours=24*7):
    """Analyze statistics of recent chunks"""
    df = get_recent_chunks(hours_ago=hours, limit=1000)
    
    if df.empty:
        print(f"No chunks found in the last {hours} hours")
        return
    
    print(f"Chunk Statistics (last {hours} hours):\n")
    
    # Content type distribution
    print("Content Type Distribution:")
    print(df['topic'].value_counts())
    print()
    
    # Speaker distribution
    print("Top Speakers:")
    print(df['speaker'].value_counts().head())
    print()
    
    # Temporal distribution
    df['hour'] = df['timestamp'].dt.hour
    print("Chunks by Hour of Day:")
    print(df['hour'].value_counts().sort_index())
    print()
    
    # Vector dimensions
    print(f"Vector Dimensions: {df['vector_dim'].unique()}")
    print()
    
    # Average content length
    df['content_length'] = df['content'].str.len()
    print(f"Average Content Length: {df['content_length'].mean():.0f} characters")
    print(f"Min Content Length: {df['content_length'].min()}")
    print(f"Max Content Length: {df['content_length'].max()}")

# %%
# Run statistical analysis
# analyze_chunk_statistics(hours=24*7) 