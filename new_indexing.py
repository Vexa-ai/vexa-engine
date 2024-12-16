from voyageai import Client as VoyageClient
from typing import List, Dict, Any, Tuple
import pandas as pd
from datetime import datetime
import uuid
from qdrant_client.models import PointStruct

from core import BaseCall, system_msg, user_msg
from pydantic import Field
from vexa import VexaAPI
from bm25_search import ElasticsearchBM25
from qdrant_search import QdrantSearchEngine
from elasticsearch import AsyncElasticsearch
from qdrant_client import AsyncQdrantClient

from pydantic_models import TopicsExtraction

from core import generic_call,system_msg,user_msg
import asyncio


voyage_apikey = 'pa-g4x1yHL4Hv6qBjXcrT2Nhm5gG0srlDhVBp6Op7_IJBQ'

DOCUMENT_CONTEXT_PROMPT = """
<document>
{doc_content}
</document>
"""

CHUNK_CONTEXT_PROMPT = """
Here is the chunk we want to situate within the whole document
<chunk>
{chunk_content}
</chunk>

Please give a short succinct context to situate this chunk within the overall document for the purposes of improving search retrieval of the chunk.
Answer only with the succinct context and nothing else.
"""



async def process_meeting(meeting: Dict, vexa_api, voyage: VoyageClient) -> Tuple[List[Dict], List[PointStruct]]:
    """Process single meeting and return prepared documents for both engines"""
    
    # 1. Get meeting data
    df, formatted_output, start_datetime, speakers, transcript = \
        await vexa_api.get_transcription(meeting_session_id=meeting['id'])
    
    if df.empty:
        return [], []

    # 2. Get topics mapping
    input_text = df[['formatted_time','speaker', 'content']].to_markdown()
    topics_result = await TopicsExtraction.call([
        system_msg(f"Extract topics from the following text: {input_text}"),
        user_msg(input_text)
    ])
    
    # 3. Convert mapping to DataFrame and merge
    topics_df = pd.DataFrame([
        {"formatted_time": m.formatted_time, "topic": m.topic} 
        for m in topics_result.mapping
    ])
    
    # 4. Merge and forward fill topics
    df = df.merge(topics_df, on='formatted_time', how='left')
    df = df[['formatted_time','topic','content','speaker']].fillna(method='ffill')
    
    # 5. Create groups based on speaker + topic changes
    df['speaker_shift'] = (df['speaker']+df['topic'] != df['speaker']+df['topic'].shift(1)).cumsum()
    
    # 6. Group content into chunks
    df_grouped = df.groupby('speaker_shift').agg({
        'formatted_time': 'first',
        'topic': 'first',
        'speaker': 'first',
        'content': ' '.join
    }).reset_index()
    
    # 7. Create chunks with speaker prefix
    chunks = (df_grouped['speaker'] + ': ' + df_grouped['content']).tolist()
    
    # New step: Contextualize chunks
    doc_content = '\n'.join(chunks)
    contextualized_chunks = []
    
    # First chunk - process normally to warm up cache
    messages = [
        system_msg(DOCUMENT_CONTEXT_PROMPT.format(doc_content=doc_content)),
        user_msg(CHUNK_CONTEXT_PROMPT.format(chunk_content=chunks[0]))
    ]
    first_context = await generic_call(messages)
    contextualized_chunks = [f"{first_context}\n{chunks[0]}"]
    
    # Process remaining chunks concurrently
    async def get_context(chunk):
        messages = [
            system_msg(DOCUMENT_CONTEXT_PROMPT.format(doc_content=doc_content)),
            user_msg(CHUNK_CONTEXT_PROMPT.format(chunk_content=chunk))
        ]
        context = await generic_call(messages)
        return f"{context}\n{chunk}"
    
    remaining_contexts = await asyncio.gather(
        *(get_context(chunk) for chunk in chunks[1:])
    )
    contextualized_chunks.extend(remaining_contexts)
    
    # 8. Get embeddings for contextualized chunks
    embeddings = voyage.embed(texts=contextualized_chunks, model='voyage-3')
    
    # 9. Prepare documents for both engines
    es_documents = []
    qdrant_points = []
    
    for i, (chunk, contextualized_chunk, row) in enumerate(zip(chunks, contextualized_chunks, df_grouped.itertuples())):
        timestamp = start_datetime.isoformat() if isinstance(start_datetime, datetime) else start_datetime
        
        # Prepare Elasticsearch document with contextualized content
        es_doc = {
            'content': contextualized_chunk,  # Using contextualized version
            'raw_content': chunk,  # Keep original
            'topic': row.topic,
            'meeting_id': meeting['id'],
            'formatted_time': row.formatted_time,
            'timestamp': timestamp,
            'chunk_index': i
        }
        es_documents.append(es_doc)
        
        # Prepare Qdrant point
        qdrant_point = PointStruct(
            id=str(uuid.uuid4()),
            vector=embeddings.embeddings[i],
            payload={
                'meeting_id': meeting['id'],
                'timestamp': timestamp,
                'formatted_time': row.formatted_time,
                'speaker': row.speaker,
                'content': chunk,  # Keep original content
                'contextualized_content': contextualized_chunk,  # Add contextualized version
                'topic': row.topic,
                'speakers': speakers,
                'chunk_index': i
            }
        )
        qdrant_points.append(qdrant_point)
    
    return es_documents, qdrant_points

async def process_meetings(meetings: List[Dict], vexa_api, voyage_api_key: str, qdrant_engine, es_engine: ElasticsearchBM25):
    """Process meetings and index to both Qdrant and Elasticsearch"""
    voyage = VoyageClient(voyage_api_key)
    
    for meeting in meetings:
        try:
            es_documents, qdrant_points = await process_meeting(meeting, vexa_api, voyage)
            
            if not es_documents:
                continue
                
            # Store in batches of 100
            for i in range(0, len(es_documents), 100):
                batch_es = es_documents[i:i+100]
                batch_qdrant = qdrant_points[i:i+100]
                
                # Index to both engines
                es_engine.index_documents(batch_es)
                await qdrant_engine.client.upsert(
                    collection_name=qdrant_engine.collection_name,
                    points=batch_qdrant
                )
            
            print(f"Processed meeting {meeting['id']} - indexed to both Qdrant and Elasticsearch")
            
        except Exception as e:
            print(f"Error processing meeting {meeting['id']}: {str(e)}")
            continue

async def reset_indices(es_engine: ElasticsearchBM25, qdrant_engine: QdrantSearchEngine):
    # Reset Elasticsearch
    try:
        await es_engine.client.indices.delete(index=es_engine.index_name)
        print(f"Deleted Elasticsearch index: {es_engine.index_name}")
    except Exception as e:
        print(f"Error deleting Elasticsearch index: {str(e)}")
    
    # Recreate ES index - note: create_index is not async
    es_engine.create_index()  # Removed await
    print("Created new Elasticsearch index")
    
    # Reset Qdrant
    try:
        await qdrant_engine.client.delete_collection(collection_name=qdrant_engine.collection_name)
        print(f"Deleted Qdrant collection: {qdrant_engine.collection_name}")
    except Exception as e:
        print(f"Error deleting Qdrant collection: {str(e)}")
    
    # Recreate Qdrant collection
    await qdrant_engine.create_collection()
    print("Created new Qdrant collection")

async def main():
    vexa = VexaAPI()
    qdrant_engine = QdrantSearchEngine(voyage_apikey)
    es_engine = ElasticsearchBM25()
    
    # Reset both indices before processing
    await reset_indices(es_engine, qdrant_engine)
    
    meetings = await vexa.get_meetings()
    await process_meetings(
        meetings=meetings,
        vexa_api=vexa,
        voyage_api_key=voyage_apikey,
        qdrant_engine=qdrant_engine,
        es_engine=es_engine
    )

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())