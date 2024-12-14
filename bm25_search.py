from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from typing import List, Dict, Any
import os
import pandas as pd
from datetime import datetime
from vexa import VexaAPI
class ElasticsearchBM25:
    def __init__(self, index_name: str = "meeting_chunks"):
        self.es_host = os.getenv('ELASTICSEARCH_HOST', 'elasticsearch')
        self.es_port = os.getenv('ELASTICSEARCH_PORT', '9200')
        self.index_name = index_name
        
        try:
            self.es_client = Elasticsearch(f"http://{self.es_host}:{self.es_port}")
            print(f"Connecting to Elasticsearch at {self.es_host}:{self.es_port}")
            self.create_index()
        except Exception as e:
            print(f"Failed to connect to Elasticsearch: {str(e)}")
            print(f"Make sure Elasticsearch is running at {self.es_host}:{self.es_port}")
            # Initialize without creating index - will retry on operations
            self.es_client = None

    def create_index(self):
        if not self.es_client:
            print("No Elasticsearch connection available")
            return
            
        index_settings = {
            "settings": {
                "analysis": {"analyzer": {"default": {"type": "english"}}},
                "similarity": {"default": {"type": "BM25"}},
                "index.queries.cache.enabled": False
            },
            "mappings": {
                "properties": {
                    "content": {"type": "text", "analyzer": "english"},
                    "topic": {"type": "keyword"},
                    "meeting_id": {"type": "keyword"},
                    "formatted_time": {"type": "keyword"},
                    "timestamp": {"type": "date"},
                    "chunk_index": {"type": "integer"}
                }
            }
        }
        
        try:
            if not self.es_client.indices.exists(index=self.index_name):
                self.es_client.indices.create(index=self.index_name, body=index_settings)
                print(f"Created index: {self.index_name}")
        except Exception as e:
            print(f"Error creating index: {str(e)}")

    def index_documents(self, documents: List[Dict[str, Any]]) -> bool:
        if not self.es_client:
            print("No Elasticsearch connection available")
            return False
            
        try:
            actions = [
                {
                    "_index": self.index_name,
                    "_source": {
                        "content": doc["content"],
                        "topic": doc["topic"],
                        "meeting_id": doc["meeting_id"],
                        "formatted_time": doc["formatted_time"],
                        "timestamp": doc["timestamp"],
                        "chunk_index": doc["chunk_index"]
                    }
                }
                for doc in documents
            ]
            success, _ = bulk(self.es_client, actions)
            self.es_client.indices.refresh(index=self.index_name)
            return success
        except Exception as e:
            print(f"Error indexing documents: {str(e)}")
            return False

    def search(self, 
              query: str, 
              meeting_ids: List[str] = None, 
              k: int = 20) -> List[Dict[str, Any]]:
        if not self.es_client:
            print("No Elasticsearch connection available")
            return []
            
        try:
            self.es_client.indices.refresh(index=self.index_name)
            
            must = [
                {
                    "multi_match": {
                        "query": query,
                        "fields": ["content", "topic"]
                    }
                }
            ]
            
            if meeting_ids:
                must.append({
                    "terms": {
                        "meeting_id": meeting_ids
                    }
                })
                
            search_body = {
                "query": {
                    "bool": {
                        "must": must
                    }
                },
                "size": k
            }
            
            response = self.es_client.search(index=self.index_name, body=search_body)
            
            return [
                {
                    "score": hit["_score"],
                    "content": hit["_source"]["content"],
                    "topic": hit["_source"]["topic"],
                    "meeting_id": hit["_source"]["meeting_id"],
                    "formatted_time": hit["_source"]["formatted_time"],
                    "timestamp": hit["_source"]["timestamp"]
                }
                for hit in response["hits"]["hits"]
            ]
        except Exception as e:
            print(f"Error searching: {str(e)}")
            return []

async def hybrid_search(
    query: str,
    qdrant_engine,
    es_engine: ElasticsearchBM25,
    meeting_ids: List[str] = None,
    k: int = 10,
    semantic_weight: float = 0.7,
    bm25_weight: float = 0.3
) -> List[Dict]:
    # Get results from both engines
    semantic_results = await qdrant_engine.search(query, meeting_ids=meeting_ids, limit=k*2)
    bm25_results = es_engine.search(query, meeting_ids=meeting_ids, k=k*2)
    
    # Create dictionaries for scoring
    all_results = {}
    
    # Score semantic results
    for i, result in enumerate(semantic_results):
        key = (result['meeting_id'], result['formatted_time'])
        score = semantic_weight * (1.0 - (i / len(semantic_results)))
        all_results[key] = {
            'data': result,
            'score': score,
            'sources': ['semantic']
        }
    
    # Score BM25 results
    for i, result in enumerate(bm25_results):
        key = (result['meeting_id'], result['formatted_time'])
        bm25_score = bm25_weight * (1.0 - (i / len(bm25_results)))
        
        if key in all_results:
            all_results[key]['score'] += bm25_score
            all_results[key]['sources'].append('bm25')
        else:
            all_results[key] = {
                'data': result,
                'score': bm25_score,
                'sources': ['bm25']
            }
    
    # Sort by combined score
    sorted_results = sorted(
        all_results.values(),
        key=lambda x: x['score'],
        reverse=True
    )[:k]
    
    # Format final results
    return [
        {
            **result['data'],
            'combined_score': result['score'],
            'sources': result['sources']
        }
        for result in sorted_results
    ]

# Usage example:
async def main():
    vexa = VexaAPI()
    es_engine = ElasticsearchBM25()
    
    # Get meetings
    meetings = await vexa.get_meetings()
    
    # Index to Elasticsearch
    await index_to_elasticsearch(meetings, vexa, es_engine)