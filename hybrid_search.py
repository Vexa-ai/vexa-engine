from typing import List, Dict, Any, Optional
from qdrant_search import QdrantSearchEngine
from bm25_search import ElasticsearchBM25

async def hybrid_search(
    query: str,
    qdrant_engine: QdrantSearchEngine,
    es_engine: ElasticsearchBM25,
    meeting_ids: Optional[List[str]] = None,
    speakers: Optional[List[str]] = None,
    k: int = 100,
    semantic_weight: float = 0.8,
    bm25_weight: float = 0.2
) -> List[Dict]:
    """Hybrid search combining semantic and BM25 results using reciprocal rank fusion"""
    
    num_chunks_to_recall = 300  # Retrieve more candidates initially
    
    # Get results from both engines with speaker filtering
    semantic_results = await qdrant_engine.search(
        query, 
        meeting_ids=meeting_ids, 
        speakers=speakers,
        limit=num_chunks_to_recall,
        min_score=0.000
    )
    bm25_results = es_engine.search(
        query, 
        meeting_ids=meeting_ids, 
        speakers=speakers,
        k=num_chunks_to_recall
    )
    
    # Convert to ranked IDs
    ranked_semantic_ids = [
        (result['meeting_id'], result['formatted_time']) 
        for result in semantic_results
    ]
    
    ranked_bm25_ids = [
        (result['meeting_id'], result['formatted_time']) 
        for result in bm25_results
    ]
    
    # Combine unique IDs
    all_chunk_ids = list(set(ranked_semantic_ids + ranked_bm25_ids))
    chunk_id_to_score = {}
    
    # Score using reciprocal rank fusion with weights
    for chunk_id in all_chunk_ids:
        score = 0
        if chunk_id in ranked_semantic_ids:
            index = ranked_semantic_ids.index(chunk_id)
            score += semantic_weight * (1 / (index + 1))
        if chunk_id in ranked_bm25_ids:
            index = ranked_bm25_ids.index(chunk_id)
            score += bm25_weight * (1 / (index + 1))
        chunk_id_to_score[chunk_id] = score
    
    # Sort by combined scores
    sorted_chunk_ids = sorted(
        chunk_id_to_score.keys(),
        key=lambda x: chunk_id_to_score[x],
        reverse=True
    )
    
    # Prepare final results
    final_results = []
    semantic_count = 0
    bm25_count = 0
    
    for chunk_id in sorted_chunk_ids[:k]:
        # Find original result data
        semantic_result = next(
            (r for r in semantic_results 
             if r['meeting_id'] == chunk_id[0] and r['formatted_time'] == chunk_id[1]),
            None
        )
        bm25_result = next(
            (r for r in bm25_results 
             if r['meeting_id'] == chunk_id[0] and r['formatted_time'] == chunk_id[1]),
            None
        )
        
        # Track source counts
        is_from_semantic = semantic_result is not None
        is_from_bm25 = bm25_result is not None
        
        if is_from_semantic and not is_from_bm25:
            semantic_count += 1
        elif is_from_bm25 and not is_from_semantic:
            bm25_count += 1
        else:  # Found in both
            semantic_count += 0.5
            bm25_count += 0.5
        
        # Use semantic result data if available, otherwise BM25
        result_data = semantic_result or bm25_result
        
        final_results.append({
            **result_data,
            'combined_score': chunk_id_to_score[chunk_id],
            'sources': [
                source for source, condition in 
                [('semantic', is_from_semantic), ('bm25', is_from_bm25)]
                if condition
            ]
        })
    
    return {
        'results': final_results,
        'stats': {
            'semantic_percentage': (semantic_count / k) * 100,
            'bm25_percentage': (bm25_count / k) * 100
        }
    }

# Example usage in a notebook: 