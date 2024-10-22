import pandas as pd
import asyncio
from qdrant_client import QdrantClient, models
from sentence_transformers import SentenceTransformer
from core import generic_call, system_msg, user_msg
from prompts import Prompts
import uuid
import numpy as np
from typing import List, Dict, Any, Tuple
import torch
from datetime import datetime
from typing import Union, Optional
from pydantic_models import QueryPlan

prompts = Prompts()

class VectorSearch:
    def __init__(self, gpu_device=None):
        self.gpu_device = gpu_device
        
        # Set the device for PyTorch
        self.device = f'cuda:{gpu_device}' if gpu_device is not None else 'cpu'
        
        # Initialize models with the specified device
        self.embeddings_model = SentenceTransformer("sentence-transformers/paraphrase-multilingual-mpnet-base-v2")
        self.embeddings_model.to(self.device)
        
        self.qdrant_client = QdrantClient("127.0.0.1", port=6333)
        self.collection_name = "transcript_collection"
        
        if not self.qdrant_client.collection_exists(self.collection_name):
            self.qdrant_client.create_collection(
                collection_name=self.collection_name,
                vectors_config=models.VectorParams(size=768, distance=models.Distance.COSINE),
            )
            
    def count_documents(self, user_id: Optional[str] = None):
        filter_conditions = [
            models.FieldCondition(
                key="type",
                match=models.MatchValue(value="summary")
            )
        ]
        
        if user_id is not None:
            filter_conditions.append(
                models.FieldCondition(
                    key="user_id",
                    match=models.MatchValue(value=user_id)
                )
            )
        
        search_result = self.qdrant_client.count(
            collection_name=self.collection_name,
            count_filter=models.Filter(must=filter_conditions)
        )
        
        return search_result.count
    
    def delete_collection(self):
        self.qdrant_client.delete_collection(self.collection_name)

    def get_embeddings(self, texts: List[str]) -> np.ndarray:
        self.embeddings_model.eval()
        with torch.no_grad():
            embeddings = self.embeddings_model.encode(texts, device=self.device, show_progress_bar=False)
        return embeddings

    async def update_vectorstore_with_qoutes(self, chunks: List[str], points: List[str], qoutes: List[str], start_datetime: pd.Timestamp, speakers: List[str], meeting_session_id: str, user_id: str, user_name: str) -> List[models.PointStruct]:
        points = [
            models.PointStruct(
                id=str(uuid.uuid4()),
                vector=self.embeddings_model.encode(chunk, device=self.device).tolist(),
                payload={
                    "content": chunk,
                    "qoutes": qoute,
                    "point": point,
                    "start_datetime": int(start_datetime.timestamp()),
                    "speakers": speakers,
                    "meeting_session_id": meeting_session_id,
                    "user_id": user_id,
                    "user_name": user_name,
                    "source_type": "meeting",  # Add source_type field
                    "type": "point"
                }
            )
            for chunk, point, qoute in zip(chunks, points, qoutes)
        ]
        
        self.qdrant_client.upsert(
            collection_name=self.collection_name,
            points=points
        )
        return points

    async def add_summary(self, meeting_name: str, summary: str, start_datetime: pd.Timestamp, speakers: List[str], meeting_session_id: str, user_id: str, user_name: str) -> models.PointStruct:
        summary_point = models.PointStruct(
            id=str(uuid.uuid4()),
            vector=self.embeddings_model.encode(summary, device=self.device).tolist(),
            payload={
                "content": summary,
                "meeting_name": meeting_name,
                "start_datetime": int(start_datetime.timestamp()),
                "speakers": speakers,
                "meeting_session_id": meeting_session_id,
                "user_id": user_id,
                "user_name": user_name,
                "source_type": "meeting",  # Add source_type field
                "type": "summary"
            }
        )
        
        self.qdrant_client.upsert(
            collection_name=self.collection_name,
            points=[summary_point]
        )
        return summary_point

    async def search_documents(self, vector_search_query: Optional[str] = None, k: int = 10, include_summary: bool = False, start: Optional[Union[str, datetime]] = None, end: Optional[Union[str, datetime]] = None, user_id: Optional[Union[str, List[str]]] = None, user_name: Optional[Union[str, List[str]]] = None, source_type: Optional[str] = None) -> List[Tuple[Dict[str, Any], float, str]]:
        if not user_id and not user_name:
            raise ValueError("Either user_id or user_name must be provided")

        filter_conditions = []
        
        if not include_summary:
            filter_conditions.append(models.FieldCondition(key="type", match=models.MatchValue(value="point")))
        
        if start:
            filter_conditions.append(models.FieldCondition(
                key="start_datetime",
                range=models.Range(gte=int(start.timestamp()))
            ))
        if end:
            filter_conditions.append(models.FieldCondition(
                key="start_datetime",
                range=models.Range(lte=int(end.timestamp()))
            ))
        
        if user_id:
            if isinstance(user_id, str):
                filter_conditions.append(models.FieldCondition(key="user_id", match=models.MatchValue(value=user_id)))
            else:
                filter_conditions.append(models.FieldCondition(key="user_id", match=models.MatchAny(any=user_id)))
        elif user_name:
            if isinstance(user_name, str):
                filter_conditions.append(models.FieldCondition(key="user_name", match=models.MatchValue(value=user_name)))
            else:
                filter_conditions.append(models.FieldCondition(key="user_name", match=models.MatchAny(any=user_name)))
        
        if source_type:
            filter_conditions.append(models.FieldCondition(key="source_type", match=models.MatchValue(value=source_type)))
        
        search_filter = models.Filter(must=filter_conditions) if filter_conditions else None
        
        if vector_search_query:
            query_vector = self.embeddings_model.encode(vector_search_query, device=self.device).tolist()
            search_result = self.qdrant_client.search(
                collection_name=self.collection_name,
                query_vector=query_vector,
                limit=k,
                query_filter=search_filter,
                with_payload=True,
                with_vectors=False
            )
        else:
            search_result = self.qdrant_client.scroll(
                collection_name=self.collection_name,
                scroll_filter=search_filter,
                limit=k,
                with_payload=True,
                with_vectors=False,
            )[0]
        
        return [(hit.payload, hit.score if hasattr(hit, 'score') else 1.0, hit.id) for hit in search_result]

    async def multi_search_documents(self, queries: List[Dict[str, Any]], k: int = 10, user_id: Optional[Union[str, List[str]]] = None, user_name: Optional[Union[str, List[str]]] = None, source_type: Optional[str] = None) -> List[Dict[str, Any]]:
        if not user_id and not user_name:
            raise ValueError("Either user_id or user_name must be provided")

        search_results = await asyncio.gather(*[
            self.search_documents(
                vector_search_query=query.get('vector_search_query'),
                k=k,
                include_summary=False,
                start=query.get('start'),
                end=query.get('end'),
                user_id=user_id,
                user_name=user_name,
                source_type=source_type
            )
            for query in queries
        ])

        all_results = [item for sublist in search_results for item in sublist]
        
        seen_ids = set()
        deduplicated_results = []

        for doc, score, doc_id in all_results:
            if doc_id not in seen_ids:
                seen_ids.add(doc_id)
                deduplicated_results.append((doc, score, doc_id))

        sorted_results = sorted(deduplicated_results, key=lambda x: x[1], reverse=True)

        return sorted_results

    def get_summaries(self, user_id: Optional[Union[str, List[str]]] = None, user_name: Optional[Union[str, List[str]]] = None, source_type: Optional[str] = None):
        if not user_id and not user_name:
            raise ValueError("Either user_id or user_name must be provided")

        filter_conditions = [
            models.FieldCondition(
                key="type",
                match=models.MatchValue(value="summary")
            )
        ]
        
        if user_id:
            if isinstance(user_id, str):
                filter_conditions.append(models.FieldCondition(key="user_id", match=models.MatchValue(value=user_id)))
            else:
                filter_conditions.append(models.FieldCondition(key="user_id", match=models.MatchAny(any=user_id)))
        elif user_name:
            if isinstance(user_name, str):
                filter_conditions.append(models.FieldCondition(key="user_name", match=models.MatchValue(value=user_name)))
            else:
                filter_conditions.append(models.FieldCondition(key="user_name", match=models.MatchAny(any=user_name)))
        
        if source_type:
            filter_conditions.append(models.FieldCondition(key="source_type", match=models.MatchValue(value=source_type)))
        
        summaries = self.qdrant_client.scroll(
            collection_name=self.collection_name,
            scroll_filter=models.Filter(must=filter_conditions),
            limit=10000,
            with_payload=True,
            with_vectors=False
        )[0]
        summaries = [point.payload for point in summaries]
        
        summaries.sort(key=lambda x: pd.Timestamp(x['start_datetime']))
        
        return summaries

    async def build_context(self, queries, summaries, only_summaries=False, k=20, include_all_summaries=True,user_id: Optional[Union[str, List[str]]] = None, user_name: Optional[Union[str, List[str]]] = None):
        points_with_scores = await self.multi_search_documents(queries=queries, k=k,user_id=user_id,user_name=user_name)
        points_by_meeting = {}
        for point, score, id in points_with_scores:
            if score > 0.1:
                meeting_id = point['meeting_session_id']
                if meeting_id not in points_by_meeting:
                    points_by_meeting[meeting_id] = []
                points_by_meeting[meeting_id].append(point)

        full_context, meeting_ids = build_context_string(summaries, points_by_meeting, only_summaries, include_all_summaries)
        return full_context, meeting_ids

    def check_meeting_session_id_exists(self, meeting_session_id: str) -> bool:
        search_result = self.qdrant_client.scroll(
            collection_name=self.collection_name,
            scroll_filter=models.Filter(
                must=[
                    models.FieldCondition(
                        key="meeting_session_id",
                        match=models.MatchValue(value=meeting_session_id)
                    )
                ]
            ),
            limit=1,
            with_payload=["meeting_session_id"],
            with_vectors=False
        )

        return len(search_result[0]) > 0 and any(
            point.payload.get("meeting_session_id") == meeting_session_id 
            for point in search_result[0]
        )

    async def generate_search_queries(self, query: str,last_n_meetings:int=100,user_id: Optional[Union[str, List[str]]] = None, user_name: Optional[Union[str, List[str]]] = None) -> List[Dict[str, Any]]:
        summaries = self.get_summaries(user_id=user_id, user_name=user_name)[-last_n_meetings:]
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        planner_prompt = """think step by step of which of the following meetings are relevant to the user request. and create many (5+) requests to the search system which will be used to find similar meetings to the user request, 
                            requests should have at least 125 char each and at least 2 sentences."""
        
        plan = await QueryPlan.call([
            system_msg(f"""{planner_prompt}.
                           Now is {now}.
                           Write queries based on user request and general context you have, don't use start and end if timing is not obvious from the user query.
                        """),
            user_msg(f'general context: last {last_n_meetings} meeting summaries in ascending order: {build_context_string(summaries,only_summaries=True)[0]}'),
            user_msg(f'user request: {query}.')
        ])

        # Extract queries from the plan
        queries = plan[0].model_dump()['queries']
        return queries



    def check_meeting_session_id_exists(self, meeting_session_id: str) -> bool:
        search_result = self.qdrant_client.scroll(
            collection_name=self.collection_name,
            scroll_filter=models.Filter(
                must=[
                    models.FieldCondition(
                        key="meeting_session_id",
                        match=models.MatchValue(value=meeting_session_id)
                    )
                ]
            ),
            limit=1,
            with_payload=["meeting_session_id"],
            with_vectors=False
        )

        return len(search_result[0]) > 0 and any(
            point.payload.get("meeting_session_id") == meeting_session_id 
            for point in search_result[0]
        )

    async def get_first_meeting_timestamp(self, user_id: str) -> Optional[str]:
        summaries = self.get_summaries(user_id=user_id)
        if summaries:
            first_meeting = min(summaries, key=lambda x: x['start_datetime'])
            return datetime.fromtimestamp(first_meeting['start_datetime']).strftime('%Y-%m-%d %H:%M:%S')
        return None

    async def get_last_meeting_timestamp(self, user_id: str) -> Optional[str]:
        summaries = self.get_summaries(user_id=user_id)
        if summaries:
            last_meeting = max(summaries, key=lambda x: x['start_datetime'])
            return datetime.fromtimestamp(last_meeting['start_datetime']).strftime('%Y-%m-%d %H:%M:%S')
        return None

    async def remove_user_data(self, user_id: str) -> int:
        filter_condition = models.Filter(
            must=[
                models.FieldCondition(
                    key="user_id",
                    match=models.MatchValue(value=user_id)
                )
            ]
        )

        # First, count the number of points to be deleted
        count_response = self.qdrant_client.count(
            collection_name=self.collection_name,
            count_filter=filter_condition
        )
        points_to_delete = count_response.count

        # Delete the points
        self.qdrant_client.delete(
            collection_name=self.collection_name,
            points_selector=models.FilterSelector(
                filter=filter_condition
            )
        )

        return points_to_delete

# Utility functions
def extract_tag_content(text, tag='ARTICLE'):
    start_tag = f"<{tag}>"
    end_tag = f"</{tag}>"
    results = []
    
    start_index = 0
    while True:
        start_index = text.find(start_tag, start_index)
        if start_index == -1:
            break
        
        end_index = text.find(end_tag, start_index)
        if end_index == -1:
            break
        
        content = text[start_index + len(start_tag):end_index].strip()
        results.append(content)
        
        start_index = end_index + len(end_tag)
    
    if results:
        return results
    else:
        return [f"Error: Content not found. Make sure the content is enclosed in <{tag}> tags."]

async def generate_summary(transcript, output_length=10000, max_tokens=10000):
    return await generic_call(messages=[
        system_msg(f"""create consice summary of the following text as markdown with atention to company FACTS names, people and dates,numbers and facts. All FACTS must be preserved
                    wrap into tags <summary></summary> and <point></point>
                            output should be at most {output_length} characters long, if meeting is long enough. 
                            structure: <summary>sort summary (500 characters),
                            main points as bullets (500 characters each) <point>point content should mention speakers, FACTS, main ideas, consice information </point>"""),
        user_msg(transcript)
        ], model='gpt-4o-mini', temperature=1, max_tokens=max_tokens)
    
    
def build_context_string(summaries, points_by_meeting=None, only_summaries=False, include_all_summaries=True):
    context = []
    meeting_counter = 1
    meeting_ids = []
    for summary in summaries:
        meeting_id = summary['meeting_session_id']
        if include_all_summaries or (points_by_meeting and meeting_id in points_by_meeting):
            start_datetime = pd.to_datetime(datetime.fromtimestamp(summary['start_datetime']))
            context.append(f"# [{meeting_counter}] {summary['meeting_name']} ({start_datetime.strftime('%Y-%m-%d %H:%M')}) participants: {summary['speakers']}\n{summary['content']}")
            
            if not only_summaries and points_by_meeting and meeting_id in points_by_meeting:
                context.append("## Related Quotes:")
                for point in points_by_meeting[meeting_id]:
                    context.append(f"{point['qoutes']}")
            
            context.append("\n---\n")
            meeting_ids.append(meeting_id)
            meeting_counter += 1

    # Join context
    full_context = "\n".join(context)
    return full_context, meeting_ids







