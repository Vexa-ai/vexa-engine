import pandas as pd
import asyncio
from qdrant_client import QdrantClient, models
from sentence_transformers import SentenceTransformer, CrossEncoder
from core import generic_call, system_msg, user_msg, assistant_msg
from prompts import Prompts
import uuid
import numpy as np
from typing import List, Dict, Any, Tuple
import torch
from datetime import datetime
from typing import Optional
from datetime import datetime
from typing import Union, Optional
import time

#INPORTANT:always use gpt-4o-mini

prompts = Prompts()

class TranscriptAnalyzer:
    def __init__(self, gpu_device=None):
        self.gpu_device = gpu_device
        
        # Set the device for PyTorch
        if gpu_device is not None:
            self.device = f'cuda:{gpu_device}'
        else:
            self.device = 'cpu'
        
        # Initialize models with the specified device
        self.embeddings_model = SentenceTransformer("sentence-transformers/paraphrase-multilingual-mpnet-base-v2")
        self.embeddings_model.to(self.device)
        
        self.cross_encoder = CrossEncoder('cross-encoder/ms-marco-MiniLM-L-6-v2')
        if gpu_device is not None:
            self.cross_encoder.model.to(self.device)
        
        self.qdrant_client = QdrantClient("127.0.0.1", port=6333)
        self.collection_name = "transcript_collection"
        
        if not self.qdrant_client.collection_exists(self.collection_name):
            self.qdrant_client.create_collection(
                collection_name=self.collection_name,
                vectors_config=models.VectorParams(size=768, distance=models.Distance.COSINE),
            )

    def prepare_data(self, df):
        df = df[['speaker', 'formatted_time', 'content','chunk_number']]
        
        chunk_texts = []
        for chunk_num in df['chunk_number'].unique():
            chunk_df = df[df['chunk_number'] == chunk_num]
            chunk_text = "\n".join([f"{row['speaker']} ({row['formatted_time']}): {row['content']}" for _, row in chunk_df.iterrows()])
            chunk_texts.append(chunk_text)
        
        return chunk_texts
    
    def concat_chunks(self, chunk_texts, summary):
        return [f"{summary}\n\n{chunk}" for chunk in chunk_texts]

    async def contextualize_chunks(self, chunk_texts, summary,model='gpt-4o-mini'):
        async def contextualize_chunk(chunk):
            return await generic_call(messages=[
                system_msg(prompts.claude_chunk_contextualizer.format(WHOLE_DOCUMENT=summary, CHUNK_CONTENT=chunk))
            ],model=model,temperature=0.2)

        contextualized_chunks = await asyncio.gather(*[contextualize_chunk(chunk) for chunk in chunk_texts])
        
        return [f"{context}\n\n{chunk}" for context, chunk in zip(contextualized_chunks, chunk_texts)]
    
    
    def get_embeddings(self, texts: List[str]) -> np.ndarray:
        """
        Generate embeddings for a list of texts using the embeddings model.

        Args:
            texts (List[str]): A list of texts to generate embeddings for.

        Returns:
            np.ndarray: A 2D numpy array where each row is an embedding vector for the corresponding text.
        """
        # Ensure the model is in evaluation mode
        self.embeddings_model.eval()

        # Generate embeddings
        with torch.no_grad():  # Disable gradient calculation for inference
            embeddings = self.embeddings_model.encode(texts, device=self.device, show_progress_bar=False)

        return embeddings

    async def update_vectorstore(self, contextualized_chunks: List[str], chunk_texts: List[str], start_datetime: pd.Timestamp, speakers: List[str], meeting_session_id: str) -> List[models.PointStruct]:
        points = [
            models.PointStruct(
                id=str(uuid.uuid4()),
                vector=self.embeddings_model.encode(chunk, device=self.device).tolist(),
                payload={
                    "content": chunk + ';' + source,
                    "raw_chunk": source,
                    "start_datetime": start_datetime.isoformat(),
                    "speakers": speakers,
                    "meeting_session_id": meeting_session_id,
                    "type": "point"
                }
            )
            for chunk, source in zip(contextualized_chunks, chunk_texts)
        ]
        
        self.qdrant_client.upsert(
            collection_name=self.collection_name,
            points=points
        )
        return points
    
    
    async def update_vectorstore_with_qoutes(self, chunks: List[str], points: List[str], qoutes: List[str], start_datetime: pd.Timestamp, speakers: List[str], meeting_session_id: str) -> List[models.PointStruct]:
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

    async def add_summary(self, meeting_name: str, summary: str, start_datetime: pd.Timestamp, speakers: List[str], meeting_session_id: str) -> models.PointStruct:
        summary_point = models.PointStruct(
            id=str(uuid.uuid4()),
            vector=self.embeddings_model.encode(summary, device=self.device).tolist(),
            payload={
                "content": summary,
                "meeting_name": meeting_name,
                "start_datetime": int(start_datetime.timestamp()),
                "speakers": speakers,
                "meeting_session_id": meeting_session_id,
                "type": "summary"
            }
        )
        
        self.qdrant_client.upsert(
            collection_name=self.collection_name,
            points=[summary_point]
        )
        return summary_point

    async def search_documents(self, vector_search_query: Optional[str] = None, k: int = 10, include_summary: bool = False, start: Optional[Union[str, datetime]] = None, end: Optional[Union[str, datetime]] = None) -> List[Tuple[Dict[str, Any], float, str]]:
        # Initialize the filter conditions
        filter_conditions = []
        
        if not include_summary:
            filter_conditions.append(models.FieldCondition(key="type", match=models.MatchValue(value="point")))
        
        # Add timestamp filters if provided
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
        
        # Combine all filter conditions
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

    async def multi_search_documents(self, queries: List[Dict[str, Any]], k: int = 10) -> List[Dict[str, Any]]:
        # Run searches for all queries
        search_results = await asyncio.gather(*[
            self.search_documents(
                vector_search_query=query.get('vector_search_query'),
                k=k,
                include_summary=False,  # Assuming we don't want summaries by default
                start=query.get('start'),
                end=query.get('end')
            )
            for query in queries
        ])

        # Flatten the results
        all_results = [item for sublist in search_results for item in sublist]
        
        print(f"Total results before deduplication: {len(all_results)}")

        # Deduplicate based on the 'id' field
        seen_ids = set()
        deduplicated_results = []

        for doc, score, doc_id in all_results:
            if doc_id not in seen_ids:
                seen_ids.add(doc_id)
                deduplicated_results.append((doc, score, doc_id))

        print(f"Total results after deduplication: {len(deduplicated_results)}")

        # Sort the deduplicated results by score in descending order
        sorted_results = sorted(deduplicated_results, key=lambda x: x[1], reverse=True)

        return sorted_results

    async def rerank_documents(self, query, documents_with_scores, k=5):
        current_time = pd.Timestamp.now()
        
        # Prepare pairs for cross-encoder
        pairs = [[query, doc['content']] for doc, _ in documents_with_scores]
      #  cross_scores = self.cross_encoder.predict(pairs, device=self.device)
        
        # Calculate recency score (logarithmic)
        time_diffs = [abs((pd.Timestamp.fromisoformat(doc['start_datetime']) - current_time).total_seconds()) for doc, _ in documents_with_scores]
        max_time_diff = max(time_diffs)
        
        # Combine scores: cross-encoder score, vector similarity score, and recency
        combined_scores = []
        for i, (doc, vector_score) in enumerate(documents_with_scores):
            time_diff = time_diffs[i]
            
            # Logarithmic recency score
            epsilon = 1e-10  # Small constant to avoid log(0)
            recency_score = 1 - np.log(1 + time_diff) / np.log(1 + max_time_diff + epsilon)
            
            # Apply additional non-linear transformation to emphasize very recent documents
            recency_score = np.power(recency_score, 0.5)  # Square root to boost high values
            
            combined_score = (
              #  0.05 * cross_scores[i] +  # Cross-encoder score
                0.15 * vector_score +     # Vector similarity score
                0.8 * recency_score      # Logarithmic recency score with higher weight
            )
            combined_scores.append((doc, combined_score))
        
        # Sort by combined score (descending) and return top k
        reranked = sorted(combined_scores, key=lambda x: x[1], reverse=True)
        return [doc for doc, _ in reranked[:k]]

    def delete_collection(self):
        self.qdrant_client.delete_collection(self.collection_name)
        
    def count_documents(self):
        collection_info = self.qdrant_client.get_collection(self.collection_name)
        return collection_info.points_count

    def get_unique_meeting_session_ids(self) -> List[str]:
        scroll_result = self.qdrant_client.scroll(
            collection_name=self.collection_name,
            scroll_filter=models.Filter(
                must=[
                    models.FieldCondition(
                        key="meeting_session_id",
                        match=models.MatchValue(value="")
                    )
                ]
            ),
            limit=10000,
            with_payload=["meeting_session_id"],
            with_vectors=False
        )

        unique_ids = set()
        for point in scroll_result[0]:
            unique_ids.add(point.payload["meeting_session_id"])

        return list(unique_ids)

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
        
    async def prepare_context(self, query: str, k: int = 50) -> Tuple[str, List[Dict[str, Any]]]:
        # Get all summaries
        summaries = self.qdrant_client.scroll(
            collection_name=self.collection_name,
            scroll_filter=models.Filter(
                must=[
                    models.FieldCondition(
                        key="type",
                        match=models.MatchValue(value="summary")
                    )
                ]
            ),
            limit=10000,  # Adjust this value based on your expected maximum number of summaries
            with_payload=True,
            with_vectors=False
        )[0]
        
        summaries = [point.payload for point in summaries]
        
        # Sort summaries by start_datetime
        summaries.sort(key=lambda x: pd.Timestamp(x['start_datetime']))
        
        # Search for related points
        points_with_scores = await self.search_documents(query, k=k*2, include_summary=False)
        
        # Rerank points
        reranked_points = await self.rerank_documents(query, points_with_scores, k=k)
        
        # Group points by meeting_session_id
        points_by_meeting = {}
        for point in reranked_points:
            meeting_id = point['meeting_session_id']
            if meeting_id not in points_by_meeting:
                points_by_meeting[meeting_id] = []
            points_by_meeting[meeting_id].append(point)
        
        # Build context
        context = []
        for summary in summaries:
            meeting_id = summary['meeting_session_id']
            context.append(f"Meeting Summary ({summary['start_datetime']}):\n{summary['content']}")
            
            if meeting_id in points_by_meeting:
                for point in points_by_meeting[meeting_id]:
                    context.append(f"Related Point ({point['raw_chunk']}")
            else:
                context.append("No related points found for this meeting.")
            
            context.append("\n---\n")
        
        # Join context
        full_context = "\n".join(context)
        
        return full_context, reranked_points

    async def answer_query(self, query: str, k: int = 5, model: str = "gpt-4o-mini") -> Tuple[str, List[Dict[str, Any]]]:
        full_context, reranked_points = await self.prepare_context(query, k)
        
        # Generate answer
        final_answer = await generic_call(messages=[
            system_msg(prompts.sys),
            system_msg("""You are a helpful assistant that has memories about meetings you wisited with Dmitry Grankin.
                       This context may contain information about the related question, search for relevant information, as well as recreate the context around the question. 
                       First imagine and explain the current context that is related to the question, step by step. Finally, craft your answer to the question based on the context and the question itself.
                       
                       
                       """),
            user_msg(f"Based on the following context, answer the question: {query}\n\nContext:\n{full_context}")
        ], model=model, temperature=0.2)
        
        return final_answer, reranked_points, full_context

    def get_items_by_meeting_session_id(self, meeting_session_id: str) -> List[Dict[str, Any]]:
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
            limit=100,  # Adjust this value based on your expected maximum number of items per meeting
            with_payload=True,
            with_vectors=False
        )

        return [item.payload for item in search_result[0]]

    @classmethod
    def calculate_similarity(cls, string1: str, string2: str) -> float:
        # Initialize the model if it doesn't exist
        if not hasattr(cls, 'embeddings_model'):
            cls.embeddings_model = SentenceTransformer("sentence-transformers/paraphrase-multilingual-mpnet-base-v2")
            cls.embeddings_model.to('cpu')  # Default to CPU

        # Encode both strings
        embedding1 = cls.embeddings_model.encode(string1)
        embedding2 = cls.embeddings_model.encode(string2)
        
        # Calculate cosine similarity
        similarity = torch.nn.functional.cosine_similarity(
            torch.tensor(embedding1).unsqueeze(0),
            torch.tensor(embedding2).unsqueeze(0)
        ).item()
        
        return similarity

    @classmethod
    def calculate_similarity_matrix(cls, array1: List[str], array2: List[str]) -> torch.Tensor:
        # Initialize the model if it doesn't exist
        if not hasattr(cls, 'embeddings_model'):
            cls.embeddings_model = SentenceTransformer("sentence-transformers/paraphrase-multilingual-mpnet-base-v2")
            cls.embeddings_model.to('cpu')  # Default to CPU

        # Encode all strings in both arrays
        embeddings1 = cls.embeddings_model.encode(array1)
        embeddings2 = cls.embeddings_model.encode(array2)

        # Convert to PyTorch tensors
        embeddings1 = torch.tensor(embeddings1)
        embeddings2 = torch.tensor(embeddings2)

        # Calculate cosine similarity matrix
        similarity_matrix = torch.nn.functional.cosine_similarity(
            embeddings1.unsqueeze(1),
            embeddings2.unsqueeze(0),
            dim=2
        )

        return similarity_matrix

    @classmethod
    def associate_chunks_with_points(cls, similarity_matrix: torch.Tensor, chunks: List[str], points: List[str], threshold: float = 0.5) -> List[Dict[str, Any]]:
        associations = []
        for i, chunk in enumerate(chunks):
            chunk_associations = []
            for j, point in enumerate(points):
                if similarity_matrix[i, j] >= threshold:
                    chunk_associations.append({
                        "point": point,
                        "similarity": similarity_matrix[i, j].item()
                    })
            
            if chunk_associations:
                associations.append({
                    "chunk": chunk,
                    "associations": sorted(chunk_associations, key=lambda x: x["similarity"], reverse=True)
                })
        
        return associations
    
    

    async def build_context(self, queries, summaries, only_summaries=False):
        points_with_scores = await self.multi_search_documents(queries=queries)
        points_by_meeting = {}
        for point, score, id in points_with_scores:
            if score > 0.1:
                meeting_id = point['meeting_session_id']
                if meeting_id not in points_by_meeting:
                    points_by_meeting[meeting_id] = []
                points_by_meeting[meeting_id].append(point)

        # Build context
        context = []
        for summary in summaries:
            meeting_id = summary['meeting_session_id']
            if meeting_id in points_by_meeting or only_summaries:
                start_datetime = pd.to_datetime(datetime.fromtimestamp(summary['start_datetime']))
                context.append(f"# Meeting Summary ({summary['meeting_name']}: {start_datetime.strftime('%Y-%m-%d %H:%M')}):\n{summary['content']}")
                
                if not only_summaries and meeting_id in points_by_meeting:
                    context.append("## Related Quotes:")
                    for point in points_by_meeting[meeting_id]:
                        context.append(f"{point['qoutes']}")
                
                context.append("\n---\n")

        # Join context
        full_context = "\n".join(context)
        return full_context
    
    def get_summaries(self):
        summaries = self.qdrant_client.scroll(
            collection_name=self.collection_name,
            scroll_filter=models.Filter(
                must=[
                    models.FieldCondition(
                        key="type",
                        match=models.MatchValue(value="summary")
                    )
                ]
            ),
            limit=10000,  # Adjust this value based on your expected maximum number of summaries
            with_payload=True,
            with_vectors=False
        )[0]
        summaries = [point.payload for point in summaries]
        
        # Sort summaries by start_datetime
        summaries.sort(key=lambda x: pd.Timestamp(x['start_datetime']))
        
        return summaries

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
    
    
    
async def generate_summary(transcript,output_length=10000,max_tokens=10000):
    return await generic_call(messages=[
        system_msg(f"""create consice summary of the following text as markdown with atention to company FACTS names, people and dates,numbers and facts. All FACTS must be preserved
                    wrap into tags <summary></summary> and <point></point>
                            output should be at most {output_length} characters long, if meeting is long enough. 
                            structure: <summary>sort summary (500 characters),
                            main points as bullets (500 characters each) <point>point content should mention speakers, FACTS, main ideas, consice information </point>"""),
        user_msg(transcript)
        ],model='gpt-4o-mini',temperature=1,max_tokens=max_tokens)

    def get_transcription(self, df, start_index=None, end_index=None, use_index=False):
        if use_index:
            df = df.reset_index()
            time_column = 'index'
        else:
            time_column = 'formatted_time'
        
        if start_index is not None:
            df = df[df[time_column] >= start_index]
        if end_index is not None:
            df = df[df[time_column] <= end_index]
        
        transcript = ""
        for _, row in df.iterrows():
            transcript += f"{row['speaker']} ({row[time_column]}): {row['content']}\n"
        
        return transcript
    
    
