from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime

import pandas as pd
from core import BaseCall, system_msg, user_msg
from prompts import Prompts
prompts = Prompts()


class Entity(BaseModel):
    entity: str = Field(..., description="Name of the entity")
    type: str = Field(..., description="type of the entity, like person, company, product, topic, concept, task, goal, etc.")
    summary: str = Field(..., max_length=250, description="Full standalone concise summary of the entity's context, including its type. Avoid generic words, be specific")
    details: str = Field(..., description="specific details about the relationship between the entity and the speaker, like facts, numbers, and connections, opinions, usecases,etc")
    speaker: str = Field(
        ..., 
        description="Extract the speaker's name EXACTLY as written in the text - do not modify, interpret or standardize the name format. Copy the name verbatim as it appears."
    )
   # ref: Reference = Field(..., description="reference to the meeting transcript")

class EntityExtraction(BaseCall):
    language: str = Field(..., description="Detected language code of the input text (e.g. 'en')")
    list_of_entities: list[str] = Field(..., description="START WITH THIS ONE:List of all of the the entities in the meeting.")
    entities: List[Entity] = Field(..., description="List of extracted entities with their context. Get all the entities and their context.")
    
    @classmethod
    async def extract(cls, formatted_input: str, model: str = "gpt-4o-mini", use_cache: bool = False, force_store: bool = False):
        output = await cls.call([
            system_msg(f"""{prompts.naming} 
                          Think critically about the meeting content and extract key entities. Summarize the meeting, focusing on important details, names, and facts. 
                          IMPORTANT: Always copy speaker names EXACTLY as they appear in the text - do not modify or interpret names.
                          Entity to speaker can be one to one or many to many. Extract all the entities and their context. CHeck again is all the entities extracted. 
                          Write exclusively in the language in input"""),
            user_msg(formatted_input)
        ], model=model, use_cache=use_cache, force_store=force_store)

        # Process the output and create a DataFrame
        items_df = pd.DataFrame(output.model_dump()['entities'])
        return items_df

class MeetingItem(BaseModel):
    item: str = Field(..., description="Name of the meeting item (e.g., action plan, goal, idea, task, usecase, etc.)")
    type: str = Field(..., description="Type of the item: action plan, goal, future projection, idea, task, decision, concern, usecase, problem, solution, opportunity, risk, etc. Use the most appropriate descriptor.")
    summary: str = Field(..., max_length=250, description="Full standalone concise summary of the item's context, including its significance and potential impact. Be specific and avoid generic words.")
    details: str = Field(..., description="Specific details about the item, including timelines, responsible parties, importance, potential impacts, usecases, implementation steps, challenges, benefits, and any relevant facts or numbers")
    speaker: str = Field(
        ..., 
        description="Extract the speaker's name EXACTLY as written in the text - do not modify, interpret or standardize the name format. Copy the name verbatim as it appears."
    )

class MeetingExtraction(BaseCall):
    language: str = Field(..., description="Detected language code of the input text (e.g. 'en')")
    list_of_items: list[str] = Field(..., description="START WITH THIS ONE: List of all significant items discussed in the meeting, including action plans, goals, future projections, ideas, tasks, decisions, concerns, usecases, problems, solutions, opportunities, risks, etc.")
    items: List[MeetingItem] = Field(..., description="List of extracted meeting items with their full context. Capture all significant discussion points, plans, outcomes, usecases, and their implications. Write exclusively in the language in input")

    @classmethod
    async def extract(cls, formatted_input: str, model: str = "gpt-4o-mini", use_cache: bool = False, force_store: bool = False):
        output = await cls.call([
            system_msg(f"""{prompts.naming} 
                            Analyze the meeting content and extract key purposes, tasks, and actions. Summarize each item, focusing on important details, deadlines, assignees, usecases, etc. and any relevant facts or numbers.
                          IMPORTANT: Always copy speaker names EXACTLY as they appear in the text - do not modify or interpret names.
                          Write exclusively in the language in input"""),
            user_msg(formatted_input)
        ], model=model, use_cache=use_cache, force_store=force_store)

        # Process the output and create a DataFrame
        items_df = pd.DataFrame(output.model_dump()['items'])
        return items_df

from pydantic import BaseModel, Field
from typing import List

class Reference(BaseModel):
    """
    Represents a range in the transcript that directly corresponds to a part of a summary item.
    This range should cover the relevant discussion in the transcript that supports the summary point.
    Aim for wider ranges to provide sufficient context, ideally spanning at least 10 indices.
    """
    s: int = Field(..., description="Start index in the transcript where the relevant discussion for this summary point begins. Choose a position that captures the start of the relevant context.")
    e: int = Field(..., description="End index in the transcript where the relevant discussion for this summary point ends. Ensure this captures the full context of the point made in the summary.")

class SummaryReference(BaseModel):
    """
    Links a specific summary item to its corresponding references in the transcript.
    Each summary item should have one or more references that directly support its content.
    Aim for comprehensive coverage of the summary point in the transcript.
    """
    summary_index: int = Field(..., description="Index of the summary item in the list of summaries. Ensure this matches the position of the summary point you're referencing.")
    references: List[Reference] = Field(..., description="List of reference ranges in the transcript that support this summary item's content. Include 1-5 ranges that collectively cover all aspects of the summary point.")

class SummaryIndexesRefs(BaseCall):
    """
    Maps each summary item to its supporting references in the transcript.
    This structure ensures that all summary items are accounted for and properly referenced in the original transcript.
    It's crucial that the references accurately reflect the content of each summary point.
    """
    summary_indexes_refs: List[SummaryReference] = Field(
        description="Provide a list of SummaryReference objects, one for each summary item, in order. "
                    "For each summary item, include 1-5 reference ranges in the transcript that directly support its content. "
                    "Ensure that each range spans at least 10 indices to provide adequate context. "
                    "The referenced transcript portions must discuss the specific content mentioned in the corresponding summary point. "
                    "All summary indexes must be present, starting from 0 and in consecutive order. "
                    "Example: [{'summary_index': 0, 'references': [{'s': 10, 'e': 50}]}, "
                    "{'summary_index': 1, 'references': [{'s': 60, 'e': 100}, {'s': 150, 'e': 200}]}]"
    )
    
    @classmethod
    async def extract(cls, entities_df: pd.DataFrame, formatted_input: str, model: str = "gpt-4o-mini", use_cache: bool = False, force_store: bool = False):
        output = await cls.call([
            system_msg(f"""find all the references to the summary in the meeting transcript"""),
            user_msg(f"summary: {entities_df.to_markdown()}, transcript: {formatted_input}")
        ], model=model, use_cache=use_cache, force_store=force_store)

        # Extract the summary_indexes_refs from the output
        summary_refs = output.model_dump()['summary_indexes_refs']

        return summary_refs
    
    
class Summary(BaseModel):
    summary: str = Field(..., description="Direct, to-the-point summary of the meeting")
    meeting_name: str = Field(..., description="Name of the meeting")

class MeetingSummary(BaseCall):
    output: Summary = Field(..., description="Meeting name and summary. Reference each item exactly as (reference item here as appropriate to the context)[item index] .")

    @classmethod
    async def extract(cls, formatted_input: str, final_df: pd.DataFrame, model: str = "gpt-4o-mini", use_cache: bool = False, force_store: bool = False):
        output = await cls.call([
            system_msg("""Create a concise, direct summary incorporating all provided items from the data. 
                          Begin immediately with key points and content. Do not use any introductory phrases.
                          Absolutely avoid starting with phrases like "The meeting involved," "During the discussion," or any similar openings.
                          Reference each item exactly as (reference item name here as appropriate to the context)[item index] in the summary. 
                          Ensure all items are included and the summary flows naturally while capturing key points.
                          Use the full transcript for additional context if needed.
                          Avoid any temporal references or statements about the meeting itself.
                          Write exclusively in the language in input"""),
            user_msg(f"""Here are the items and their details:

{final_df.reset_index(drop=True)[['meeting_time','speaker_name','topic_name','summary','details','summary_index']].sort_values('summary_index').to_csv()}

Full transcript:

{formatted_input}

Create a direct, to-the-point summary incorporating all these items. Start immediately with the content, referencing items exactly as (reference item here as appropriate to the context)[item index]. Do not use any introductory phrases or mention that this is a summary of a meeting.""")
        ], model=model, use_cache=use_cache, force_store=force_store)

        return output.output


class ThreadName(BaseCall):
    thread_name: str = Field(..., max_length=50, description="Condensed explanatory concise dense name of the meeting")
    
    @classmethod
    async def extract(cls, formatted_input: str, model: str = "gpt-4o-mini", use_cache: bool = False, force_store: bool = False):
        output = await cls.call([
            system_msg(f"{prompts.naming}\nCreate a dense, explanatory name that captures the main purpose."),
            user_msg(formatted_input)
        ], model=model, use_cache=use_cache, force_store=force_store)
        return output


class SearchQuery(BaseModel):
    query: str = Field(
        ..., 
        description="Search query to find relevant information in meeting transcripts"
    )

class ParsedSearchRequest(BaseCall):
    search_queries: List[SearchQuery] = Field(
        ...,
        description="List of search queries that will be used to search meeting transcripts",
        min_items=1
    )
    
    @classmethod
    async def parse_request(cls, spoken_request: str) -> "ParsedSearchRequest":
        """Helper method to create a ParsedSearchRequest instance"""
        messages = [
            system_msg("""You are a search query parser that helps find relevant information in meeting transcripts.

Your role in the system:
1. User makes a natural language request
2. You break it down into search queries
3. These queries will be sent to a search engine that looks through meeting transcripts
4. The search results will be used to construct a response to the user

Guidelines for creating effective search queries:
- Create queries that will find relevant discussions in meetings
- Focus on key terms that people would actually say in conversations
- Break complex requests into simple searchable concepts
- Include different variations of important terms
- Keep queries concise but meaningful
- Think about what words would appear in relevant meeting discussions

Remember: Your queries will be used to find actual conversations where people discussed these topics."""),
            
            user_msg(f"What search queries would find relevant meeting discussions to answer this request: {spoken_request}")
        ]
        
        return await cls.call(messages)
    

class MeetingNameAndSummary(BaseCall):
    language: str = Field(..., description="Detected language code of the input text (e.g. 'en')")
    meeting_name: str = Field(..., description="Concise meeting name")
    summary: str = Field(..., description="Meeting summary")

    @classmethod
    async def extract(cls, formatted_input: str, summary_input: str, general_context: str = None, model: str = "gpt-4o-mini", use_cache: bool = False, force_store: bool = False):
        # Prepare context from sampled discussions if available
        context_str = ""
        if general_context is not None:
            context_str = f"""
            Sample of recent discussions (for broad con and not for including into summary):
            {general_context}
            """

        output = await cls.call([
            system_msg(f"""{prompts.naming}
                      Create a direct text representation and meeting name based on the discussion points, transcript, and historical context.
                      For the text: Start immediately with content, no introductory phrases.
                      For the meeting name: Create a dense, explanatory name (max 50 chars) that captures the main purpose.
                      Include and highlight ALL the POINTS and important keywords with **...** markdown syntax. Write exclusively in the language in input"""),
            user_msg(f"""Points. Include and highlight ALL the POINTS  with **...** markdown syntax:
                        {summary_input}

                        {context_str}

                        Full transcript:
                        {formatted_input}

Generate a direct summary and concise meeting name.""")
        ], model=model, use_cache=use_cache, force_store=force_store)

        return output
