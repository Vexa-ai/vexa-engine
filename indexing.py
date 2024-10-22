import asyncio
import pandas as pd
from vector_search import VectorSearch, build_context_string
from vexa import VexaAPI
from core import generic_call, system_msg, user_msg
from pydantic_models import Summary
from prompts import Prompts

class Indexing:
    def __init__(self, token: str, gpu_device=3, model="gpt-4o-mini"):
        self.analyzer = VectorSearch(gpu_device=gpu_device)
        self.vexa = VexaAPI(token=token)
        self.prompts = Prompts()
        self.model = model

    async def create_exploded_points_df(self, output):
        points_data = output[0].model_dump()['points']
        df_points = pd.DataFrame(points_data)
        df_points = df_points.rename(columns={'c': 'point'})
        
        def create_range(start, end):
            return list(range(int(start), int(end) + 1))
        
        df_points['range'] = df_points.apply(lambda row: create_range(row['s'], row['e']), axis=1)
        df_exploded = df_points.explode('range')
        df_exploded = df_exploded.drop(columns=['s', 'e'])
        df_exploded = df_exploded.reset_index(drop=True)
        return df_exploded

    def combine_initials_and_content(self, group):
        combined = group['speaker'].fillna('').astype(str) + ': ' + group['content'].fillna('')
        return ' '.join(combined)

    async def index_meetings(self, num_meetings=10):
        await self.vexa.get_user_info()
        meetings = await self.vexa.get_meetings()
        meetings = meetings[-num_meetings:]

        for meeting in reversed(meetings):
            meeting_id = meeting['id']
            if not await self.analyzer.check_meeting_session_id_exists(meeting_id):
                result = await self.vexa.get_transcription(meeting_session_id=meeting_id, use_index=True)
                if result:
                    df, formatted_input, start_datetime, speakers = result
                    output = await Summary.call([
                        system_msg(self.prompts.think + self.prompts.summarize_meeting + f'.The User: {self.vexa.user_name}'),
                        user_msg(formatted_input)
                    ], model=self.model)
                    
                    df_exploded = await self.create_exploded_points_df(output)
                    joined_df = df_exploded.join(df, on='range', rsuffix='_transcript')

                    points_with_qoutes = joined_df.groupby('point').apply(self.combine_initials_and_content)
                    points_with_qoutes.name = 'qoutes'
                    points_with_qoutes = points_with_qoutes.reset_index().to_dict(orient='records')
                    summary = output[0].summary
                    meeting_name = output[0].meeting_name

                    if points_with_qoutes:
                        chunks = [f"{summary}\n\n{p['point']}\n\n{p['qoutes']}" for p in points_with_qoutes]
                        points = [p['point'] for p in points_with_qoutes]
                        qoutes = [p['qoutes'] for p in points_with_qoutes]
                        await self.analyzer.add_summary(meeting_name, summary, start_datetime, speakers, meeting_id, self.vexa.user_id, self.vexa.user_name)
                        await self.analyzer.update_vectorstore_with_qoutes(chunks, points, qoutes, start_datetime, speakers, meeting_id, self.vexa.user_id, self.vexa.user_name)
