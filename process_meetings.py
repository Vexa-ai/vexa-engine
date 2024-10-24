import asyncio
import pandas as pd
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.future import select
from sqlalchemy import func
from psql_models import Speaker, Meeting, DiscussionPoint, Topic, Base, DATABASE_URL, discussion_topic_association
from vexa import VexaAPI
from pydantic_models import MeetingExtraction, EntityExtraction, SummaryIndexesRefs, MeetingSummary

# Database setup

engine = create_async_engine(DATABASE_URL, echo=True)

async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

async def get_session():
    async with AsyncSession(engine) as session:
        yield session

async def check_discussion_points_exist(meeting_id):
    async with AsyncSession(engine) as session:
        meeting_id_str = str(meeting_id)
        query = select(func.count(DiscussionPoint.id)).where(DiscussionPoint.meeting_id == meeting_id_str)
        result = await session.execute(query)
        count = result.scalar()
        return count > 0

async def process_meeting_data(formatted_input, df):
    discussion_points_df = await MeetingExtraction.extract(formatted_input)
    topics_df = await EntityExtraction.extract(formatted_input)
    summary_df = pd.concat([discussion_points_df, topics_df.rename(columns={'entity': 'topic'})]).reset_index(drop=True)
    summary_refs = await SummaryIndexesRefs.extract(summary_df, formatted_input)

    ref_df = pd.DataFrame([(ref['summary_index'], r['s'], r['e']) 
                           for ref in summary_refs 
                           for r in ref['references']],
                          columns=['summary_index', 'start', 'end'])

    entities_with_refs = summary_df.reset_index().rename(columns={'index': 'summary_index'})
    entities_with_refs = entities_with_refs.merge(ref_df, on='summary_index', how='left')

    def get_text_range_with_speaker(row):
        text_range = df.loc[row['start']:row['end']]
        return ' | '.join(f"{speaker}: {content}" for speaker, content in zip(text_range['speaker'], text_range['content']))

    entities_with_refs['referenced_text'] = entities_with_refs.apply(get_text_range_with_speaker, axis=1)

    final_df = entities_with_refs.groupby('summary_index').agg({
        'topic': 'first',
        'type': 'first',
        'summary': 'first',
        'details': 'first',
        'speaker': 'first',
        'referenced_text': ' | '.join
    }).reset_index()

    return final_df

async def save_meeting_data_to_db(final_df, meeting_id, transcript, meeting_datetime):
    async with AsyncSession(engine) as session:
        try:
            existing_meeting = await session.execute(
                select(Meeting).where(Meeting.meeting_id == meeting_id)
            )
            existing_meeting = existing_meeting.scalar_one_or_none()

            if not existing_meeting:
                naive_datetime = meeting_datetime.replace(tzinfo=None) - meeting_datetime.utcoffset()
                new_meeting = Meeting(
                    meeting_id=meeting_id, 
                    transcript=str(transcript),
                    timestamp=naive_datetime
                )
                session.add(new_meeting)
                await session.flush()
            else:
                new_meeting = existing_meeting

            for _, row in final_df.iterrows():
                speaker_query = await session.execute(
                    select(Speaker).where(Speaker.name == row['speaker'])
                )
                speaker = speaker_query.scalar_one_or_none()
                
                if not speaker:
                    speaker = Speaker(name=row['speaker'])
                    session.add(speaker)
                    await session.flush()

                new_discussion_point = DiscussionPoint(
                    summary_index=row['summary_index'],
                    summary=row['summary'],
                    details=row['details'],
                    referenced_text=row['referenced_text'],
                    meeting_id=new_meeting.meeting_id,
                    speaker_id=speaker.id
                )
                session.add(new_discussion_point)
                await session.flush()

                topic_query = await session.execute(
                    select(Topic).where(Topic.name == row['topic'])
                )
                topic = topic_query.scalar_one_or_none()

                if not topic:
                    topic = Topic(name=row['topic'], type=row['type'])
                    session.add(topic)
                    await session.flush()

                # Instead of appending directly, create an association
                await session.execute(
                    discussion_topic_association.insert().values(
                        discussion_point_id=new_discussion_point.id,
                        topic_id=topic.id
                    )
                )

            await session.commit()
            print("Meeting data, discussion points, and topics saved successfully to the database.")
        except Exception as e:
            await session.rollback()
            print(f"Error saving to database: {e}")
            raise

async def main():
    await init_db()
    vexa = VexaAPI()
    await vexa.get_user_info()
    meetings = await vexa.get_meetings()
    meetings = meetings[-101:]

    print(f"Total meetings: {len(meetings)}")
    meeting_id = meetings[1]['id']
    print(f"Processing meeting with ID: {meeting_id}")
    
    discussion_points_exist = await check_discussion_points_exist(meeting_id)
    print(f"Discussion points exist: {discussion_points_exist}")

    if not discussion_points_exist:
        print("No discussion points found. Processing meeting data...")
        transcription = await vexa.get_transcription(meeting_session_id=meeting_id, use_index=True)
        df, formatted_input, start_datetime, speakers, transcript = transcription
        final_df = await process_meeting_data(formatted_input, df)
        print('final_df len', len(final_df))
        await save_meeting_data_to_db(final_df, meeting_id, transcript, start_datetime)
    else:
        print("Discussion points already exist for this meeting. Skipping processing.")

    print("Processing complete.")

if __name__ == "__main__":
    asyncio.run(main())
