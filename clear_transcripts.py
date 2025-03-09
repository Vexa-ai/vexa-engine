#!/usr/bin/env python3
import sys
import asyncio
import argparse

# Add app to path
sys.path.append('/app')

from services.psql_helpers import get_session
from models.db import Transcript, TranscriptAccess
from sqlalchemy import select, delete, func

async def delete_all_transcripts(dry_run=True, session=None):
    async with (session or get_session()) as session:
        # Count records before deletion
        transcript_count_query = select(func.count()).select_from(Transcript)
        transcript_access_count_query = select(func.count()).select_from(TranscriptAccess)
        
        transcript_count = await session.execute(transcript_count_query)
        transcript_access_count = await session.execute(transcript_access_count_query)
        
        total_transcripts = transcript_count.scalar()
        total_transcript_access = transcript_access_count.scalar()
        
        # Prepare deletion statements
        delete_transcript_access = delete(TranscriptAccess)
        delete_transcripts = delete(Transcript)
        
        # Only execute if not dry run
        if not dry_run:
            # Delete transcript access records first (due to foreign key constraint)
            if total_transcript_access > 0:
                await session.execute(delete_transcript_access)
            
            # Then delete transcript records
            if total_transcripts > 0:
                await session.execute(delete_transcripts)
                
            # Commit changes
            await session.commit()
            status = "DELETED"
        else:
            status = "DRY RUN - NO CHANGES MADE"
        
        # Return summary
        return {
            "status": status,
            "total_transcripts": total_transcripts,
            "total_transcript_access": total_transcript_access,
            "action": "would delete" if dry_run else "deleted"
        }

async def main(args):
    # First do a dry run to show what would be deleted
    dry_run_result = await delete_all_transcripts(dry_run=True)
    
    print("Current transcript data:")
    print(f"- Transcript records: {dry_run_result['total_transcripts']}")
    print(f"- Transcript access records: {dry_run_result['total_transcript_access']}")
    print()
    
    # If nothing to delete, exit early
    if dry_run_result['total_transcripts'] == 0 and dry_run_result['total_transcript_access'] == 0:
        print("No transcripts to delete.")
        return
    
    # Check if actually deleting
    if args.force or args.dry_run:
        proceed = True
    else:
        # Ask for confirmation
        response = input(f"Are you sure you want to delete all {dry_run_result['total_transcripts']} transcripts? [y/N]: ")
        proceed = response.lower() in ['y', 'yes']
    
    if not proceed:
        print("Operation cancelled.")
        return
    
    # Do the actual deletion if not in dry run mode
    if not args.dry_run:
        result = await delete_all_transcripts(dry_run=False)
        print(f"DELETED {result['total_transcripts']} transcripts and {result['total_transcript_access']} transcript access records.")
    else:
        print(f"DRY RUN: Would delete {dry_run_result['total_transcripts']} transcripts and {dry_run_result['total_transcript_access']} transcript access records.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Delete all transcriptions from the database")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be deleted without making changes")
    parser.add_argument("--force", "-f", action="store_true", help="Don't ask for confirmation")
    
    args = parser.parse_args()
    
    try:
        asyncio.run(main(args))
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
    except Exception as e:
        print(f"Error: {e}")