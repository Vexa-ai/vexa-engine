from typing import Tuple, Optional, BinaryIO
import os
import tempfile
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class AudioProcessor:
    def __init__(self):
        self.temp_dir = tempfile.gettempdir()

    async def writestream2file(self, stream: BinaryIO, filename: Optional[str] = None) -> Tuple[str, str, datetime]:
        # Create temp file if no filename provided
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'audio_{timestamp}.wav'
        filepath = os.path.join(self.temp_dir, filename)
        # Write stream to file
        try:
            with open(filepath, 'wb') as f:
                chunk = stream.read(8192)
                while chunk:
                    f.write(chunk)
                    chunk = stream.read(8192)
            # Get file info
            file_size = os.path.getsize(filepath)
            created_time = datetime.fromtimestamp(os.path.getctime(filepath))
            logger.info(f'Wrote audio file {filepath} ({file_size} bytes)')
            return filepath, filename, created_time
        except Exception as e:
            logger.error(f'Error writing audio file: {str(e)}')
            if os.path.exists(filepath):
                os.remove(filepath)
            raise 