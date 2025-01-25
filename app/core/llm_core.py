from app.core.llm_functions import Msg, generic_call, generic_call_stream, system_msg, user_msg
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

ANTROPIC_API_KEY = os.getenv('ANTROPIC_API_KEY')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
GROQ_API_KEY = os.getenv('GROQ_API_KEY')
TOGETHERAI_API_KEY = os.getenv('TOGETHERAI_API_KEY')
os.environ["OPENAI_API_KEY"] = OPENAI_API_KEY
os.environ["ANTHROPIC_API_KEY"] = ANTROPIC_API_KEY
os.environ["GROQ_API_KEY"] = GROQ_API_KEY
os.environ["TOGETHERAI_API_KEY"] = TOGETHERAI_API_KEY

if not all([OPENAI_API_KEY]):
    raise ValueError("One or more required API keys are missing from the .env file")
