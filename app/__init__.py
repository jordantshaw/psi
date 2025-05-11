import dotenv
from pathlib import Path

env_file = Path(__file__).parent.parent / ".env"
dotenv.load_dotenv(env_file)
