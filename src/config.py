import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

class Config:
    URL = os.getenv("INTERMAPPER_URL")
    USERNAME = os.getenv("INTERMAPPER_USER")
    PASSWORD = os.getenv("INTERMAPPER_PASS")
    
    # URL de tu flujo en n8n
    N8N_WEBHOOK_URL = os.getenv("N8N_WEBHOOK_URL")
    
    BASE_DIR = Path(__file__).resolve().parent.parent
    SCREENSHOT_DIR = BASE_DIR / "screenshots"
    
    WORKERS = int(os.getenv("CONCURRENT_WORKERS", 3))

    @classmethod
    def setup_directories(cls):
        cls.SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)