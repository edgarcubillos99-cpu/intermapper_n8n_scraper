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

    # Intervalo en segundos entre cada envío de imagen a n8n (throttling).
    # Por defecto: 60s => 1 imagen por minuto.
    N8N_SEND_INTERVAL_SECONDS = int(os.getenv("N8N_SEND_INTERVAL_SECONDS", 60))

    DB_HOST = os.getenv("DB_HOST")
    DB_USER = os.getenv("DB_USER")
    DB_PASS = os.getenv("DB_PASS")
    DB_NAME = os.getenv("DB_NAME")

    MCP_BEARER_TOKEN = os.getenv("MCP_BEARER_TOKEN")
    
    @classmethod
    def setup_directories(cls):
        cls.SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)