import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


def _bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


class Config:
    # --- Intermapper ---
    URL = os.getenv("INTERMAPPER_URL")
    USERNAME = os.getenv("INTERMAPPER_USER")
    PASSWORD = os.getenv("INTERMAPPER_PASS")

    # --- n8n ---
    N8N_WEBHOOK_URL = os.getenv("N8N_WEBHOOK_URL")
    N8N_SEND_INTERVAL_SECONDS = int(os.getenv("N8N_SEND_INTERVAL_SECONDS", "60"))

    # --- Paths ---
    BASE_DIR = Path(__file__).resolve().parent.parent
    DATA_DIR = Path(os.getenv("DATA_DIR", str(BASE_DIR / "data")))
    SCREENSHOT_DIR = Path(os.getenv("SCREENSHOT_DIR", str(BASE_DIR / "screenshots")))
    # Mapping {torre: {ap_name_completo: ip_address}} generado en Fase 1
    # y consumido por el MCP server al recibir el sync de n8n.
    IP_MAP_PATH = DATA_DIR / "ip_map.json"

    WORKERS = int(os.getenv("CONCURRENT_WORKERS", "3"))

    # --- Base de datos ---
    DB_HOST = os.getenv("DB_HOST")
    DB_USER = os.getenv("DB_USER")
    DB_PASS = os.getenv("DB_PASS")
    DB_NAME = os.getenv("DB_NAME")

    # --- MCP server ---
    MCP_BEARER_TOKEN = os.getenv("MCP_BEARER_TOKEN")
    MCP_HOST = os.getenv("MCP_HOST", "0.0.0.0")
    MCP_PORT = int(os.getenv("MCP_PORT", "8000"))

    # --- Orquestación (qué arranca el entrypoint unificado main.py) ---
    RUN_MCP = _bool_env("RUN_MCP", True)
    RUN_PIPELINE = _bool_env("RUN_PIPELINE", True)
    # "once"  → corre el pipeline una sola vez y luego el MCP queda solo.
    # "loop"  → repite el pipeline cada PIPELINE_INTERVAL_SECONDS para siempre.
    PIPELINE_MODE = os.getenv("PIPELINE_MODE", "once").strip().lower()
    PIPELINE_INTERVAL_SECONDS = int(os.getenv("PIPELINE_INTERVAL_SECONDS", "3600"))
    # Espera inicial antes de la primera ronda del pipeline, para dar tiempo
    # al MCP server a estar listo (importante en arranque del contenedor).
    PIPELINE_INITIAL_DELAY_SECONDS = int(os.getenv("PIPELINE_INITIAL_DELAY_SECONDS", "5"))

    @classmethod
    def setup_directories(cls):
        cls.SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)
        cls.DATA_DIR.mkdir(parents=True, exist_ok=True)
