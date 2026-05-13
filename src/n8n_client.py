import httpx
from pathlib import Path
from src.logger import get_logger
from src.config import Config

logger = get_logger(__name__)

async def send_image_to_n8n(tower_name: str, image_path: Path) -> bool:
    """
    Envía la captura de pantalla y el nombre de la torre al webhook de n8n.
    """
    if not Config.N8N_WEBHOOK_URL:
        logger.error("❌ No se ha definido N8N_WEBHOOK_URL en el .env")
        return False

    if not image_path.exists():
        logger.error(f"❌ La imagen {image_path} no existe.")
        return False

    logger.info(f"[{tower_name}] 📤 Enviando imagen a n8n...")

    try:
        # Usamos timeout largo porque las imágenes pueden ser pesadas
        async with httpx.AsyncClient(timeout=60.0) as client:
            with open(image_path, "rb") as f:
                # Preparamos el payload (Multipart Form)
                files = {
                    "image": (image_path.name, f, "image/png")
                }
                data = {
                    "tower_name": tower_name
                }
                
                response = await client.post(
                    Config.N8N_WEBHOOK_URL,
                    data=data,
                    files=files
                )
                
                response.raise_for_status()
                logger.info(f"[{tower_name}] ✅ Recibido por n8n correctamente. Status: {response.status_code}")
                return True

    except httpx.HTTPStatusError as e:
        logger.error(f"[{tower_name}] ❌ Error HTTP de n8n: {e.response.status_code} - {e.response.text}")
        return False
    except Exception as e:
        logger.error(f"[{tower_name}] ❌ Error de conexión al enviar a n8n: {e}")
        return False