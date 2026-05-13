import time
import asyncio
from pathlib import Path

from src.logger import get_logger
from src.config import Config
from src.scraper.browser import BrowserManager
from src.scraper.navigator import IntermapperScraper
from src.scraper.tower_naming import tower_name_from_screenshot_stem
from src.n8n_client import send_image_to_n8n

logger = get_logger(__name__)

async def run_scraper_phase():
    """Fase 1: Navegación y captura de pantallas (Igual que antes)"""
    logger.info("--- INICIANDO FASE 1: NAVEGACIÓN Y CAPTURAS ---")
    browser_manager = BrowserManager()
    context = await browser_manager.start()
    
    scraper = IntermapperScraper(context)
    page = await scraper.login()
    
    urls = await scraper.get_site_links(page)
    await page.close()

    semaphore = asyncio.Semaphore(Config.WORKERS)
    
    tasks = [scraper.process_site(url, semaphore) for url in urls]
    results = await asyncio.gather(*tasks)

    await browser_manager.stop()

    # Filtramos los que fueron exitosos
    sites_to_process = [r for r in results if r is not None]
    
    # Fallback por si falló el scraping en vivo pero hay imágenes locales
    if not sites_to_process:
        for screenshot_path in Config.SCREENSHOT_DIR.glob("*.png"):
            tower_name = tower_name_from_screenshot_stem(screenshot_path.stem)
            sites_to_process.append((tower_name, screenshot_path, None))

    return sites_to_process

async def run_n8n_phase(sites_to_process: list):
    """Fase 2: Envío de capturas a n8n concurrentemente y limpieza local"""
    logger.info("--- INICIANDO FASE 2: ENVÍO A N8N Y LIMPIEZA ---")
    
    # Creamos un semáforo para no saturar n8n con 50 requests simultáneos.
    n8n_semaphore = asyncio.Semaphore(5)

    async def _bounded_send(tower, path: Path):
        async with n8n_semaphore:
            # Enviamos a n8n y guardamos el resultado (True si fue exitoso)
            success = await send_image_to_n8n(tower, path)
            
            # Si n8n respondió 200 OK, procedemos a borrar el archivo físico
            if success:
                try:
                    # unlink() elimina el archivo de forma nativa en Python
                    path.unlink(missing_ok=True) 
                    logger.info(f"[{tower}] 🗑️ Imagen local eliminada para liberar espacio.")
                except Exception as e:
                    logger.error(f"[{tower}] ⚠️ No se pudo borrar la imagen local {path.name}: {e}")
            else:
                logger.warning(f"[{tower}] 💾 La imagen se conservó localmente porque falló el envío a n8n.")

    tasks = [
        _bounded_send(tower_name, screenshot_path) 
        for tower_name, screenshot_path, _url in sites_to_process
    ]
    
    await asyncio.gather(*tasks)

async def main_async():
    logger.info("Iniciando Pipeline de Scraping hacia n8n...")
    start_time = time.time()
    Config.setup_directories()
    
    # FASE 1: Capturas
    sites_to_process = await run_scraper_phase()
    logger.info(f"Fase 1 completada. {len(sites_to_process)} capturas listas.")

    # FASE 2: Enviar al Webhook
    if sites_to_process:
        await run_n8n_phase(sites_to_process)
    else:
        logger.warning("No hay capturas para enviar.")

    logger.info("=" * 60)
    logger.info(f"🚀 PIPELINE COMPLETADO EN {time.time() - start_time:.2f} SEGUNDOS 🚀")
    logger.info("=" * 60)

def main():
    asyncio.run(main_async())

if __name__ == '__main__':
    main()