import asyncio

from src.config import Config
from src.logger import get_logger
from src.scraper.tower_naming import (
    MAX_SCREENSHOT_STEM_BASE,
    fallback_map_slug_from_url,
    map_slug_from_intermapper_url,
    tower_name_from_screenshot_stem,
)

logger = get_logger(__name__)

class IntermapperScraper:
    def __init__(self, context):
        self.context = context
        self.base_url = Config.URL

    async def login(self):
        """Navega a la página. El Basic Auth lo maneja el contexto automáticamente."""
        page = await self.context.new_page()
        try:
            logger.info("Navegando al mapa principal (Autenticación automática en proceso)...")
            await page.goto(self.base_url, wait_until="networkidle")
            
            # Verificamos que cargó el mapa buscando el id="imap"
            await page.wait_for_selector("map#imap", state="attached", timeout=10000)
            logger.info("Acceso confirmado. Mapa principal cargado.")
            
            return page
        except Exception as e:
            logger.error(f"Error al acceder al mapa principal: {e}")
            await page.close()
            raise

    async def get_site_links(self, page):
        """Extrae los href de las áreas del mapa."""
        logger.info("Extrayendo enlaces de los sites desde <map id='imap'>...")
        
        # En JavaScript, 'el.href' devuelve la URL absoluta, resolviendo la ruta relativa
        links = await page.locator("map#imap area").evaluate_all(
            "elements => elements.map(el => el.href)"
        )
        
        unique_links = list(set(links))
        logger.info(f"Se encontraron {len(unique_links)} sites para procesar.")
        return unique_links

    async def process_site(self, url, semaphore):
        """Navega a un site específico y toma la captura. Devuelve (torre, ruta_png, url) o None."""
        async with semaphore:
            page = await self.context.new_page()
            try:
                # Bloquear imágenes de fondo del propio Intermapper si las hay para ahorrar RAM
                await page.route("**/*.{png,jpg,jpeg}", lambda route: route.continue_())

                logger.info(f"Navegando al site: {url}")

                # Intermapper nos redirigirá a la URL completa del submapa.
                await page.goto(url, wait_until="networkidle")

                # Le damos 2 segundos extra para que los nodos SVG/iconos terminen de renderizar
                await asyncio.sleep(2)

                title = await page.title()
                safe_name = "".join([c if c.isalnum() else "_" for c in title]).strip("_")

                # Si el título está vacío por alguna razón, usamos un hash de la URL
                if not safe_name:
                    safe_name = f"site_{hash(url)}"

                if len(safe_name) > MAX_SCREENSHOT_STEM_BASE:
                    safe_name = safe_name[:MAX_SCREENSHOT_STEM_BASE].rstrip("_")

                final_url = page.url
                map_slug = map_slug_from_intermapper_url(final_url) or fallback_map_slug_from_url(
                    final_url
                )
                screenshot_path = (
                    Config.SCREENSHOT_DIR / f"{safe_name}__intermapper_{map_slug}.png"
                )

                await page.screenshot(path=screenshot_path, full_page=True)
                logger.info(f"📸 Captura guardada: {screenshot_path}")

                tower_name = tower_name_from_screenshot_stem(screenshot_path.stem)
                return (tower_name, screenshot_path, final_url)

            except Exception as e:
                logger.error(f"Error procesando {url}: {e}")
                return None
            finally:
                await page.close()