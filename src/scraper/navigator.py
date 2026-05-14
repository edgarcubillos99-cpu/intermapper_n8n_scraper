import asyncio
import re
from urllib.parse import urljoin

from src.config import Config
from src.logger import get_logger
from src.scraper.tower_naming import (
    MAX_SCREENSHOT_STEM_BASE,
    fallback_map_slug_from_url,
    map_slug_from_intermapper_url,
    tower_name_from_screenshot_stem,
)

logger = get_logger(__name__)

# Regex para extraer la IPv4 que aparece tras el label "Address:" en !device.html
_ADDRESS_RE = re.compile(
    r"Address:\s*</font>\s*([0-9]{1,3}(?:\.[0-9]{1,3}){3})",
    re.IGNORECASE,
)
# Filtro: solo procesar dispositivos cuyo nombre visible NO sea solo una IP
_LOOKS_LIKE_IP = re.compile(r"^\s*[0-9]{1,3}(?:\.[0-9]{1,3}){3}\s*$")

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
        """Navega a un site específico, toma la captura y recolecta IPs de sus
        dispositivos. Devuelve (torre, ruta_png, url, devices_ip_dict) o None.

        devices_ip_dict tiene la forma {ap_name_completo: ip_address}.
        """
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

                # Reutilizamos la misma página para entrar al Device List y a
                # cada dispositivo. Es secuencial dentro del site para no abrir
                # docenas de pestañas en paralelo.
                devices_ips = await self._collect_device_ips(page, final_url, tower_name)

                return (tower_name, screenshot_path, final_url, devices_ips)

            except Exception as e:
                logger.error(f"Error procesando {url}: {e}")
                return None
            finally:
                await page.close()

    async def _collect_device_ips(self, page, submap_url: str, tower_name: str) -> dict:
        """Navega al device_list.html del submapa, entra a cada dispositivo con
        enlace a !device.html y devuelve {ap_name_completo: ip}.

        No falla el proceso si algún dispositivo no se puede leer; loggea y sigue.
        """
        ip_map: dict[str, str] = {}
        try:
            # Construimos la URL absoluta del Device List desde la URL del submapa.
            device_list_url = urljoin(submap_url, "device_list.html?REFRESH=30+Seconds")
            logger.info(f"[{tower_name}] 🔎 Abriendo Device List: {device_list_url}")
            await page.goto(device_list_url, wait_until="domcontentloaded")

            # Extraemos (nombre_visible, href_absoluto) de cada enlace de dispositivo.
            # Los enlaces de dispositivo tienen path .../device/.../!device.html
            devices = await page.locator("a[href*='/device/'][href*='!device.html']").evaluate_all(
                "els => els.map(a => ({ name: (a.textContent || '').trim(), href: a.href }))"
            )
            # Quitamos duplicados conservando el orden
            seen = set()
            unique_devices = []
            for d in devices:
                key = (d["name"], d["href"])
                if key in seen:
                    continue
                seen.add(key)
                unique_devices.append(d)

            logger.info(f"[{tower_name}] {len(unique_devices)} dispositivos detectados en Device List.")
        except Exception as e:
            logger.warning(f"[{tower_name}] ⚠️ No se pudo abrir Device List ({e}); sin IPs para esta torre.")
            return ip_map

        for dev in unique_devices:
            ap_name = dev["name"]
            href = dev["href"]

            if not ap_name:
                continue
            # Saltamos filas cuyo "nombre" es ya una IP (no son APs nombrados)
            if _LOOKS_LIKE_IP.match(ap_name):
                continue

            try:
                await page.goto(href, wait_until="domcontentloaded")
                html = await page.content()
                m = _ADDRESS_RE.search(html)
                if m:
                    ip = m.group(1)
                    ip_map[ap_name] = ip
                    logger.info(f"[{tower_name}]    └─ {ap_name} → {ip}")
                else:
                    logger.warning(f"[{tower_name}]    └─ {ap_name} sin Address visible en !device.html")
            except Exception as e:
                logger.warning(f"[{tower_name}]    └─ Error leyendo {ap_name}: {e}")

        return ip_map