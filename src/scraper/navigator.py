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

_ADDRESS_RE = re.compile(
    r"Address:\s*</font>\s*([0-9]{1,3}(?:\.[0-9]{1,3}){3})",
    re.IGNORECASE,
)
_LOOKS_LIKE_IP = re.compile(r"^\s*[0-9]{1,3}(?:\.[0-9]{1,3}){3}\s*$")
_GENERIC_IP_RE = re.compile(r"\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b")

_DEVICE_LINK_SELECTORS = (
    "a[href*='/device/'][href*='!device.html']",
    "a[href*='!device.html']",
)


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

            await page.wait_for_selector("map#imap", state="attached", timeout=10000)
            logger.info("Acceso confirmado. Mapa principal cargado.")

            return page
        except Exception as e:
            logger.error(f"Error al acceder al mapa principal: {e}")
            await page.close()
            raise

    async def get_site_links(self, page):
        """Extrae sites del mapa principal con href y etiqueta (title/alt), ordenados."""
        logger.info("Extrayendo enlaces de los sites desde <map id='imap'>...")

        sites = await page.locator("map#imap area").evaluate_all(
            """elements => elements.map(el => ({
                href: el.href,
                label: (el.getAttribute('title') || el.getAttribute('alt') || '').trim()
            }))"""
        )

        seen_hrefs: set[str] = set()
        unique_sites: list[dict] = []
        for site in sites:
            href = (site.get("href") or "").strip()
            if not href or href in seen_hrefs:
                continue
            seen_hrefs.add(href)
            unique_sites.append({"href": href, "label": site.get("label") or ""})

        unique_sites.sort(key=lambda s: s["href"])
        logger.info(f"Se encontraron {len(unique_sites)} sites para procesar.")
        return unique_sites

    def _tower_name_candidates(self, page_title: str, area_label: str, screenshot_stem: str) -> str:
        """Elige el nombre de torre más estable entre título de página, área del mapa y archivo."""
        from_stem = tower_name_from_screenshot_stem(screenshot_stem)
        label = (area_label or "").strip()
        title = re.sub(r"^(Map and Charts|Map)\s*[-–—:]\s*", "", page_title or "", flags=re.I).strip()
        title = title.replace("_", " ").strip()

        for candidate in (label, title, from_stem):
            if candidate and len(candidate) >= 2:
                return candidate[:150]
        return from_stem

    async def process_site(self, site_info: dict, semaphore):
        """Navega a un site, captura pantalla y recolecta IPs.

        site_info: {"href": str, "label": str}
        Devuelve (torre, ruta_png, url, devices_ip_dict) o None.
        """
        url = site_info["href"]
        area_label = site_info.get("label") or ""

        async with semaphore:
            page = await self.context.new_page()
            try:
                logger.info(f"Navegando al site: {url}")
                await page.goto(url, wait_until="networkidle", timeout=90000)
                await page.wait_for_load_state("domcontentloaded")
                await asyncio.sleep(2)

                title = await page.title()
                safe_name = "".join([c if c.isalnum() else "_" for c in title]).strip("_")
                if not safe_name:
                    safe_name = f"site_{abs(hash(url)) % 10**8}"

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

                tower_name = self._tower_name_candidates(
                    title, area_label, screenshot_path.stem
                )
                devices_ips = await self._collect_device_ips(page, final_url, tower_name)

                return (tower_name, screenshot_path, final_url, devices_ips)

            except Exception as e:
                logger.error(f"Error procesando {url}: {e}")
                return None
            finally:
                await page.close()

    async def _collect_device_ips(self, page, submap_url: str, tower_name: str) -> dict:
        """Navega al device_list.html del submapa y extrae IPs de forma estable."""
        ip_map: dict[str, str] = {}
        device_list_url = urljoin(submap_url, "device_list.html?REFRESH=30+Seconds")

        try:
            logger.info(f"[{tower_name}] 🔎 Abriendo Device List: {device_list_url}")
            await page.goto(
                device_list_url,
                wait_until="networkidle",
                timeout=Config.DEVICE_LIST_TIMEOUT_MS,
            )
            await page.wait_for_load_state("domcontentloaded")
            await asyncio.sleep(2)

            devices: list[dict] = []
            for selector in _DEVICE_LINK_SELECTORS:
                found = await page.locator(selector).evaluate_all(
                    """els => els.map(a => {
                        const tr = a.closest('tr');
                        return {
                            name: (a.textContent || '').trim(),
                            href: a.href,
                            row_text: tr ? (tr.innerText || tr.textContent || '').trim() : ''
                        };
                    })"""
                )
                if found:
                    devices = found
                    break

            seen: set[tuple[str, str]] = set()
            unique_devices: list[dict] = []
            for d in devices:
                key = (d["name"], d["href"])
                if key in seen:
                    continue
                seen.add(key)
                unique_devices.append(d)

            logger.info(f"[{tower_name}] {len(unique_devices)} dispositivos listados.")
        except Exception as e:
            logger.warning(f"[{tower_name}] ⚠️ No se pudo cargar Device List ({e}).")
            return ip_map

        for dev in unique_devices:
            ap_name = dev["name"]
            href = dev["href"]
            row_text = dev.get("row_text", "")

            if not ap_name or _LOOKS_LIKE_IP.match(ap_name):
                continue

            found_ips = _GENERIC_IP_RE.findall(row_text)
            valid_ips = [ip for ip in found_ips if ip != ap_name]

            if valid_ips:
                ip_map[ap_name] = valid_ips[0]
                logger.info(f"[{tower_name}]    └─ {ap_name} → {valid_ips[0]} (tabla)")
                continue

            max_retries = 3
            for attempt in range(max_retries):
                try:
                    await page.goto(href, wait_until="domcontentloaded", timeout=20000)
                    html = await page.content()
                    m = _ADDRESS_RE.search(html)
                    if m:
                        ip_map[ap_name] = m.group(1)
                        logger.info(f"[{tower_name}]    └─ {ap_name} → {m.group(1)}")
                    else:
                        logger.warning(f"[{tower_name}]    └─ {ap_name} sin IP en subpágina.")
                    break
                except Exception as e:
                    if attempt < max_retries - 1:
                        await asyncio.sleep(1.5)
                    else:
                        logger.warning(
                            f"[{tower_name}]    └─ Error leyendo {ap_name} tras reintentos: {e}"
                        )

            try:
                await page.goto(
                    device_list_url,
                    wait_until="domcontentloaded",
                    timeout=30000,
                )
            except Exception:
                pass

        return ip_map
