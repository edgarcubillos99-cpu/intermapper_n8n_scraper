from playwright.async_api import async_playwright
from src.config import Config
from src.logger import get_logger

logger = get_logger(__name__)

class BrowserManager:
    def __init__(self):
        self.playwright = None
        self.browser = None
        self.context = None

    async def start(self):
        logger.info("Iniciando Playwright y navegador Chromium...")
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(headless=True)
        
        # Pasamos las credenciales directamente al contexto para manejar el Basic Auth
        self.context = await self.browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            http_credentials={
                'username': Config.USERNAME,
                'password': Config.PASSWORD
            }
        )
        return self.context

    async def stop(self):
        logger.info("Cerrando navegador...")
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()