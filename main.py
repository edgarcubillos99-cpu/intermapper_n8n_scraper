"""Entrypoint unificado del proyecto.

`python main.py` arranca, en el mismo proceso y dependiendo de la config:

  - El servidor MCP (uvicorn) escuchando en MCP_HOST:MCP_PORT.
  - El pipeline de scraping + envío a n8n (modo `once` o `loop`).

Variables de control (en `.env` o entorno):
  - RUN_MCP                  = true|false   (default: true)
  - RUN_PIPELINE             = true|false   (default: true)
  - PIPELINE_MODE            = once|loop    (default: once)
  - PIPELINE_INTERVAL_SECONDS                (default: 3600)
  - PIPELINE_INITIAL_DELAY_SECONDS           (default: 5)
"""
import asyncio
import json
import signal
import time
from pathlib import Path

from src.config import Config
from src.logger import get_logger
from src.n8n_client import send_image_to_n8n
from src.scraper.browser import BrowserManager
from src.scraper.navigator import IntermapperScraper
from src.scraper.tower_naming import tower_name_from_screenshot_stem

logger = get_logger(__name__)


# ----------------------------------------------------------------------------
# Pipeline: Fase 1 (scrape + IPs) + Fase 2 (envío a n8n con throttle)
# ----------------------------------------------------------------------------

async def run_scraper_phase():
    """Fase 1: Navegación, captura de pantallas y recolección de IPs."""
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

    sites_to_process = [r for r in results if r is not None]

    # Fallback: si el scraping en vivo falló pero hay screenshots locales.
    if not sites_to_process:
        for screenshot_path in Config.SCREENSHOT_DIR.glob("*.png"):
            tower_name = tower_name_from_screenshot_stem(screenshot_path.stem)
            sites_to_process.append((tower_name, screenshot_path, None, {}))

    return sites_to_process


def persist_ip_map(sites_to_process: list) -> None:
    """Escribe {torre: {ap_name: ip}} a Config.IP_MAP_PATH para que el MCP
    server lo consuma al recibir datos de n8n."""
    ip_map: dict[str, dict[str, str]] = {}
    for tower_name, _path, _url, devices in sites_to_process:
        if not devices:
            continue
        ip_map.setdefault(tower_name, {}).update(devices)

    Config.IP_MAP_PATH.parent.mkdir(parents=True, exist_ok=True)
    Config.IP_MAP_PATH.write_text(
        json.dumps(ip_map, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    total_ips = sum(len(v) for v in ip_map.values())
    logger.info(
        f"💾 ip_map.json escrito en {Config.IP_MAP_PATH} "
        f"({len(ip_map)} torres, {total_ips} IPs)."
    )


async def run_n8n_phase(sites_to_process: list):
    """Fase 2: Envío de capturas a n8n de forma secuencial (1 imagen / intervalo)."""
    interval = Config.N8N_SEND_INTERVAL_SECONDS
    total = len(sites_to_process)
    logger.info(
        f"--- INICIANDO FASE 2: ENVÍO A N8N (throttle {interval}s entre envíos, {total} pendientes) ---"
    )

    async def _send_one(tower: str, path: Path):
        success = await send_image_to_n8n(tower, path)
        if success:
            try:
                path.unlink(missing_ok=True)
                logger.info(f"[{tower}] 🗑️ Imagen local eliminada para liberar espacio.")
            except Exception as e:
                logger.error(f"[{tower}] ⚠️ No se pudo borrar la imagen local {path.name}: {e}")
        else:
            logger.warning(f"[{tower}] 💾 La imagen se conservó localmente porque falló el envío a n8n.")

    for idx, (tower_name, screenshot_path, _url, _devices) in enumerate(sites_to_process, start=1):
        logger.info(f"📦 Envío {idx}/{total} — torre: {tower_name}")
        await _send_one(tower_name, screenshot_path)

        if idx < total:
            logger.info(f"⏳ Esperando {interval}s antes del siguiente envío...")
            await asyncio.sleep(interval)


async def run_pipeline_once():
    """Una corrida completa del pipeline (Fase 1 + Fase 2)."""
    start_time = time.time()
    Config.setup_directories()

    sites_to_process = await run_scraper_phase()
    logger.info(f"Fase 1 completada. {len(sites_to_process)} capturas listas.")

    persist_ip_map(sites_to_process)

    if sites_to_process:
        await run_n8n_phase(sites_to_process)
    else:
        logger.warning("No hay capturas para enviar.")

    logger.info("=" * 60)
    logger.info(f"🚀 PIPELINE COMPLETADO EN {time.time() - start_time:.2f} SEGUNDOS 🚀")
    logger.info("=" * 60)


# ----------------------------------------------------------------------------
# Ciclo de vida: MCP + pipeline en paralelo
# ----------------------------------------------------------------------------

async def run_mcp_server(stop_event: asyncio.Event):
    """Arranca uvicorn en el mismo loop asyncio. Termina cuando stop_event se setea."""
    import uvicorn
    from src.mcp_server import app

    config = uvicorn.Config(
        app,
        host=Config.MCP_HOST,
        port=Config.MCP_PORT,
        log_level="info",
        # Deshabilitar el manejo de señales de uvicorn — lo gestiona run_all().
        # Así Ctrl+C / SIGTERM se canalizan correctamente por nosotros.
    )
    server = uvicorn.Server(config)
    # Desactivar el handler interno de señales de uvicorn (lo manejamos arriba).
    server.install_signal_handlers = lambda: None  # type: ignore[method-assign]

    logger.info(f"🛰️  Arrancando MCP server en http://{Config.MCP_HOST}:{Config.MCP_PORT}")

    serve_task = asyncio.create_task(server.serve())
    stop_task = asyncio.create_task(stop_event.wait())

    try:
        await asyncio.wait({serve_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        # Pedir shutdown limpio al uvicorn server.
        server.should_exit = True
        try:
            await asyncio.wait_for(serve_task, timeout=10)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            logger.warning("Uvicorn no terminó a tiempo; forzando cancel.")
            serve_task.cancel()
        logger.info("🛰️  MCP server detenido.")


async def run_pipeline_lifecycle(stop_event: asyncio.Event):
    """Ejecuta el pipeline (once o loop) hasta que stop_event se setea."""
    if Config.PIPELINE_INITIAL_DELAY_SECONDS > 0:
        logger.info(
            f"⏱️  Esperando {Config.PIPELINE_INITIAL_DELAY_SECONDS}s antes de iniciar el pipeline..."
        )
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=Config.PIPELINE_INITIAL_DELAY_SECONDS)
            return  # Stop antes de empezar.
        except asyncio.TimeoutError:
            pass  # Timeout esperado → empezamos.

    while not stop_event.is_set():
        try:
            await run_pipeline_once()
        except Exception as e:
            logger.error(f"💥 Pipeline falló: {e!r}", exc_info=True)

        if Config.PIPELINE_MODE != "loop":
            logger.info("Pipeline en modo 'once' completado. El MCP server sigue activo.")
            return

        logger.info(f"⏳ Próxima ronda del pipeline en {Config.PIPELINE_INTERVAL_SECONDS}s...")
        try:
            await asyncio.wait_for(
                stop_event.wait(), timeout=Config.PIPELINE_INTERVAL_SECONDS
            )
            return  # stop_event setado mientras esperábamos.
        except asyncio.TimeoutError:
            continue


async def run_all():
    """Orquestador principal. Lanza MCP y pipeline en paralelo según config."""
    if not Config.RUN_MCP and not Config.RUN_PIPELINE:
        logger.error("RUN_MCP y RUN_PIPELINE están en false. Nada que ejecutar.")
        return

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _request_stop(signame: str):
        if not stop_event.is_set():
            logger.info(f"Señal {signame} recibida; iniciando shutdown limpio...")
            stop_event.set()

    for signame in ("SIGINT", "SIGTERM"):
        try:
            loop.add_signal_handler(getattr(signal, signame), _request_stop, signame)
        except (NotImplementedError, AttributeError):
            # Windows no soporta add_signal_handler en asyncio.
            pass

    tasks: list[asyncio.Task] = []

    if Config.RUN_MCP:
        tasks.append(asyncio.create_task(run_mcp_server(stop_event), name="mcp"))

    if Config.RUN_PIPELINE:
        tasks.append(asyncio.create_task(run_pipeline_lifecycle(stop_event), name="pipeline"))

    logger.info(
        f"🎬 Iniciando ejecución unificada — MCP={'on' if Config.RUN_MCP else 'off'}, "
        f"PIPELINE={'on' if Config.RUN_PIPELINE else 'off'}, "
        f"PIPELINE_MODE={Config.PIPELINE_MODE}"
    )

    try:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for t, r in zip(tasks, results):
            if isinstance(r, BaseException) and not isinstance(r, asyncio.CancelledError):
                logger.error(f"[{t.get_name()}] terminó con excepción: {r!r}")
    finally:
        # Asegura que las tasks pendientes se cancelen al salir.
        stop_event.set()
        for t in tasks:
            if not t.done():
                t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        logger.info("👋 Proceso finalizado.")


def main():
    try:
        asyncio.run(run_all())
    except KeyboardInterrupt:
        logger.info("Interrumpido por usuario.")


if __name__ == "__main__":
    main()
