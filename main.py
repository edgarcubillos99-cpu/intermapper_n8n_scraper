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
import os
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

    sites = await scraper.get_site_links(page)
    await page.close()

    semaphore = asyncio.Semaphore(Config.WORKERS)

    async def _process_with_retries(site_info: dict):
        last_err = None
        for attempt in range(1, Config.SITE_PROCESS_RETRIES + 2):
            result = await scraper.process_site(site_info, semaphore)
            if result is not None:
                return result
            last_err = site_info.get("href")
            if attempt <= Config.SITE_PROCESS_RETRIES:
                wait_s = min(2 * attempt, 8)
                logger.warning(
                    f"Reintento {attempt}/{Config.SITE_PROCESS_RETRIES} para site {last_err} "
                    f"(espera {wait_s}s)"
                )
                await asyncio.sleep(wait_s)
        logger.error(f"Site descartado tras reintentos: {last_err}")
        return None

    tasks = [_process_with_retries(site) for site in sites]
    results = await asyncio.gather(*tasks)

    await browser_manager.stop()

    sites_to_process = [r for r in results if r is not None]
    failed = len(sites) - len(sites_to_process)
    if failed:
        logger.warning(
            f"{failed}/{len(sites)} sites no se procesaron (revisa logs de red/timeout)."
        )

    # Fallback: si el scraping en vivo falló pero hay screenshots locales.
    if not sites_to_process:
        for screenshot_path in Config.SCREENSHOT_DIR.glob("*.png"):
            tower_name = tower_name_from_screenshot_stem(screenshot_path.stem)
            sites_to_process.append((tower_name, screenshot_path, None, {}))

    return sites_to_process


def _load_existing_ip_map() -> dict[str, dict[str, str]]:
    if not Config.IP_MAP_PATH.exists():
        return {}
    try:
        data = json.loads(Config.IP_MAP_PATH.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception as e:
        logger.warning(f"No se pudo leer ip_map.json previo ({e}); se creará uno nuevo.")
        return {}


def persist_ip_map(sites_to_process: list) -> dict:
    """Fusiona {torre: {ap_name: ip}} con el archivo previo y escribe ip_map.json."""
    ip_map: dict[str, dict[str, str]] = _load_existing_ip_map()
    torres_esta_corrida = 0
    torres_sin_ips = 0

    for tower_name, _path, _url, devices in sites_to_process:
        torres_esta_corrida += 1
        bucket = ip_map.setdefault(tower_name, {})
        if devices:
            bucket.update(devices)
        else:
            torres_sin_ips += 1

    sorted_map = {
        torre: dict(sorted(dispositivos.items()))
        for torre, dispositivos in sorted(ip_map.items(), key=lambda x: x[0].lower())
    }

    Config.IP_MAP_PATH.parent.mkdir(parents=True, exist_ok=True)
    Config.IP_MAP_PATH.write_text(
        json.dumps(sorted_map, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    try:
        from src.mcp_sync import invalidate_ip_map_cache
        invalidate_ip_map_cache()
    except Exception:
        pass
    total_ips = sum(len(v) for v in sorted_map.values())
    logger.info(
        f"💾 ip_map.json escrito en {Config.IP_MAP_PATH} "
        f"({len(sorted_map)} torres, {total_ips} IPs; "
        f"esta corrida: {torres_esta_corrida} torres, {torres_sin_ips} sin IPs nuevas)."
    )
    return sorted_map


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
    """Una corrida completa del pipeline (Fase 1 + Fase Intermedia + Fase 2)."""
    start_time = time.time()
    Config.setup_directories()

    sites_to_process = await run_scraper_phase()
    logger.info(f"Fase 1 completada. {len(sites_to_process)} capturas listas.")

    # Almacenamos el JSON y mantenemos el mapa en memoria
    ip_map = persist_ip_map(sites_to_process)

    # --- NUEVA FASE INTERMEDIA: Sincronización en BD en Background ---
    def _sync_db_phase():
        from src.mcp_server import get_db_connection
        from src.db_schema import ensure_intermapper_tables, sync_all_devices
        conn = get_db_connection()
        try:
            ensure_intermapper_tables(conn)
            if ip_map:
                sync_all_devices(conn, ip_map)
        finally:
            conn.close()

    logger.info("--- INICIANDO FASE INTERMEDIA: SINCRONIZACIÓN DE DISPOSITIVOS EN BD ---")
    try:
        # Se ejecuta en un Threadpool para evitar bloquear el ciclo de eventos asíncrono del MCP
        await asyncio.to_thread(_sync_db_phase)
    except Exception as e:
        logger.error(f"⚠️ Error en la sincronización de dispositivos a BD: {e}", exc_info=True)
    # -----------------------------------------------------------------

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
        timeout_keep_alive=30,
        limit_concurrency=int(os.getenv("MCP_LIMIT_CONCURRENCY", "200")),
    )
    server = uvicorn.Server(config)
    server.install_signal_handlers = lambda: None  

    logger.info(f"🛰️  Arrancando MCP server en http://{Config.MCP_HOST}:{Config.MCP_PORT}")

    serve_task = asyncio.create_task(server.serve())
    stop_task = asyncio.create_task(stop_event.wait())

    try:
        await asyncio.wait({serve_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)
    finally:
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
            return
        except asyncio.TimeoutError:
            pass 

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
            return 
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