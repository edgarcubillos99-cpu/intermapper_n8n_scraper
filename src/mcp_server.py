import asyncio
import json
import logging
import os
import sys
import time
from contextlib import AsyncExitStack, asynccontextmanager
from pathlib import Path
from typing import Annotated, List

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import pymysql  # noqa: E402
import uvicorn  # noqa: E402
from dotenv import load_dotenv  # noqa: E402
from fastapi import FastAPI  # noqa: E402
from mcp.server.fastmcp import FastMCP  # noqa: E402
from pydantic import BaseModel, Field, WithJsonSchema  # noqa: E402
from starlette.applications import Starlette  # noqa: E402
from starlette.types import ASGIApp, Receive, Scope, Send  # noqa: E402

from src.config import Config  # noqa: E402
from src.db_schema import ensure_intermapper_tables  # noqa: E402
from src.mcp_sync import sync_intermapper_data_impl  # noqa: E402

load_dotenv()
logger = logging.getLogger("mcp_server")

_start_time = time.monotonic()
_active_syncs = 0
_sync_semaphore: asyncio.Semaphore | None = None


class APDeviceInfo(BaseModel):
    torre: str = Field(description="Nombre de la torre")
    torre_latitud: str | None = Field(default=None, description="Latitud de la torre")
    torre_longitud: str | None = Field(default=None, description="Longitud de la torre")
    ap_name: str | None = Field(default="", description="Nombre del Access Point")
    tipo: str | None = Field(default="", description="Tipo de tecnología (ePMP, Rocket AC, etc)")
    azimut: str | None = Field(default="", description="Dirección en grados")
    tilt: str | None = Field(default="", description="Inclinación del equipo")
    altura: str | None = Field(default="", description="Altura en pies (Ft)")
    contacto1: str | None = Field(default="", description="Primer contacto o teléfono en el plano")
    contacto2: str | None = Field(default="", description="Segundo contacto o teléfono en el plano")
    contacto3: str | None = Field(default="", description="Tercer contacto o teléfono en el plano")
    contacto4: str | None = Field(default="", description="Cuarto contacto o teléfono en el plano")

_APS_DATA_JSON_SCHEMA = {
    "type": "array",
    "description": "Lista de APs y metadatos de la torre detectados en el mapa Intermapper.",
    "items": {
        "type": "object",
        "properties": {
            "torre": {"type": "string", "description": "Nombre de la torre"},
            "torre_latitud": {"type": "string", "description": "Latitud de la torre"},
            "torre_longitud": {"type": "string", "description": "Longitud de la torre"},
            "ap_name": {"type": "string", "description": "Nombre del Access Point"},
            "tipo": {"type": "string", "description": "Tipo de tecnología"},
            "azimut": {"type": "string", "description": "Dirección en grados"},
            "tilt": {"type": "string", "description": "Inclinación del equipo"},
            "altura": {"type": "string", "description": "Altura en pies (Ft)"},
            "contacto1": {"type": "string", "description": "Contacto 1"},
            "contacto2": {"type": "string", "description": "Contacto 2"},
            "contacto3": {"type": "string", "description": "Contacto 3"},
            "contacto4": {"type": "string", "description": "Contacto 4"},
        },
        "required": ["torre"],
    },
}

SECRET_TOKEN = os.getenv("MCP_BEARER_TOKEN")
MCP_HOST = os.getenv("MCP_HOST", "0.0.0.0")
MCP_PORT = int(os.getenv("MCP_PORT", "8000"))
MCP_MAX_CONCURRENT_SYNCS = int(os.getenv("MCP_MAX_CONCURRENT_SYNCS", "6"))
MCP_ALLOWED_HOSTS = os.getenv("MCP_ALLOWED_HOSTS", "").strip()
MCP_ENABLE_SSE = os.getenv("MCP_ENABLE_SSE", "false").strip().lower() in {"1", "true", "yes", "on"}
MCP_ENABLE_STREAMABLE = os.getenv("MCP_ENABLE_STREAMABLE", "true").strip().lower() in {
    "1", "true", "yes", "on"
}
# Stateless: cada petición HTTP es independiente (recomendado para n8n, que abre sesión por llamada).
MCP_STATELESS_HTTP = os.getenv("MCP_STATELESS_HTTP", "true").strip().lower() in {
    "1", "true", "yes", "on"
}
MCP_JSON_RESPONSE = os.getenv("MCP_JSON_RESPONSE", "false").strip().lower() in {
    "1", "true", "yes", "on"
}
# Solo aplica en modo stateful; cierra sesiones SSE inactivas (evita fugas de conexiones).
MCP_SESSION_IDLE_TIMEOUT_RAW = os.getenv("MCP_SESSION_IDLE_TIMEOUT", "120").strip()

mcp = FastMCP(
    "Intermapper_Sync_Service",
    stateless_http=MCP_STATELESS_HTTP,
    json_response=MCP_JSON_RESPONSE,
)

_streamable_app: Starlette | None = None
_sse_app: Starlette | None = None


def _parse_session_idle_timeout() -> float | None:
    if MCP_STATELESS_HTTP or not MCP_SESSION_IDLE_TIMEOUT_RAW:
        return None
    try:
        value = float(MCP_SESSION_IDLE_TIMEOUT_RAW)
    except ValueError:
        logger.warning(
            "MCP_SESSION_IDLE_TIMEOUT inválido (%r); sin expiración de sesiones.",
            MCP_SESSION_IDLE_TIMEOUT_RAW,
        )
        return None
    if value <= 0:
        return None
    return value


def _configure_streamable_session_manager() -> None:
    """Aplica session_idle_timeout al manager (FastMCP no lo expone en Settings)."""
    sm = mcp._session_manager
    if sm is None or sm.stateless:
        return
    idle_timeout = _parse_session_idle_timeout()
    if idle_timeout is not None:
        sm.session_idle_timeout = idle_timeout
        logger.info("MCP sesiones stateful: idle_timeout=%ss", idle_timeout)


def _mcp_active_sessions() -> int:
    sm = mcp._session_manager
    if sm is None or sm.stateless:
        return 0
    return len(getattr(sm, "_server_instances", {}))


def _get_streamable_app() -> Starlette:
    global _streamable_app
    if _streamable_app is None:
        _streamable_app = mcp.streamable_http_app()
        _configure_streamable_session_manager()
        logger.info(
            "MCP /mcp: stateless=%s json_response=%s sse_habilitado=%s",
            MCP_STATELESS_HTTP,
            MCP_JSON_RESPONSE,
            MCP_ENABLE_SSE,
        )
    return _streamable_app


def _get_sse_app() -> Starlette:
    global _sse_app
    if _sse_app is None:
        _sse_app = mcp.sse_app()
    return _sse_app


if MCP_ALLOWED_HOSTS:
    mcp.settings.transport_security.allowed_hosts = [
        h.strip() for h in MCP_ALLOWED_HOSTS.split(",") if h.strip()
    ]
else:
    mcp.settings.transport_security.enable_dns_rebinding_protection = False


def get_db_connection():
    return pymysql.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        database=os.getenv("DB_NAME"),
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=10,
        read_timeout=60,
        write_timeout=60,
    )


def _bootstrap_mysql_schema_once() -> None:
    connection = get_db_connection()
    try:
        ensure_intermapper_tables(connection)
    finally:
        connection.close()


async def _bootstrap_mysql_schema_with_retries(
    max_attempts: int = 15,
    delay_s: float = 2.0,
) -> None:
    global _sync_semaphore
    _sync_semaphore = asyncio.Semaphore(MCP_MAX_CONCURRENT_SYNCS)

    last_err: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            await asyncio.to_thread(_bootstrap_mysql_schema_once)
            return
        except Exception as e:
            last_err = e
            logger.warning(
                "Esquema MySQL no listo (intento %s/%s): %s",
                attempt,
                max_attempts,
                e,
            )
            if attempt < max_attempts:
                await asyncio.sleep(delay_s)
    assert last_err is not None
    logger.error("No se pudo crear/verificar tablas en MySQL tras varios reintentos.")
    raise last_err


def _aps_to_dicts(aps_data: List[APDeviceInfo]) -> list[dict]:
    return [ap.model_dump() if hasattr(ap, "model_dump") else dict(ap) for ap in aps_data]


@mcp.tool()
async def sync_intermapper_data(
    aps_data: Annotated[List[APDeviceInfo], WithJsonSchema(_APS_DATA_JSON_SCHEMA)],
) -> str:
    """
    Sincroniza los datos extraídos de Intermapper en las tablas 'torres' y 'dispositivos_ap'.
    """
    global _active_syncs, _sync_semaphore
    if _sync_semaphore is None:
        _sync_semaphore = asyncio.Semaphore(MCP_MAX_CONCURRENT_SYNCS)

    payload = _aps_to_dicts(aps_data)
    torre = payload[0].get("torre", "?") if payload else "?"
    logger.info("sync_intermapper_data inicio torre=%s registros=%s", torre, len(payload))

    async with _sync_semaphore:
        _active_syncs += 1
        try:
            result = await asyncio.to_thread(
                sync_intermapper_data_impl, payload, get_db_connection
            )
            logger.info("sync_intermapper_data ok torre=%s", torre)
            return result
        finally:
            _active_syncs -= 1


class BearerAuthMiddleware:
    """ASGI puro con protección contra doble http.response.start (corrupción SSE)."""

    PUBLIC_PATHS = {"/health"}
    MCP_PATH_PREFIXES = ("/sse", "/messages", "/mcp")

    def __init__(self, app: ASGIApp):
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "")

        if path in self.PUBLIC_PATHS:
            await self.app(scope, receive, send)
            return

        if not any(path.startswith(p) for p in self.MCP_PATH_PREFIXES):
            await self.app(scope, receive, send)
            return

        if not SECRET_TOKEN:
            await _send_json(
                send,
                500,
                {"detail": "Server token not configured (MCP_BEARER_TOKEN missing)"},
            )
            return

        headers = {
            k.decode("latin-1").lower(): v.decode("latin-1")
            for k, v in scope.get("headers", [])
        }
        auth = headers.get("authorization", "")

        if not auth.lower().startswith("bearer "):
            await _send_json(send, 401, {"detail": "Missing Bearer token"})
            return

        token = auth.split(" ", 1)[1].strip()
        if token != SECRET_TOKEN:
            await _send_json(send, 401, {"detail": "Invalid Token"})
            return

        response_started = False

        async def send_wrapper(message):
            nonlocal response_started
            if message["type"] == "http.response.start":
                if response_started:
                    logger.warning(
                        "ASGI: ignorando response.start duplicado en %s", path
                    )
                    return
                response_started = True
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        except Exception:
            logger.exception("Error ASGI en ruta MCP %s", path)
            if not response_started:
                await _send_json(send, 500, {"detail": "Internal MCP error"})


async def _send_json(send: Send, status: int, body: dict) -> None:
    data = json.dumps(body).encode("utf-8")
    await send(
        {
            "type": "http.response.start",
            "status": status,
            "headers": [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(data)).encode("ascii")),
            ],
        }
    )
    await send({"type": "http.response.body", "body": data})


def _build_mcp_routes() -> list:
    """Rutas MCP en la raíz (/mcp, /sse). Requiere lifespan encadenado en FastAPI."""
    routes = []
    if MCP_ENABLE_STREAMABLE:
        routes.extend(_get_streamable_app().routes)
        logger.info("MCP Streamable HTTP → https://<host>/mcp")
    if MCP_ENABLE_SSE:
        routes.extend(_get_sse_app().routes)
        logger.info("MCP SSE → https://<host>/sse")
    if not routes:
        raise RuntimeError("Debe habilitarse al menos MCP_ENABLE_SSE o MCP_ENABLE_STREAMABLE")
    return routes


@asynccontextmanager
async def _app_lifespan(app: FastAPI):
    """Inicializa MySQL y el task group de Streamable HTTP (obligatorio para /mcp)."""
    await _bootstrap_mysql_schema_with_retries()

    async with AsyncExitStack() as stack:
        if MCP_ENABLE_STREAMABLE:
            streamable = _get_streamable_app()
            await stack.enter_async_context(streamable.router.lifespan_context(streamable))
            logger.info("Streamable HTTP session manager iniciado.")
        yield


app = FastAPI(title="Intermapper MCP Server", lifespan=_app_lifespan)


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "uptime_s": round(time.monotonic() - _start_time, 1),
        "active_syncs": _active_syncs,
        "mcp_active_sessions": _mcp_active_sessions(),
        "mcp_stateless": MCP_STATELESS_HTTP,
        "mcp_sse": MCP_ENABLE_SSE,
        "mcp_streamable": MCP_ENABLE_STREAMABLE,
    }


mcp_starlette = Starlette(routes=_build_mcp_routes())
app.mount("/", BearerAuthMiddleware(mcp_starlette))


if __name__ == "__main__":
    uvicorn.run(app, host=MCP_HOST, port=MCP_PORT)
