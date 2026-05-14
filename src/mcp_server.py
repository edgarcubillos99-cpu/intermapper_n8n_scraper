import json
import logging
import os
import sys
from pathlib import Path
from typing import List

# Permite ejecutar este archivo directamente (`python src/mcp_server.py`)
# añadiendo la raíz del proyecto al sys.path para resolver `from src...`.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import pymysql  # noqa: E402
import uvicorn  # noqa: E402
from dotenv import load_dotenv  # noqa: E402
from fastapi import FastAPI  # noqa: E402
from mcp.server.fastmcp import FastMCP  # noqa: E402
from pydantic import BaseModel, Field  # noqa: E402
from starlette.types import ASGIApp, Receive, Scope, Send  # noqa: E402

from src.config import Config  # noqa: E402
from src.scraper.tower_naming import normalize_ap_name  # noqa: E402

load_dotenv()
logger = logging.getLogger("mcp_server")


# --- MODELO DE DATOS PARA N8N ---
class APDeviceInfo(BaseModel):
    torre: str = Field(description="Nombre de la torre")
    ap_name: str = Field(description="Nombre del Access Point")
    tipo: str = Field(description="Tipo de tecnología (ePMP, Rocket AC, etc)")
    azimut: str = Field(description="Dirección en grados")
    tilt: str = Field(description="Inclinación del equipo")
    altura: str = Field(description="Altura en pies (Ft)")


# --- CONFIG / SEGURIDAD ---
SECRET_TOKEN = os.getenv("MCP_BEARER_TOKEN")
MCP_HOST = os.getenv("MCP_HOST", "0.0.0.0")
MCP_PORT = int(os.getenv("MCP_PORT", "8000"))

# Lista separada por comas de hosts permitidos por la protección DNS-rebinding
# del SDK (p.ej. "midominio.com,1.2.3.4:8000"). Si está vacío, se deshabilita
# esa protección (aceptable porque ya validamos el Bearer token).
MCP_ALLOWED_HOSTS = os.getenv("MCP_ALLOWED_HOSTS", "").strip()


# --- SERVIDOR MCP ---
mcp = FastMCP("Intermapper_Sync_Service")

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
    )


# --- HELPERS PARA IP_ADDRESS ---
def _load_ip_map() -> dict:
    """Carga {torre: {ap_name_completo: ip}} desde ip_map.json (escrito por Fase 1).
    Si el archivo no existe o está corrupto, devuelve {} sin fallar.
    """
    path = Config.IP_MAP_PATH
    if not path.exists():
        logger.warning(f"ip_map.json no existe en {path}; las IPs quedarán NULL.")
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        logger.error(f"No se pudo parsear {path}: {e}")
        return {}


def _lookup_ip(ip_map: dict, torre: str, ap_name: str) -> str | None:
    """Busca la IP usando match exacto y, como fallback, match por token prefijo
    (e.g. 'OSNAP22-A' coincide con 'OSNAP22-A (Lite AC)').
    """
    tower_map = ip_map.get(torre)
    if not tower_map:
        return None
    if ap_name in tower_map:
        return tower_map[ap_name]
    target = normalize_ap_name(ap_name)
    if not target:
        return None
    for fullname, ip in tower_map.items():
        if normalize_ap_name(fullname) == target:
            return ip
    return None


def _ensure_ip_address_column(connection) -> None:
    """ALTER TABLE para añadir `ip_address VARCHAR(45)` si no existe.
    Idempotente: si la columna ya está, no hace nada.
    """
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT COUNT(*) AS c
              FROM information_schema.columns
             WHERE table_schema = DATABASE()
               AND table_name   = 'dispositivos_ap'
               AND column_name  = 'ip_address'
            """
        )
        row = cursor.fetchone()
        already = (row or {}).get("c", 0)
        if not already:
            logger.info("Columna 'ip_address' no existe; ejecutando ALTER TABLE.")
            cursor.execute(
                "ALTER TABLE dispositivos_ap "
                "ADD COLUMN ip_address VARCHAR(45) NULL AFTER altura"
            )
    connection.commit()


@mcp.tool()
def sync_intermapper_data(aps_data: List[APDeviceInfo]) -> str:
    """
    Sincroniza los datos extraídos de Intermapper en las tablas 'torres' y 'dispositivos_ap'.

    Además de los campos enviados por n8n, completa `ip_address` haciendo lookup
    en ip_map.json (generado por la fase 1 del scraper). El match entre el
    `ap_name` de n8n y el nombre que aparece en Intermapper es por token prefijo
    (case-insensitive), p.ej. 'OSNAP22-A' coincide con 'OSNAP22-A (Lite AC)'.
    """
    connection = get_db_connection()
    try:
        _ensure_ip_address_column(connection)
        ip_map = _load_ip_map()
        ips_aplicadas = 0
        ips_no_encontradas: list[str] = []

        with connection.cursor() as cursor:
            for ap in aps_data:
                cursor.execute("INSERT IGNORE INTO torres (nombre) VALUES (%s)", (ap.torre,))

                ip_address = _lookup_ip(ip_map, ap.torre, ap.ap_name)
                if ip_address:
                    ips_aplicadas += 1
                else:
                    ips_no_encontradas.append(f"{ap.torre}/{ap.ap_name}")

                # COALESCE(VALUES(ip_address), ip_address) preserva la IP existente
                # si en este sync no se pudo determinar una nueva.
                sql_upsert = """
                    INSERT INTO dispositivos_ap
                        (torre_nombre, ap_name, tipo, azimut, tilt, altura, ip_address)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        tipo       = VALUES(tipo),
                        azimut     = VALUES(azimut),
                        tilt       = VALUES(tilt),
                        altura     = VALUES(altura),
                        ip_address = COALESCE(VALUES(ip_address), ip_address)
                """
                cursor.execute(
                    sql_upsert,
                    (
                        ap.torre,
                        ap.ap_name,
                        ap.tipo,
                        ap.azimut,
                        ap.tilt,
                        ap.altura,
                        ip_address,
                    ),
                )

        connection.commit()

        if ips_no_encontradas:
            logger.warning(
                f"{len(ips_no_encontradas)} APs sin IP encontrada en ip_map.json: "
                + ", ".join(ips_no_encontradas[:10])
                + (" ..." if len(ips_no_encontradas) > 10 else "")
            )

        return (
            f"Sincronizados {len(aps_data)} dispositivos exitosamente. "
            f"IPs aplicadas: {ips_aplicadas}/{len(aps_data)}."
        )
    except Exception as e:
        connection.rollback()
        return f"Error: {str(e)}"
    finally:
        connection.close()


# --- MIDDLEWARE BEARER (ASGI puro, compatible con streaming/SSE) ---
class BearerAuthMiddleware:
    """ASGI middleware puro: NO usa Starlette BaseHTTPMiddleware porque éste
    rompe respuestas streaming como las de SSE (issue conocido, asserts en
    starlette/middleware/base.py al cerrar el stream).

    Valida el header `Authorization: Bearer <token>` en todas las rutas
    excepto las explícitamente públicas.
    """

    PUBLIC_PATHS = {"/health"}

    def __init__(self, app: ASGIApp):
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        if scope.get("path") in self.PUBLIC_PATHS:
            await self.app(scope, receive, send)
            return

        if not SECRET_TOKEN:
            await _send_json(
                send,
                500,
                {"detail": "Server token not configured (MCP_BEARER_TOKEN missing)"},
            )
            return

        headers = {k.decode("latin-1").lower(): v.decode("latin-1") for k, v in scope.get("headers", [])}
        auth = headers.get("authorization", "")

        if not auth.lower().startswith("bearer "):
            await _send_json(send, 401, {"detail": "Missing Bearer token"})
            return

        token = auth.split(" ", 1)[1].strip()
        if token != SECRET_TOKEN:
            await _send_json(send, 401, {"detail": "Invalid Token"})
            return

        await self.app(scope, receive, send)


async def _send_json(send: Send, status: int, body: dict) -> None:
    """Helper: emite una respuesta JSON corta vía ASGI puro."""
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


# --- EXPOSICIÓN FASTAPI ---
app = FastAPI(title="Intermapper MCP Server")
app.add_middleware(BearerAuthMiddleware)


@app.get("/health")
async def health():
    return {"status": "ok"}


# mcp.sse_app() devuelve una Starlette con las rutas /sse (GET) y /messages/ (POST)
# correctamente cableadas usando SseServerTransport internamente.
app.mount("/", mcp.sse_app())


if __name__ == "__main__":
    uvicorn.run(app, host=MCP_HOST, port=MCP_PORT)
