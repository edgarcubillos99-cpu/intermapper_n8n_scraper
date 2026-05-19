import asyncio
import json
import logging
import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, List

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
from pydantic import BaseModel, Field, WithJsonSchema  # noqa: E402
from starlette.types import ASGIApp, Receive, Scope, Send  # noqa: E402

from src.config import Config  # noqa: E402
from src.db_schema import ensure_intermapper_tables  # noqa: E402
from src.scraper.tower_naming import normalize_ap_name  # noqa: E402

load_dotenv()
logger = logging.getLogger("mcp_server")


# --- MODELO DE DATOS PARA N8N ---
class APDeviceInfo(BaseModel):
    torre: str = Field(description="Nombre de la torre")
    torre_latitud: str | None = Field(default=None, description="Latitud de la torre")
    torre_longitud: str | None = Field(default=None, description="Longitud de la torre")
    # Los siguientes campos ahora son opcionales/tienen valor por defecto vacío
    ap_name: str | None = Field(default="", description="Nombre del Access Point")
    tipo: str | None = Field(default="", description="Tipo de tecnología (ePMP, Rocket AC, etc)")
    azimut: str | None = Field(default="", description="Dirección en grados")
    tilt: str | None = Field(default="", description="Inclinación del equipo")
    altura: str | None = Field(default="", description="Altura en pies (Ft)")

_APS_DATA_JSON_SCHEMA = {
    "type": "array",
    "description": "Lista de APs detectados en el mapa Intermapper para sincronizar.",
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
        },
        # AHORA SOLO LA TORRE ES ESTRICTAMENTE OBLIGATORIA
        "required": ["torre"], 
    },
}


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
    """Espera a que MySQL acepte conexión (p. ej. contenedor recién levantado)."""
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

def _ensure_torres_columns(connection) -> None:
    """ALTER TABLE para añadir `latitud` y `longitud` a torres si no existen.
    Idempotente: si las columnas ya están, no hace nada.
    """
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT COUNT(*) AS c
              FROM information_schema.columns
             WHERE table_schema = DATABASE()
               AND table_name   = 'torres'
               AND column_name  = 'latitud'
            """
        )
        row = cursor.fetchone()
        already = (row or {}).get("c", 0)
        if not already:
            logger.info("Columnas latitud/longitud no existen en 'torres'; ejecutando ALTER TABLE.")
            cursor.execute(
                "ALTER TABLE torres "
                "ADD COLUMN latitud VARCHAR(50) NULL AFTER nombre, "
                "ADD COLUMN longitud VARCHAR(50) NULL AFTER latitud"
            )
    connection.commit()

def _ensure_disp_id_column(connection) -> None:
    """ALTER TABLE para añadir `disp_id` y su FK a dispositivos_ap si no existen.
    Idempotente: previene fallos si las columnas ya fueron creadas.
    """
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT COUNT(*) AS c
              FROM information_schema.columns
             WHERE table_schema = DATABASE()
               AND table_name   = 'dispositivos_ap'
               AND column_name  = 'disp_id'
            """
        )
        row = cursor.fetchone()
        already = (row or {}).get("c", 0)
        if not already:
            logger.info("Columna 'disp_id' no existe en 'dispositivos_ap'; ejecutando ALTER TABLE relacional.")
            cursor.execute(
                "ALTER TABLE dispositivos_ap "
                "ADD COLUMN disp_id INT NULL AFTER id, "
                "ADD CONSTRAINT fk_dispositivos_ap_disp FOREIGN KEY (disp_id) "
                "REFERENCES dispositivos (id) ON DELETE SET NULL"
            )
    connection.commit()

@mcp.tool()
def sync_intermapper_data(
    aps_data: Annotated[List[APDeviceInfo], WithJsonSchema(_APS_DATA_JSON_SCHEMA)],
) -> str:
    """
    Sincroniza los datos extraídos de Intermapper en las tablas 'torres' y 'dispositivos_ap'.
    """
    connection = get_db_connection()
    try:
        _ensure_ip_address_column(connection)
        _ensure_torres_columns(connection)
        _ensure_disp_id_column(connection) # <--- MIGRACIÓN AUTOMÁTICA DE LA NUEVA COLUMNA
        ip_map = _load_ip_map()
        ips_aplicadas = 0
        ips_no_encontradas: list[str] = []

        with connection.cursor() as cursor:
            for ap in aps_data:
                # 1. ACTUALIZAR TORRE Y COORDENADAS
                sql_torre = """
                    INSERT INTO torres (nombre, latitud, longitud)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        latitud = COALESCE(NULLIF(VALUES(latitud), ''), latitud),
                        longitud = COALESCE(NULLIF(VALUES(longitud), ''), longitud)
                """
                cursor.execute(sql_torre, (ap.torre, ap.torre_latitud, ap.torre_longitud))

                ap_name_clean = (ap.ap_name or "").strip()

                # 2. ACTUALIZAR DISPOSITIVOS_AP (Solo si viene con un nombre de AP válido)
                if ap_name_clean:
                    # --- 2.1 OBTENER IP PRIMERO ---
                    # Aprovechamos tu función que maneja variaciones en el nombre
                    ip_address = _lookup_ip(ip_map, ap.torre, ap_name_clean)
                    if ip_address:
                        ips_aplicadas += 1
                    else:
                        ips_no_encontradas.append(f"{ap.torre}/{ap_name_clean}")

                    # --- 2.2 BUSCAR ID RELACIONAL (Priorizando la IP) ---
                    disp_id = None
                    if ip_address:
                        # Si tenemos IP, es el método más exacto para cruzar la tabla
                        cursor.execute(
                            "SELECT id FROM dispositivos WHERE torre = %s AND ip_address = %s LIMIT 1",
                            (ap.torre, ip_address)
                        )
                        row_disp = cursor.fetchone()
                        if row_disp:
                            disp_id = row_disp.get("id")
                    
                    # Fallback por si acaso no tenía IP pero el nombre coincide exactamente
                    if not disp_id:
                        cursor.execute(
                            "SELECT id FROM dispositivos WHERE torre = %s AND dispositivo = %s LIMIT 1",
                            (ap.torre, ap_name_clean)
                        )
                        row_disp = cursor.fetchone()
                        if row_disp:
                            disp_id = row_disp.get("id")
                    # ---------------------------------------------------

                    # --- 2.3 INSERTAR EN DISPOSITIVOS_AP ---
                    sql_upsert = """
                        INSERT INTO dispositivos_ap
                            (disp_id, torre_nombre, ap_name, tipo, azimut, tilt, altura, ip_address)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            disp_id    = VALUES(disp_id),
                            tipo       = VALUES(tipo),
                            azimut     = VALUES(azimut),
                            tilt       = VALUES(tilt),
                            altura     = VALUES(altura),
                            ip_address = COALESCE(VALUES(ip_address), ip_address)
                    """
                    cursor.execute(
                        sql_upsert,
                        (
                            disp_id,
                            ap.torre,
                            ap_name_clean,
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
@asynccontextmanager
async def _app_lifespan(app: FastAPI):
    await _bootstrap_mysql_schema_with_retries()
    yield


app = FastAPI(title="Intermapper MCP Server", lifespan=_app_lifespan)
app.add_middleware(BearerAuthMiddleware)


@app.get("/health")
async def health():
    return {"status": "ok"}


# mcp.sse_app() devuelve una Starlette con las rutas /sse (GET) y /messages/ (POST)
# correctamente cableadas usando SseServerTransport internamente.
app.mount("/", mcp.sse_app())


if __name__ == "__main__":
    uvicorn.run(app, host=MCP_HOST, port=MCP_PORT)
