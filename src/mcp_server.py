import os
from typing import List

import pymysql
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI
from mcp.server.fastmcp import FastMCP
from pydantic import BaseModel, Field
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

load_dotenv()


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


@mcp.tool()
def sync_intermapper_data(aps_data: List[APDeviceInfo]) -> str:
    """
    Sincroniza los datos extraídos de Intermapper en las tablas 'torres' y 'dispositivos_ap'.
    """
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            for ap in aps_data:
                cursor.execute("INSERT IGNORE INTO torres (nombre) VALUES (%s)", (ap.torre,))

                sql_upsert = """
                    INSERT INTO dispositivos_ap (torre_nombre, ap_name, tipo, azimut, tilt, altura)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        tipo=VALUES(tipo), azimut=VALUES(azimut),
                        tilt=VALUES(tilt), altura=VALUES(altura)
                """
                cursor.execute(
                    sql_upsert,
                    (ap.torre, ap.ap_name, ap.tipo, ap.azimut, ap.tilt, ap.altura),
                )

        connection.commit()
        return f"Sincronizados {len(aps_data)} dispositivos exitosamente."
    except Exception as e:
        connection.rollback()
        return f"Error: {str(e)}"
    finally:
        connection.close()


# --- MIDDLEWARE BEARER ---
class BearerAuthMiddleware(BaseHTTPMiddleware):
    """Valida el header Authorization: Bearer <token> en todas las rutas
    excepto las explícitamente públicas."""

    PUBLIC_PATHS = {"/health"}

    async def dispatch(self, request: Request, call_next):
        if request.url.path in self.PUBLIC_PATHS:
            return await call_next(request)

        if not SECRET_TOKEN:
            return JSONResponse(
                {"detail": "Server token not configured (MCP_BEARER_TOKEN missing)"},
                status_code=500,
            )

        auth = request.headers.get("authorization", "")
        if not auth.lower().startswith("bearer "):
            return JSONResponse({"detail": "Missing Bearer token"}, status_code=401)

        token = auth.split(" ", 1)[1].strip()
        if token != SECRET_TOKEN:
            return JSONResponse({"detail": "Invalid Token"}, status_code=401)

        return await call_next(request)


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
