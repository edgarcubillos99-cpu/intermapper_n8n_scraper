# Imagen oficial de Playwright para Python: incluye Chromium y todas
# las dependencias del sistema necesarias (libnss, libxss, fontconfig, etc.).
# La versión debe matchear la del paquete `playwright` en requirements.txt.
FROM mcr.microsoft.com/playwright/python:v1.59.0-noble

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    MCP_HOST=0.0.0.0 \
    MCP_PORT=8000 \
    RUN_MCP=true \
    RUN_PIPELINE=true \
    PIPELINE_MODE=once \
    DATA_DIR=/app/data \
    SCREENSHOT_DIR=/app/screenshots

WORKDIR /app

# Instala primero las dependencias para aprovechar la cache de Docker.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copia el código del proyecto.
COPY . .

# Asegura los directorios persistibles (en runtime también los crea Config.setup_directories()).
RUN mkdir -p "${DATA_DIR}" "${SCREENSHOT_DIR}"

# El MCP server expone /health sin auth para chequeos de salud.
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD python -c "import urllib.request,sys; \
                   r=urllib.request.urlopen('http://127.0.0.1:${MCP_PORT}/health',timeout=3); \
                   sys.exit(0 if r.status==200 else 1)" || exit 1

EXPOSE 8000

CMD ["python", "main.py"]
