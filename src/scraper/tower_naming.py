import hashlib
import re
from urllib.parse import urlparse

_STEM_MAP_SUFFIX = re.compile(r"__intermapper_([A-Za-z0-9]+)$", re.IGNORECASE)
_SLUG_FROM_PATH = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{2,63}$")
_MAX_TOWER_NOMBRE = 150
MAX_SCREENSHOT_STEM_BASE = 180


def map_slug_from_intermapper_url(url: str) -> str:
    """Primer segmento del path (p. ej. g55b3f3c4); solo alfanuméricos para usar en nombres de archivo."""
    if not (url or "").strip():
        return ""
    try:
        parts = [p for p in urlparse(url).path.strip("/").split("/") if p]
        if not parts:
            return ""
        first = parts[0]
        if not _SLUG_FROM_PATH.match(first):
            return ""
        return re.sub(r"[^A-Za-z0-9]", "", first)
    except Exception:
        return ""


def fallback_map_slug_from_url(url: str) -> str:
    """Hash corto si no pudimos extraer slug del path (URLs raras)."""
    h = hashlib.sha256(url.encode("utf-8", errors="replace")).hexdigest()
    return f"x{h[:12]}"


def tower_name_from_screenshot_stem(stem: str) -> str:
    """Nombre lógico de torre para BD/OCR (sin id de Intermapper).

    El sufijo __intermapper_* solo existe en el nombre de archivo para evitar
    colisiones entre capturas; aquí se ignora para que el nombre en BD siga
    siendo el del mapa (p. ej. «Inter»), como antes.
    """
    m = _STEM_MAP_SUFFIX.search(stem)
    if m:
        stem = stem[: m.start()]

    tower_name = re.sub(
        r"^(Map_and_Charts__|Map__)",
        "",
        stem,
        flags=re.IGNORECASE,
    )
    tower_name = tower_name.replace("_", " ").strip()
    return tower_name[:_MAX_TOWER_NOMBRE]
