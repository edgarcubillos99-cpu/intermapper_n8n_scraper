"""Lógica de sincronización MCP → MySQL (ejecutar en thread pool)."""
from __future__ import annotations

import json
import logging
import threading
from pathlib import Path

from src.config import Config
from src.contact_utils import contactos_from_ap_fields
from src.db_schema import ensure_intermapper_tables
from src.scraper.tower_naming import normalize_ap_name

logger = logging.getLogger("mcp_sync")

_schema_ready = False
_schema_lock = threading.Lock()
_ip_map_cache: dict | None = None
_ip_map_mtime: float = 0.0
_ip_map_lock = threading.Lock()


def _load_ip_map_cached() -> dict:
    global _ip_map_cache, _ip_map_mtime
    path = Config.IP_MAP_PATH
    if not path.exists():
        return {}
    mtime = path.stat().st_mtime
    with _ip_map_lock:
        if _ip_map_cache is not None and mtime == _ip_map_mtime:
            return _ip_map_cache
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            _ip_map_cache = data if isinstance(data, dict) else {}
            _ip_map_mtime = mtime
            return _ip_map_cache
        except Exception as e:
            logger.error("No se pudo parsear %s: %s", path, e)
            return {}


def _lookup_ip(ip_map: dict, torre: str, ap_name: str) -> str | None:
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


def _ensure_schema_once(connection) -> None:
    global _schema_ready
    if _schema_ready:
        return
    with _schema_lock:
        if _schema_ready:
            return
        ensure_intermapper_tables(connection)
        _schema_ready = True


def _resolve_id_torre(cursor, torre_nombre: str) -> int | None:
    cursor.execute("SELECT id FROM torres WHERE nombre = %s", (torre_nombre,))
    row = cursor.fetchone()
    return row.get("id") if row else None


def _sync_contactos_for_torre(cursor, id_torre: int, ap: dict) -> None:
    filas = contactos_from_ap_fields(
        ap.get("contacto1"),
        ap.get("contacto2"),
        ap.get("contacto3"),
        ap.get("contacto4"),
    )
    if not filas:
        return
    for numero, nombre in filas:
        cursor.execute(
            """
            SELECT id FROM contactos
             WHERE id_torre = %s AND numero <=> %s AND nombre <=> %s
             LIMIT 1
            """,
            (id_torre, numero or None, nombre or None),
        )
        existing = cursor.fetchone()
        if existing:
            cursor.execute(
                """
                UPDATE contactos
                   SET numero = COALESCE(NULLIF(%s, ''), numero),
                       nombre = COALESCE(NULLIF(%s, ''), nombre)
                 WHERE id = %s
                """,
                (numero, nombre, existing["id"]),
            )
        else:
            cursor.execute(
                "INSERT INTO contactos (id_torre, numero, nombre) VALUES (%s, %s, %s)",
                (id_torre, numero or None, nombre or None),
            )


def _resolve_disp_id(
    cursor, id_torre: int, ap_name: str, ip_address: str | None
) -> int | None:
    if ip_address:
        cursor.execute(
            "SELECT id FROM dispositivos WHERE id_torre = %s AND ip_address = %s LIMIT 1",
            (id_torre, ip_address),
        )
        row = cursor.fetchone()
        if row:
            return row.get("id")
    cursor.execute(
        "SELECT id FROM dispositivos WHERE id_torre = %s AND dispositivo = %s LIMIT 1",
        (id_torre, ap_name),
    )
    row = cursor.fetchone()
    return row.get("id") if row else None


def sync_intermapper_data_impl(aps_data: list[dict], get_db_connection) -> str:
    """Cuerpo síncrono de la herramienta MCP (no bloquear el event loop)."""
    connection = get_db_connection()
    try:
        _ensure_schema_once(connection)
        ip_map = _load_ip_map_cached()
        ips_aplicadas = 0
        ips_no_encontradas: list[str] = []
        contactos_synced_torres: set[int] = set()

        with connection.cursor() as cursor:
            for ap in aps_data:
                torre = (ap.get("torre") or "").strip()
                if not torre:
                    continue

                cursor.execute(
                    """
                    INSERT INTO torres (nombre, latitud, longitud)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        latitud = COALESCE(NULLIF(VALUES(latitud), ''), latitud),
                        longitud = COALESCE(NULLIF(VALUES(longitud), ''), longitud)
                    """,
                    (torre, ap.get("torre_latitud"), ap.get("torre_longitud")),
                )

                id_torre = _resolve_id_torre(cursor, torre)
                if id_torre and id_torre not in contactos_synced_torres:
                    _sync_contactos_for_torre(cursor, id_torre, ap)
                    contactos_synced_torres.add(id_torre)

                ap_name_clean = (ap.get("ap_name") or "").strip()
                if not ap_name_clean:
                    continue

                ip_address = _lookup_ip(ip_map, torre, ap_name_clean)
                if ip_address:
                    ips_aplicadas += 1
                else:
                    ips_no_encontradas.append(f"{torre}/{ap_name_clean}")

                disp_id = None
                if id_torre:
                    disp_id = _resolve_disp_id(cursor, id_torre, ap_name_clean, ip_address)

                cursor.execute(
                    """
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
                    """,
                    (
                        disp_id,
                        torre,
                        ap_name_clean,
                        ap.get("tipo"),
                        ap.get("azimut"),
                        ap.get("tilt"),
                        ap.get("altura"),
                        ip_address,
                    ),
                )

        connection.commit()

        if ips_no_encontradas:
            logger.warning(
                "%s APs sin IP en ip_map.json: %s",
                len(ips_no_encontradas),
                ", ".join(ips_no_encontradas[:10])
                + (" ..." if len(ips_no_encontradas) > 10 else ""),
            )

        return (
            f"Sincronizados {len(aps_data)} registros. "
            f"IPs aplicadas: {ips_aplicadas}/{len(aps_data)}."
        )
    except Exception as e:
        connection.rollback()
        logger.exception("sync_intermapper_data falló: %s", e)
        return f"Error: {e}"
    finally:
        connection.close()


def invalidate_ip_map_cache() -> None:
    global _ip_map_cache, _ip_map_mtime
    with _ip_map_lock:
        _ip_map_cache = None
        _ip_map_mtime = 0.0
