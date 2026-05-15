"""Creación idempotente de tablas MySQL usadas por el MCP (sync Intermapper)."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

# `torres`: el MCP hace INSERT IGNORE por `nombre`; hace falta clave única.
_DDL_TORRES = """
CREATE TABLE IF NOT EXISTS torres (
    id INT NOT NULL AUTO_INCREMENT,
    nombre VARCHAR(150) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_torres_nombre (nombre)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

# Alineado con el DESCRIBE esperado; UNIQUE (torre_nombre, ap_name) para el UPSERT del MCP.
_DDL_DISPOSITIVOS_AP = """
CREATE TABLE IF NOT EXISTS dispositivos_ap (
    id INT NOT NULL AUTO_INCREMENT,
    torre_nombre VARCHAR(150) NOT NULL,
    ap_name VARCHAR(150) NOT NULL,
    tipo VARCHAR(50) NULL,
    azimut VARCHAR(50) NULL,
    tilt VARCHAR(50) NULL,
    altura VARCHAR(50) NULL,
    ip_address VARCHAR(45) NULL,
    fecha_extraccion TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_dispositivos_torre_ap (torre_nombre, ap_name),
    KEY idx_dispositivos_torre (torre_nombre)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""


def ensure_intermapper_tables(connection) -> None:
    """Crea `torres` y `dispositivos_ap` si no existen. Idempotente."""
    with connection.cursor() as cursor:
        cursor.execute(_DDL_TORRES)
        cursor.execute(_DDL_DISPOSITIVOS_AP)
    connection.commit()
    logger.info("Tablas MySQL 'torres' y 'dispositivos_ap' comprobadas (CREATE IF NOT EXISTS).")
