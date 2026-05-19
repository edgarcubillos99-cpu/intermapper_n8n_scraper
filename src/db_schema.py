from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

# `torres`: el MCP hace INSERT IGNORE por `nombre`; hace falta clave única.
_DDL_TORRES = """
CREATE TABLE IF NOT EXISTS torres (
    id INT NOT NULL AUTO_INCREMENT,
    nombre VARCHAR(150) NOT NULL,
    latitud VARCHAR(50) NULL,
    longitud VARCHAR(50) NULL,
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

# --- NUEVA TABLA ---
_DDL_DISPOSITIVOS = """
CREATE TABLE IF NOT EXISTS dispositivos (
    id INT NOT NULL AUTO_INCREMENT,
    torre VARCHAR(150) NOT NULL,
    dispositivo VARCHAR(150) NOT NULL,
    ip_address VARCHAR(45) NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_dispositivos_torre_disp (torre, dispositivo),
    KEY idx_dispositivos_torre (torre)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

def ensure_intermapper_tables(connection) -> None:
    """Crea `torres`, `dispositivos_ap` y `dispositivos` si no existen. Idempotente."""
    with connection.cursor() as cursor:
        cursor.execute(_DDL_TORRES)
        cursor.execute(_DDL_DISPOSITIVOS_AP)
        cursor.execute(_DDL_DISPOSITIVOS) # Ejecuta el script de la nueva tabla
    connection.commit()
    logger.info("Tablas MySQL 'torres', 'dispositivos_ap' y 'dispositivos' comprobadas (CREATE IF NOT EXISTS).")

def sync_all_devices(connection, ip_map: dict) -> None:
    """Recorre el diccionario completo de dispositivos y hace upsert en la BD."""
    inserted_or_updated = 0
    with connection.cursor() as cursor:
        for torre, devices in ip_map.items():
            # Garantizar que la torre exista en la tabla padre 'torres'
            cursor.execute("INSERT IGNORE INTO torres (nombre) VALUES (%s)", (torre,))
            
            for dispositivo, ip in devices.items():
                sql_upsert = """
                    INSERT INTO dispositivos (torre, dispositivo, ip_address)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE 
                        ip_address = VALUES(ip_address)
                """
                cursor.execute(sql_upsert, (torre, dispositivo, ip))
                inserted_or_updated += 1
    connection.commit()
    logger.info(f"Sincronizados {inserted_or_updated} dispositivos totales en la tabla 'dispositivos'.")