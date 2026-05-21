from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

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

_DDL_DISPOSITIVOS_AP = """
CREATE TABLE IF NOT EXISTS dispositivos_ap (
    id INT NOT NULL AUTO_INCREMENT,
    disp_id INT NULL,
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
    KEY idx_dispositivos_torre (torre_nombre),
    KEY idx_dispositivos_ap_disp_id (disp_id),
    CONSTRAINT fk_dispositivos_ap_disp FOREIGN KEY (disp_id) REFERENCES dispositivos (id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

# --- NUEVA TABLA CONTACTOS ---
_DDL_CONTACTOS = """
CREATE TABLE IF NOT EXISTS contactos (
    id INT NOT NULL AUTO_INCREMENT,
    id_torre INT NOT NULL,
    torre VARCHAR(150) NOT NULL,
    contacto1 VARCHAR(150) NULL,
    contacto2 VARCHAR(150) NULL,
    contacto3 VARCHAR(150) NULL,
    contacto4 VARCHAR(150) NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_contactos_id_torre (id_torre),
    CONSTRAINT fk_contactos_torres FOREIGN KEY (id_torre) REFERENCES torres (id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

def ensure_intermapper_tables(connection) -> None:
    """Crea torres, dispositivos, contactos y dispositivos_ap si no existen. Idempotente."""
    with connection.cursor() as cursor:
        cursor.execute(_DDL_TORRES)
        cursor.execute(_DDL_DISPOSITIVOS)
        cursor.execute(_DDL_CONTACTOS)  # Crear la tabla de contactos
        cursor.execute(_DDL_DISPOSITIVOS_AP)
    connection.commit()
    logger.info("Tablas MySQL 'torres', 'dispositivos', 'contactos' y 'dispositivos_ap' comprobadas.")

def sync_all_devices(connection, ip_map: dict) -> None:
    """Recorre el diccionario completo tras el scraper, asegura torres, inicializa contactos y llena dispositivos."""
    inserted_or_updated = 0
    with connection.cursor() as cursor:
        for torre, devices in ip_map.items():
            # Asegurar la existencia de la torre (mantiene ID si ya existía)
            cursor.execute("INSERT IGNORE INTO torres (nombre) VALUES (%s)", (torre,))
            
            # Extraer el ID (id_torre) asignado de la tabla de torres
            cursor.execute("SELECT id FROM torres WHERE nombre = %s", (torre,))
            row_torre = cursor.fetchone()
            id_torre = row_torre.get("id") if row_torre else None
            
            if id_torre:
                # Inicializar la tabla contactos dejando los campos de contacto vacíos de forma segura
                # ON DUPLICATE KEY UPDATE asegura que si ya existía, no rompa ni altere los datos guardados por la IA
                sql_init_contacto = """
                    INSERT INTO contactos (id_torre, torre)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE torre = VALUES(torre)
                """
                cursor.execute(sql_init_contacto, (id_torre, torre))
            
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
    logger.info(f"Sincronizados {inserted_or_updated} dispositivos e inicializados contactos base.")