from __future__ import annotations

import logging

from src.db_migrations import run_schema_migrations

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
    id_torre INT NOT NULL,
    dispositivo VARCHAR(150) NOT NULL,
    ip_address VARCHAR(45) NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_dispositivos_torre_disp (id_torre, dispositivo),
    KEY idx_dispositivos_id_torre (id_torre),
    CONSTRAINT fk_dispositivos_torres FOREIGN KEY (id_torre) REFERENCES torres (id) ON DELETE CASCADE
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

_DDL_CONTACTOS = """
CREATE TABLE IF NOT EXISTS contactos (
    id INT NOT NULL AUTO_INCREMENT,
    id_torre INT NOT NULL,
    numero VARCHAR(150) NULL,
    nombre VARCHAR(150) NULL,
    PRIMARY KEY (id),
    KEY idx_contactos_id_torre (id_torre),
    CONSTRAINT fk_contactos_torres FOREIGN KEY (id_torre) REFERENCES torres (id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""


def ensure_intermapper_tables(connection) -> None:
    """Crea tablas base y aplica migraciones idempotentes del esquema relacional."""
    with connection.cursor() as cursor:
        cursor.execute(_DDL_TORRES)
        cursor.execute(_DDL_DISPOSITIVOS)
        cursor.execute(_DDL_CONTACTOS)
        cursor.execute(_DDL_DISPOSITIVOS_AP)
    connection.commit()
    run_schema_migrations(connection)
    logger.info(
        "Tablas MySQL 'torres', 'dispositivos', 'contactos' y 'dispositivos_ap' comprobadas."
    )


def sync_all_devices(connection, ip_map: dict) -> None:
    """Tras el scraper: asegura torres y sincroniza dispositivos por id_torre."""
    inserted_or_updated = 0
    torres_sin_id = 0

    with connection.cursor() as cursor:
        for torre_nombre, devices in ip_map.items():
            cursor.execute("INSERT IGNORE INTO torres (nombre) VALUES (%s)", (torre_nombre,))
            cursor.execute("SELECT id FROM torres WHERE nombre = %s", (torre_nombre,))
            row_torre = cursor.fetchone()
            id_torre = row_torre.get("id") if row_torre else None

            if not id_torre:
                torres_sin_id += 1
                logger.warning("Torre sin id en BD tras INSERT IGNORE: %s", torre_nombre)
                continue

            for dispositivo, ip in (devices or {}).items():
                cursor.execute(
                    """
                    INSERT INTO dispositivos (id_torre, dispositivo, ip_address)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE ip_address = VALUES(ip_address)
                    """,
                    (id_torre, dispositivo, ip),
                )
                inserted_or_updated += 1

    connection.commit()
    logger.info(
        "Sincronizados %s dispositivos (%s torres en ip_map, %s sin id_torre).",
        inserted_or_updated,
        len(ip_map),
        torres_sin_id,
    )
