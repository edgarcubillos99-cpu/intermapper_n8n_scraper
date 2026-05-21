"""Migraciones idempotentes del esquema MySQL en caliente."""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def _column_exists(cursor, table: str, column: str) -> bool:
    cursor.execute(
        """
        SELECT COUNT(*) AS c
          FROM information_schema.columns
         WHERE table_schema = DATABASE()
           AND table_name   = %s
           AND column_name  = %s
        """,
        (table, column),
    )
    row = cursor.fetchone()
    return bool((row or {}).get("c", 0))


def _index_exists(cursor, table: str, index_name: str) -> bool:
    cursor.execute(
        """
        SELECT COUNT(*) AS c
          FROM information_schema.statistics
         WHERE table_schema = DATABASE()
           AND table_name   = %s
           AND index_name   = %s
        """,
        (table, index_name),
    )
    row = cursor.fetchone()
    return bool((row or {}).get("c", 0))


def ensure_dispositivos_id_torre(connection) -> None:
    """dispositivos.torre (varchar) -> id_torre (FK torres)."""
    with connection.cursor() as cursor:
        if not _column_exists(cursor, "dispositivos", "id_torre"):
            if _column_exists(cursor, "dispositivos", "torre"):
                logger.info("Migrando dispositivos: añadiendo id_torre desde torre.")
                cursor.execute(
                    "ALTER TABLE dispositivos "
                    "ADD COLUMN id_torre INT NULL AFTER id"
                )
                cursor.execute(
                    """
                    UPDATE dispositivos d
                    INNER JOIN torres t ON t.nombre = d.torre
                       SET d.id_torre = t.id
                    """
                )
                if _index_exists(cursor, "dispositivos", "uq_dispositivos_torre_disp"):
                    cursor.execute(
                        "ALTER TABLE dispositivos DROP INDEX uq_dispositivos_torre_disp"
                    )
                if _index_exists(cursor, "dispositivos", "idx_dispositivos_torre"):
                    cursor.execute(
                        "ALTER TABLE dispositivos DROP INDEX idx_dispositivos_torre"
                    )
                cursor.execute("ALTER TABLE dispositivos DROP COLUMN torre")
                cursor.execute(
                    "ALTER TABLE dispositivos MODIFY id_torre INT NOT NULL"
                )
                cursor.execute(
                    "ALTER TABLE dispositivos "
                    "ADD UNIQUE KEY uq_dispositivos_torre_disp (id_torre, dispositivo), "
                    "ADD KEY idx_dispositivos_id_torre (id_torre), "
                    "ADD CONSTRAINT fk_dispositivos_torres "
                    "FOREIGN KEY (id_torre) REFERENCES torres (id) ON DELETE CASCADE"
                )
            else:
                cursor.execute(
                    """
                    ALTER TABLE dispositivos
                    ADD COLUMN id_torre INT NOT NULL AFTER id,
                    ADD KEY idx_dispositivos_id_torre (id_torre),
                    ADD CONSTRAINT fk_dispositivos_torres
                        FOREIGN KEY (id_torre) REFERENCES torres (id) ON DELETE CASCADE
                    """
                )
    connection.commit()


def ensure_dispositivos_ap_torre_nombre(connection) -> None:
    """Mantiene dispositivos_ap con torre_nombre (revierte id_torre si existía)."""
    with connection.cursor() as cursor:
        if _column_exists(cursor, "dispositivos_ap", "torre_nombre"):
            return

        if not _column_exists(cursor, "dispositivos_ap", "id_torre"):
            return

        logger.info("Revirtiendo dispositivos_ap: id_torre -> torre_nombre.")
        cursor.execute(
            "ALTER TABLE dispositivos_ap "
            "ADD COLUMN torre_nombre VARCHAR(150) NULL AFTER disp_id"
        )
        cursor.execute(
            """
            UPDATE dispositivos_ap da
            INNER JOIN torres t ON t.id = da.id_torre
               SET da.torre_nombre = t.nombre
            """
        )
        cursor.execute("DELETE FROM dispositivos_ap WHERE torre_nombre IS NULL")
        if _index_exists(cursor, "dispositivos_ap", "uq_dispositivos_ap_torre_ap"):
            cursor.execute(
                "ALTER TABLE dispositivos_ap DROP INDEX uq_dispositivos_ap_torre_ap"
            )
        if _index_exists(cursor, "dispositivos_ap", "idx_dispositivos_ap_id_torre"):
            cursor.execute(
                "ALTER TABLE dispositivos_ap DROP INDEX idx_dispositivos_ap_id_torre"
            )
        cursor.execute(
            """
            SELECT CONSTRAINT_NAME
              FROM information_schema.TABLE_CONSTRAINTS
             WHERE table_schema = DATABASE()
               AND table_name = 'dispositivos_ap'
               AND constraint_name = 'fk_dispositivos_ap_torres'
            """
        )
        if cursor.fetchone():
            cursor.execute(
                "ALTER TABLE dispositivos_ap DROP FOREIGN KEY fk_dispositivos_ap_torres"
            )
        cursor.execute("ALTER TABLE dispositivos_ap DROP COLUMN id_torre")
        cursor.execute(
            "ALTER TABLE dispositivos_ap MODIFY torre_nombre VARCHAR(150) NOT NULL"
        )
        cursor.execute(
            "ALTER TABLE dispositivos_ap "
            "ADD UNIQUE KEY uq_dispositivos_torre_ap (torre_nombre, ap_name), "
            "ADD KEY idx_dispositivos_torre (torre_nombre)"
        )
    connection.commit()


def ensure_contactos_normalized(connection) -> None:
    """contactos: una fila por contacto (numero, nombre), sin contacto1..4 ni torre."""
    with connection.cursor() as cursor:
        has_numero = _column_exists(cursor, "contactos", "numero")
        has_contacto1 = _column_exists(cursor, "contactos", "contacto1")

        if has_numero and not has_contacto1:
            return

        if not _column_exists(cursor, "contactos", "id"):
            return

        logger.info("Migrando contactos al esquema numero/nombre por fila.")

        if _index_exists(cursor, "contactos", "uq_contactos_id_torre"):
            cursor.execute("ALTER TABLE contactos DROP INDEX uq_contactos_id_torre")

        if not has_numero:
            cursor.execute(
                "ALTER TABLE contactos "
                "ADD COLUMN numero VARCHAR(150) NULL AFTER id_torre, "
                "ADD COLUMN nombre VARCHAR(150) NULL AFTER numero"
            )

        if has_contacto1:
            cursor.execute("SELECT id, id_torre, contacto1, contacto2, contacto3, contacto4 FROM contactos")
            from src.contact_utils import parse_contacto

            for row in cursor.fetchall() or []:
                id_torre = row["id_torre"]
                for field in ("contacto1", "contacto2", "contacto3", "contacto4"):
                    numero, nombre = parse_contacto(row.get(field) or "")
                    if not numero and not nombre:
                        continue
                    cursor.execute(
                        """
                        SELECT id FROM contactos
                         WHERE id_torre = %s AND numero <=> %s AND nombre <=> %s
                         LIMIT 1
                        """,
                        (id_torre, numero or None, nombre or None),
                    )
                    if cursor.fetchone():
                        continue
                    cursor.execute(
                        "INSERT INTO contactos (id_torre, numero, nombre) VALUES (%s, %s, %s)",
                        (id_torre, numero or None, nombre or None),
                    )
                cursor.execute("DELETE FROM contactos WHERE id = %s", (row["id"],))

            for col in ("contacto4", "contacto3", "contacto2", "contacto1", "torre"):
                if _column_exists(cursor, "contactos", col):
                    cursor.execute(f"ALTER TABLE contactos DROP COLUMN {col}")

        cursor.execute(
            """
            SELECT COUNT(*) AS c
              FROM information_schema.statistics
             WHERE table_schema = DATABASE()
               AND table_name = 'contactos'
               AND index_name = 'idx_contactos_id_torre'
            """
        )
        if not (cursor.fetchone() or {}).get("c"):
            cursor.execute(
                "ALTER TABLE contactos ADD KEY idx_contactos_id_torre (id_torre)"
            )

    connection.commit()


def run_schema_migrations(connection) -> None:
    ensure_dispositivos_id_torre(connection)
    ensure_dispositivos_ap_torre_nombre(connection)
    ensure_contactos_normalized(connection)
