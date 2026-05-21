"""Utilidades para parsear contactos enviados por n8n (contacto1..4)."""
from __future__ import annotations

import re

_PHONE_RE = re.compile(
    r"(?:\+?1[-.\s]?)?(?:\(?\d{3}\)?[-.\s]?)?\d{3}[-.\s]?\d{4}|\b\d{7,11}\b"
)


def parse_contacto(raw: str) -> tuple[str, str]:
    """Separa número y nombre a partir de un string libre del plano."""
    text = (raw or "").strip()
    if not text:
        return "", ""

    match = _PHONE_RE.search(text)
    if match:
        numero = match.group(0).strip()
        nombre = text.replace(numero, "", 1).strip(" -–—:,;/|")
        return numero, nombre

    if re.search(r"\d{3,}", text):
        return text, ""

    return "", text


def contactos_from_ap_fields(
    contacto1: str | None,
    contacto2: str | None,
    contacto3: str | None,
    contacto4: str | None,
) -> list[tuple[str, str]]:
    """Convierte contacto1..4 del agente n8n en filas (numero, nombre)."""
    rows: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for raw in (contacto1, contacto2, contacto3, contacto4):
        numero, nombre = parse_contacto(raw or "")
        if not numero and not nombre:
            continue
        key = (numero, nombre)
        if key in seen:
            continue
        seen.add(key)
        rows.append(key)
    return rows
