import re


def normalize_reference(value) -> str:
    """Normaliza referencias leídas como texto, entero o número decimal."""
    text = str(value or "").strip()
    if re.fullmatch(r"[0-9]+\.0+", text):
        text = text.split(".", 1)[0]
    return text


def extract_risky_references(sheet_values: list[list[str]]) -> set[str]:
    """Extrae referencias cuyo Tipo de cliente sea exactamente Atrasado."""
    if not sheet_values:
        return set()

    def normalize_header(value) -> str:
        text = str(value or "").strip().lower()
        text = text.translate(str.maketrans("áéíóúü", "aeiouu"))
        return re.sub(r"\s+", " ", text).strip()

    headers = [normalize_header(header) for header in sheet_values[0]]
    try:
        reference_idx = headers.index("referencia")
        client_type_idx = headers.index("tipo de cliente")
    except ValueError as exc:
        raise RuntimeError(
            "La base de clientes riesgosos debe tener las columnas Referencia y Tipo de cliente."
        ) from exc

    risky_references = set()
    for row in sheet_values[1:]:
        reference = row[reference_idx] if reference_idx < len(row) else ""
        client_type = row[client_type_idx] if client_type_idx < len(row) else ""
        normalized_reference = normalize_reference(reference)
        if normalized_reference and normalize_header(client_type) == "atrasado":
            risky_references.add(normalized_reference)
    return risky_references
