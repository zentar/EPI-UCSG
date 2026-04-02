from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
import unicodedata

import pandas as pd


def normalize_header(header: str) -> str:
    text = str(header).strip().upper()
    # Make header matching resilient to accents and similar Unicode variants.
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.replace("_", " ").replace(".", " ")
    text = re.sub(r"\s+", " ", text)
    return text


def normalize_value(value) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() == "nan":
        return ""
    return text


def pick_value(row: dict[str, str], aliases: list[str]) -> str:
    for alias in aliases:
        value = normalize_value(row.get(alias, ""))
        if value:
            return value
    return ""


def parse_publication_year(raw_value: str) -> int | None:
    value = normalize_value(raw_value)
    if not value:
        return None

    try:
        return int(value)
    except ValueError:
        pass

    # Accept embedded 4-digit year, e.g. "Publicado 2019".
    for token in re.findall(r"\d{4}", value):
        year = int(token)
        if 1900 <= year <= 2100:
            return year

    # Accept date-like inputs, e.g. "03-JAN-10" -> 2010.
    parsed = pd.to_datetime(value, errors="coerce", dayfirst=True)
    if pd.notna(parsed):
        year = int(parsed.year)
        if 1900 <= year <= 2100:
            return year

    return None


@dataclass
class PublicationRow:
    sequence: str
    publication_type: str
    author_teacher_id: str
    publication_year: int
    title: str
    source_base: str | None
    quartile: str | None
    journal_name: str | None
    raw_fields: dict[str, str]


def read_dataframe(file_path: Path) -> pd.DataFrame:
    suffix = file_path.suffix.lower()
    if suffix == ".csv":
        encodings = ["utf-8-sig", "utf-8", "latin-1", "cp1252"]
        attempts: list[Exception] = []

        for encoding in encodings:
            try:
                return pd.read_csv(file_path, sep=None, engine="python", dtype=str, encoding=encoding)
            except Exception as exc:
                attempts.append(exc)

            try:
                return pd.read_csv(file_path, sep=";", dtype=str, encoding=encoding)
            except Exception as exc:
                attempts.append(exc)

        raise ValueError(f"No se pudo leer el CSV. Revisa la codificacion o delimitador. Error: {attempts[-1]}")

    if suffix == ".xlsx":
        return pd.read_excel(file_path, dtype=str)

    raise ValueError("Formato de archivo no soportado")


def parse_publications_file(file_path: Path) -> tuple[list[PublicationRow], list[str]]:
    df = read_dataframe(file_path).fillna("")
    df.columns = [normalize_header(col) for col in df.columns]

    valid_rows: list[PublicationRow] = []
    errors: list[str] = []

    for index, row in df.iterrows():
        row_number = index + 2
        row_dict = {col: normalize_value(value) for col, value in row.items()}

        sequence = pick_value(row_dict, ["SECUENCIA"])
        publication_type = pick_value(row_dict, ["COD TIPO", "COD_TIPO"]).upper()
        author_teacher_id = pick_value(
            row_dict,
            [
                "NUMERO IDENTIFICACION",
                "NUMERO_IDENTIFICACION",
                "COD EMPLEADO",
                "CODIGO EMPLEADO",
                "COD_EMPLEADO",
                "ID DOCENTE",
                "ID",
            ],
        )
        publication_year_raw = pick_value(
            row_dict,
            ["ANIO PUBLICACION", "ANIO_PUBLICACION", "ANO PUBLICACION", "ANO_PUBLICACION", "ANO"],
        )
        title = pick_value(row_dict, ["DESCRIPCION", "TITULO", "TITULO PUBLICACION"])
        source_base = pick_value(row_dict, ["BASE"])
        quartile = pick_value(row_dict, ["INDICE Q", "INDICE_Q"])
        journal_name = pick_value(row_dict, ["NOMBRE REVISTA", "NOMBRE_REVISTA"])

        if not sequence or not publication_type or not author_teacher_id or not publication_year_raw or not title:
            errors.append(
                f"Fila {row_number}: faltan campos obligatorios (SECUENCIA, COD_TIPO, COD_EMPLEADO, ANIO_PUBLICACION, DESCRIPCION)."
            )
            continue

        if publication_type not in {"A", "L", "C", "E"}:
            errors.append(f"Fila {row_number}: COD_TIPO inválido '{publication_type}'.")
            continue

        publication_year = parse_publication_year(publication_year_raw)
        if publication_year is None:
            errors.append(f"Fila {row_number}: ANIO_PUBLICACION inválido '{publication_year_raw}'.")
            continue

        valid_rows.append(
            PublicationRow(
                sequence=sequence,
                publication_type=publication_type,
                author_teacher_id=author_teacher_id,
                publication_year=publication_year,
                title=title,
                source_base=source_base or None,
                quartile=quartile or None,
                journal_name=journal_name or None,
                raw_fields=row_dict,
            )
        )

    return valid_rows, errors
