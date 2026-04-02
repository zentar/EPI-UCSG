from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re

import pandas as pd


DEDICATION_ALLOWED = {
    "TIEMPO PARCIAL",
    "EXCLUSIVA O TIEMPO COMPLETO",
    "SEMI EXCLUSIVA O MEDIO TIEMPO",
    "NO DEFINIDA",
}

DEDICATION_ALIASES = {
    "TIEMPO PARCIAL ": "TIEMPO PARCIAL",
    "EXCLUSIVA O TIEMPO COMPLETO ": "EXCLUSIVA O TIEMPO COMPLETO",
    "SEMI EXCLUSIVA O MEDIO TIEMPO ": "SEMI EXCLUSIVA O MEDIO TIEMPO",
}


@dataclass
class TeacherRow:
    teacher_id: str
    teacher_name: str
    category: str
    dedication: str
    faculty: str | None
    career: str | None


def normalize_header(header: str) -> str:
    text = str(header).strip().upper()
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


def read_dataframe(file_path: Path) -> pd.DataFrame:
    suffix = file_path.suffix.lower()
    if suffix == ".csv":
        try:
            return pd.read_csv(file_path, sep=None, engine="python", dtype=str)
        except Exception:
            try:
                return pd.read_csv(file_path, sep=";", dtype=str)
            except Exception:
                return pd.read_csv(file_path, sep=";", dtype=str, encoding="latin1")

    if suffix == ".xlsx":
        return pd.read_excel(file_path, dtype=str)

    raise ValueError("Formato de archivo no soportado")


def parse_teachers_file(file_path: Path) -> tuple[list[TeacherRow], list[str], int]:
    df = read_dataframe(file_path).fillna("")
    df.columns = [normalize_header(col) for col in df.columns]

    valid_rows: list[TeacherRow] = []
    errors: list[str] = []
    seen_teacher_ids: set[str] = set()
    defaulted_dedication_count = 0
    has_dedication_column = "DEDICACION DOCENTE" in set(df.columns)

    for index, row in df.iterrows():
        row_number = index + 2
        row_dict = {col: normalize_value(value) for col, value in row.items()}

        teacher_id = pick_value(row_dict, ["ID", " NUMERO IDENTIFICACION", "NUMERO IDENTIFICACION", "CODIGO", "_ID"])
        teacher_name = pick_value(row_dict, ["DOCENTE", "NOMBRE DOCENTE", "NOMBRE COMPLETO"])
        category = pick_value(row_dict, ["CATEGORIA", "CATEGORIA DOCENTE"])
        dedication = pick_value(row_dict, ["DEDICACION DOCENTE", "DEDICACION DOCENTE "])
        faculty = pick_value(row_dict, ["FACULTAD", "COD FACULTAD", "COD FACULTAD"])
        career = pick_value(row_dict, ["CARRERA"])

        if not teacher_id or not teacher_name or not category or not faculty or not career:
            errors.append(
                f"Fila {row_number}: faltan campos obligatorios (ID, DOCENTE, CATEGORIA, FACULTAD, CARRERA)."
            )
            continue

        if not dedication and not has_dedication_column:
            dedication = "NO DEFINIDA"
            defaulted_dedication_count += 1

        if not dedication:
            errors.append(
                f"Fila {row_number}: falta DEDICACION DOCENTE en la fila."
            )
            continue

        if teacher_id in seen_teacher_ids:
            errors.append(f"Fila {row_number}: ID duplicado en archivo ({teacher_id}).")
            continue

        dedication_normalized = DEDICATION_ALIASES.get(dedication, dedication).strip().upper()
        if dedication_normalized not in DEDICATION_ALLOWED:
            errors.append(
                f"Fila {row_number}: dedicación no válida '{dedication}'."
            )
            continue

        seen_teacher_ids.add(teacher_id)

        valid_rows.append(
            TeacherRow(
                teacher_id=teacher_id,
                teacher_name=teacher_name,
                category=category,
                dedication=dedication_normalized,
                faculty=faculty,
                career=career,
            )
        )

    return valid_rows, errors, defaulted_dedication_count
