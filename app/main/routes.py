import csv
import io
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from urllib.parse import urlencode

import pandas as pd
from flask import Blueprint, Response, current_app, flash, jsonify, redirect, render_template, request, send_file, url_for
from flask_login import current_user, login_required
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle
from werkzeug.utils import secure_filename

from app.extensions import db, limiter, csrf
from app.main.forms import PublicationUploadForm, TeacherUploadForm
from app.models import (
    BaseExcluded,
    BaseLabel,
    ImportBatch,
    PacArtisticSetting,
    Publication,
    PublicationAuthor,
    PublicationTypeExcluded,
    PublicationTypeLabel,
    Teacher,
)
from app.services.publication_ingestion import parse_publications_file
from app.services.teacher_ingestion import parse_teachers_file


main_bp = Blueprint("main", __name__)


PUBLICATION_DEFAULT_COLUMNS = [
    "SECUENCIA",
    "COD TIPO",
    "ANIO PUBLICACION",
    "DESCRIPCION",
    "BASE",
    "INDICE Q",
    "NOMBRE REVISTA",
    "NUMERO IDENTIFICACION",
]


def _publication_column_param(column_name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", column_name.lower()).strip("_")


def _filter_and_paginate_rows(rows: list[dict], request_args, columns: list[str], prefix: str) -> dict:
    column_param_map = {column: f"{prefix}_{_publication_column_param(column)}" for column in columns}
    filter_values = {
        column: request_args.get(param_name, type=str, default="").strip()
        for column, param_name in column_param_map.items()
    }

    filtered_rows = []
    for row in rows:
        include_row = True
        for column, filter_value in filter_values.items():
            if filter_value and filter_value.lower() not in str(row.get(column, "")).lower():
                include_row = False
                break
        if include_row:
            filtered_rows.append(row)

    page_size_options = [25, 50, 100, 200]
    page_size = request_args.get(f"{prefix}_page_size", type=int, default=50)
    if page_size not in page_size_options:
        page_size = 50

    total_filtered = len(filtered_rows)
    total_pages = max(1, (total_filtered + page_size - 1) // page_size) if total_filtered else 1

    page = request_args.get(f"{prefix}_page", type=int, default=1)
    if page < 1:
        page = 1
    if page > total_pages:
        page = total_pages

    start_idx = (page - 1) * page_size
    end_idx = start_idx + page_size
    page_rows = filtered_rows[start_idx:end_idx]

    base_params = {}
    for key in request_args.keys():
        if key == f"{prefix}_page":
            continue
        values = request_args.getlist(key)
        if values:
            base_params[key] = values

    base_params[f"{prefix}_page_size"] = [str(page_size)]
    for column, filter_value in filter_values.items():
        param_name = column_param_map[column]
        if filter_value:
            base_params[param_name] = [filter_value]
        elif param_name in base_params:
            del base_params[param_name]

    def _query_for(page_number: int) -> str:
        params = {key: list(values) for key, values in base_params.items()}
        params[f"{prefix}_page"] = [str(page_number)]
        return urlencode(params, doseq=True)

    if total_pages <= 7:
        page_numbers = list(range(1, total_pages + 1))
    else:
        page_numbers = {1, total_pages, page - 1, page, page + 1}
        page_numbers = [num for num in sorted(page_numbers) if 1 <= num <= total_pages]

    page_links = [
        {
            "number": number,
            "query": _query_for(number),
            "current": number == page,
        }
        for number in page_numbers
    ]

    return {
        "rows": page_rows,
        "filtered_rows": filtered_rows,
        "page": page,
        "page_size": page_size,
        "page_size_options": page_size_options,
        "total_filtered": total_filtered,
        "total_pages": total_pages,
        "column_param_map": column_param_map,
        "filter_values": filter_values,
        "prev_query": _query_for(page - 1) if page > 1 else "",
        "next_query": _query_for(page + 1) if page < total_pages else "",
        "page_links": page_links,
    }


def _paginate_items(items: list, request_args, prefix: str) -> dict:
    page_size_options = [25, 50, 100, 200]
    page_size = request_args.get(f"{prefix}_page_size", type=int, default=50)
    if page_size not in page_size_options:
        page_size = 50

    total_filtered = len(items)
    total_pages = max(1, (total_filtered + page_size - 1) // page_size) if total_filtered else 1

    page = request_args.get(f"{prefix}_page", type=int, default=1)
    if page < 1:
        page = 1
    if page > total_pages:
        page = total_pages

    start_idx = (page - 1) * page_size
    end_idx = start_idx + page_size
    page_items = items[start_idx:end_idx]

    base_params = {}
    for key in request_args.keys():
        if key == f"{prefix}_page":
            continue
        values = request_args.getlist(key)
        if values:
            base_params[key] = values

    base_params[f"{prefix}_page_size"] = [str(page_size)]

    def _query_for(page_number: int) -> str:
        params = {key: list(values) for key, values in base_params.items()}
        params[f"{prefix}_page"] = [str(page_number)]
        return urlencode(params, doseq=True)

    if total_pages <= 7:
        page_numbers = list(range(1, total_pages + 1))
    else:
        page_numbers = {1, total_pages, page - 1, page, page + 1}
        page_numbers = [num for num in sorted(page_numbers) if 1 <= num <= total_pages]

    page_links = [
        {
            "number": number,
            "query": _query_for(number),
            "current": number == page,
        }
        for number in page_numbers
    ]

    return {
        "rows": page_items,
        "page": page,
        "page_size": page_size,
        "page_size_options": page_size_options,
        "total_filtered": total_filtered,
        "total_pages": total_pages,
        "prev_query": _query_for(page - 1) if page > 1 else "",
        "next_query": _query_for(page + 1) if page < total_pages else "",
        "page_links": page_links,
    }


def _publication_row_payload(publication: Publication, publication_author: PublicationAuthor) -> dict[str, str]:
    payload = dict(publication_author.source_row_json or {})

    fallback_values = {
        "SECUENCIA": publication.publication_sequence or "",
        "COD TIPO": publication.publication_type or "",
        "ANIO PUBLICACION": str(publication.publication_year or ""),
        "DESCRIPCION": publication.title or "",
        "BASE": publication.source_base or "",
        "INDICE Q": publication.quartile or "",
        "NOMBRE REVISTA": publication.journal_name or "",
        "NUMERO IDENTIFICACION": publication_author.teacher_id or "",
        "COD EMPLEADO": publication_author.teacher_id or "",
    }

    for key, value in fallback_values.items():
        payload.setdefault(key, value)

    return {str(key): "" if value is None else str(value) for key, value in payload.items()}


def _collect_publication_view_state(selected_year: int | None, request_args) -> dict:
    available_columns: list[str] = []
    selected_columns: list[str] = []
    visible_filter_values: dict[str, str] = {}
    filtered_rows: list[dict] = []
    export_base_params: dict[str, list[str]] = {}
    column_param_map: dict[str, str] = {}
    search_query = request_args.get("search", type=str, default="").strip()

    if selected_year is None:
        return {
            "available_columns": available_columns,
            "selected_columns": selected_columns,
            "filtered_rows": filtered_rows,
            "search_query": search_query,
            "export_csv_query": "",
            "export_xlsx_query": "",
            "column_param_map": column_param_map,
        }

    row_records = (
        db.session.query(Publication, PublicationAuthor)
        .join(PublicationAuthor, PublicationAuthor.publication_id == Publication.id)
        .filter(Publication.publication_year == selected_year)
        .order_by(Publication.created_at.desc(), PublicationAuthor.id.desc())
        .all()
    )

    prepared_rows = []
    column_seen = set()
    for publication, publication_author in row_records:
        values = _publication_row_payload(publication, publication_author)
        for key in values.keys():
            if key not in column_seen:
                column_seen.add(key)
                available_columns.append(key)
        prepared_rows.append(
            {
                "publication": publication,
                "publication_author": publication_author,
                "values": values,
            }
        )

    available_column_set = set(available_columns)
    ordered_defaults = [column for column in PUBLICATION_DEFAULT_COLUMNS if column in available_column_set]
    remaining_columns = [column for column in available_columns if column not in ordered_defaults]
    available_columns = ordered_defaults + remaining_columns

    requested_columns = [column for column in request_args.getlist("columns") if column in available_column_set]
    if requested_columns:
        selected_columns = requested_columns
    else:
        selected_columns = [column for column in PUBLICATION_DEFAULT_COLUMNS if column in available_column_set]
        if not selected_columns:
            selected_columns = available_columns[: min(8, len(available_columns))]

    for column in available_columns:
        column_param_map[column] = _publication_column_param(column)

    for column in selected_columns:
        visible_filter_values[column] = request_args.get(column_param_map[column], type=str, default="").strip()

    for row in prepared_rows:
        values = row["values"]

        if search_query:
            haystack = " ".join(values.get(column, "") for column in selected_columns).lower()
            if search_query.lower() not in haystack:
                continue

        matches_all_filters = True
        for column, filter_value in visible_filter_values.items():
            if filter_value and filter_value.lower() not in values.get(column, "").lower():
                matches_all_filters = False
                break

        if not matches_all_filters:
            continue

        filtered_rows.append(row)

    export_base_params = {"year": [str(selected_year)]}
    if search_query:
        export_base_params["search"] = [search_query]
    for column in selected_columns:
        export_base_params.setdefault("columns", []).append(column)
    for column, filter_value in visible_filter_values.items():
        if filter_value:
            export_base_params[column_param_map[column]] = [filter_value]

    csv_params = dict(export_base_params)
    csv_params["format"] = ["csv"]

    xlsx_params = dict(export_base_params)
    xlsx_params["format"] = ["xlsx"]

    return {
        "available_columns": available_columns,
        "selected_columns": selected_columns,
        "visible_filter_values": visible_filter_values,
        "filtered_rows": filtered_rows,
        "search_query": search_query,
        "export_csv_query": urlencode(csv_params, doseq=True),
        "export_xlsx_query": urlencode(xlsx_params, doseq=True),
        "column_param_map": column_param_map,
    }


def _extract_author_identity(publication_author: PublicationAuthor) -> tuple[str, str]:
    row_json = publication_author.source_row_json or {}
    num_id = (
        row_json.get("NUMERO IDENTIFICACION")
        or row_json.get("NUMERO_IDENTIFICACION")
        or publication_author.teacher_id
        or ""
    )
    num_id = str(num_id).strip()

    name_parts = [
        str(row_json.get("PRIMER NOMBRE") or "").strip(),
        str(row_json.get("SEGUNDO NOMBRE") or "").strip(),
        str(row_json.get("APELLIDO PAT") or "").strip(),
        str(row_json.get("APELLIDO MAT") or "").strip(),
    ]
    full_name = " ".join(part for part in name_parts if part)

    return num_id, (full_name or num_id)


def _dedication_weight(dedication: str) -> float:
    normalized = str(dedication or "").strip().upper()
    if normalized == "EXCLUSIVA O TIEMPO COMPLETO":
        return 1.0
    if normalized == "SEMI EXCLUSIVA O MEDIO TIEMPO":
        return 0.5
    return 0.0


def _article_category_and_weight(quartile: str, base: str) -> tuple[str, float]:
    normalized_base = str(base or "").strip().upper()
    normalized_quartile = str(quartile or "").strip().upper()

    if normalized_base in {"1", "2"}:
        if normalized_quartile == "Q1":
            return "Q1", 1.0
        if normalized_quartile == "Q2":
            return "Q2", 0.9
        if normalized_quartile == "Q3":
            return "Q3", 0.8
        if normalized_quartile == "Q4":
            return "Q4", 0.7
        return "ACI", 0.6

    if normalized_base == "3":
        return "LATINDEX", 0.2

    return "REGIONAL", 0.5


def _parse_positive_float(value) -> float | None:
    if value is None:
        return None
    text = str(value).strip().replace(",", ".")
    if not text:
        return None
    try:
        parsed = float(text)
    except (TypeError, ValueError):
        return None
    if parsed <= 0:
        return None
    return parsed


def _parse_non_negative_float(value) -> float | None:
    if value is None:
        return None
    text = str(value).strip().replace(",", ".")
    if not text:
        return None
    try:
        parsed = float(text)
    except (TypeError, ValueError):
        return None
    if parsed < 0:
        return None
    return parsed


def _chapter_weight(publication_authors: list[PublicationAuthor]) -> float:
    for author in publication_authors:
        row_json = author.source_row_json or {}
        total_chapters = (
            row_json.get("TOTAL CAPITULOS")
            or row_json.get("TOTAL_CAPITULOS")
            or row_json.get("TOTAL CAPÍTULOS")
        )
        parsed_total = _parse_positive_float(total_chapters)
        if parsed_total:
            return round(1.0 / parsed_total, 6)
    return 0.0


def _resolve_ip_source_years(evaluation_year: int, publication_year_filter: str) -> tuple[list[int], str]:
    default_source_years = [evaluation_year - 1, evaluation_year - 2, evaluation_year - 3]
    all_publication_years = [
        row[0]
        for row in db.session.query(Publication.publication_year)
        .filter(Publication.publication_year.isnot(None))
        .distinct()
        .order_by(Publication.publication_year.desc())
        .all()
    ]
    publication_years_available = sorted([int(year) for year in all_publication_years if year], reverse=True)

    source_years = list(default_source_years)
    normalized_filter = publication_year_filter
    if publication_year_filter != "ALL":
        try:
            publication_year_int = int(publication_year_filter)
            if publication_year_int in publication_years_available:
                source_years = [publication_year_int]
            else:
                normalized_filter = "ALL"
        except (ValueError, TypeError):
            normalized_filter = "ALL"

    return source_years, normalized_filter


def _annual_pa_pia_totals(source_years: list[int]) -> tuple[float, float]:
    if not source_years:
        return 0.0, 0.0

    rows = (
        PacArtisticSetting.query.filter(
            PacArtisticSetting.evaluation_year.in_(source_years),
            PacArtisticSetting.faculty_scope == "ALL",
            PacArtisticSetting.career_scope == "ALL",
        )
        .order_by(PacArtisticSetting.evaluation_year.desc())
        .all()
    )

    pa_total = sum(float(row.artistic_value or 0.0) for row in rows)
    pia_total = sum(float(row.intellectual_value or 0.0) for row in rows)
    return round(pa_total, 4), round(pia_total, 4)


def _compute_ip_summary(
    evaluation_year: int,
    selected_faculty: str,
    selected_career: str,
    publication_year_filter: str,
    exclude_devueltos: bool,
) -> tuple[dict, list[int], str, dict[str, int]]:
    source_years, normalized_pub_year_filter = _resolve_ip_source_years(evaluation_year, publication_year_filter)
    p_artistica, p_intelectual = _annual_pa_pia_totals(source_years)

    teachers_query = Teacher.query.filter(Teacher.year == evaluation_year)
    if selected_faculty != "ALL":
        teachers_query = teachers_query.filter(Teacher.faculty == selected_faculty)
    if selected_career != "ALL":
        teachers_query = teachers_query.filter(Teacher.career == selected_career)

    teacher_rows = teachers_query.all()
    teacher_ids = {str(teacher.teacher_id or "").strip() for teacher in teacher_rows if teacher.teacher_id}
    docentes_equivalentes = sum(_dedication_weight(teacher.dedication) for teacher in teacher_rows)
    denominator_components = {
        "tiempo_completo": 0,
        "medio_tiempo": 0,
        "tiempo_parcial": 0,
        "no_definida": 0,
    }

    for teacher in teacher_rows:
        dedication = str(teacher.dedication or "").strip().upper()
        if dedication == "EXCLUSIVA O TIEMPO COMPLETO":
            denominator_components["tiempo_completo"] += 1
        elif dedication == "SEMI EXCLUSIVA O MEDIO TIEMPO":
            denominator_components["medio_tiempo"] += 1
        elif dedication == "TIEMPO PARCIAL":
            denominator_components["tiempo_parcial"] += 1
        else:
            denominator_components["no_definida"] += 1

    publications = (
        Publication.query.filter(Publication.publication_year.in_(source_years))
        .order_by(Publication.created_at.desc(), Publication.id.desc())
        .all()
    )

    publication_ids = [publication.id for publication in publications]
    author_rows = []
    if publication_ids:
        author_rows = (
            PublicationAuthor.query.filter(PublicationAuthor.publication_id.in_(publication_ids))
            .order_by(PublicationAuthor.id.asc())
            .all()
        )

    authors_by_publication: dict[int, list[PublicationAuthor]] = defaultdict(list)
    for publication_author in author_rows:
        authors_by_publication[publication_author.publication_id].append(publication_author)

    grouped_by_sequence: dict[str, dict] = {}
    for publication in publications:
        publication_year_value = int(publication.publication_year or 0)
        if publication_year_value not in source_years:
            continue
        sequence = str(publication.publication_sequence or "").strip()
        if sequence:
            group_key = f"{publication_year_value}::{sequence}"
        else:
            group_key = f"{publication_year_value}::__NOSEQ__{publication.id}"
        grouped = grouped_by_sequence.setdefault(
            group_key,
            {
                "publication": publication,
                "authors": [],
            },
        )
        grouped["authors"].extend(authors_by_publication.get(publication.id, []))

    publicaciones_docentes = 0
    articulos_total = 0
    libros_total = 0
    capitulos_total = 0
    articulos_ponderados = 0.0
    capitulos_ponderados = 0.0

    for grouped in grouped_by_sequence.values():
        publication = grouped["publication"]
        publication_authors = grouped["authors"]

        if exclude_devueltos:
            has_devueltos = False
            for author in publication_authors:
                row_json = author.source_row_json or {}
                finalizar_value = str(row_json.get("FINALIZAR", "") or "").strip().upper()
                if finalizar_value == "DEVUELTOS":
                    has_devueltos = True
                    break
            if has_devueltos:
                continue

        ordered_author_ids = []
        seen_author_ids = set()
        for author in publication_authors:
            num_id, _ = _extract_author_identity(author)
            if not num_id or num_id in seen_author_ids:
                continue
            seen_author_ids.add(num_id)
            ordered_author_ids.append(num_id)

        if not any(author_id in teacher_ids for author_id in ordered_author_ids):
            continue

        publicaciones_docentes += 1
        publication_type = str(publication.publication_type or "").strip().upper()

        if publication_type == "A":
            articulos_total += 1
            _, weight = _article_category_and_weight(
                publication.quartile or "",
                publication.source_base or "",
            )
            articulos_ponderados += weight
        elif publication_type == "L":
            libros_total += 1
        elif publication_type == "C":
            capitulos_total += 1
            chapter_weight = _chapter_weight(publication_authors)
            capitulos_ponderados += chapter_weight

    numerador = articulos_ponderados + libros_total + capitulos_ponderados + p_artistica + p_intelectual
    ip_value = (numerador / docentes_equivalentes) if docentes_equivalentes > 0 else None

    summary = {
        "docentes_total": len(teacher_rows),
        "docentes_equivalentes": round(docentes_equivalentes, 4),
        "publicaciones_docentes": publicaciones_docentes,
        "articulos_total": articulos_total,
        "libros_total": libros_total,
        "capitulos_total": capitulos_total,
        "artistica_total": round(float(p_artistica), 4),
        "intelectual_total": round(float(p_intelectual), 4),
        "articulos_ponderados": round(articulos_ponderados, 4),
        "capitulos_ponderados": round(capitulos_ponderados, 4),
        "numerador": round(numerador, 4),
        "ip": round(ip_value, 6) if ip_value is not None else None,
        "pac": round(ip_value, 6) if ip_value is not None else None,
    }

    selected_period_label = "-"
    if len(source_years) == 3:
        selected_period_label = f"Período evaluación ({source_years[0]}, {source_years[1]}, {source_years[2]})"
    elif len(source_years) == 1:
        selected_period_label = str(source_years[0])

    return (
        summary,
        source_years,
        normalized_pub_year_filter if normalized_pub_year_filter == "ALL" else str(source_years[0]),
        denominator_components,
    )


@main_bp.route("/evaluacion/ip/pdf", endpoint="evaluacion_ip_pdf")
@main_bp.route("/evaluacion/pac/pdf", endpoint="evaluacion_pac_pdf")
@login_required
def evaluacion_ip_pdf():
    evaluation_year = request.args.get("year", type=int)
    selected_faculty = request.args.get("faculty", type=str, default="ALL")
    selected_career = request.args.get("career", type=str, default="ALL")
    publication_year_filter = request.args.get("pub_year", type=str, default="ALL")
    exclude_devueltos_values = request.args.getlist("exclude_devueltos")
    exclude_devueltos = "1" in exclude_devueltos_values if exclude_devueltos_values else True

    years = [
        row[0]
        for row in db.session.query(Teacher.year)
        .distinct()
        .order_by(Teacher.year.desc())
        .all()
    ]

    if evaluation_year is None:
        evaluation_year = years[0] if years else None

    if evaluation_year is None:
        flash("No hay años disponibles para generar el reporte iP.", "error")
        return redirect(url_for("main.evaluacion_ip"))

    summary, source_years, normalized_pub_year_filter, denominator_components = _compute_ip_summary(
        evaluation_year=evaluation_year,
        selected_faculty=selected_faculty,
        selected_career=selected_career,
        publication_year_filter=publication_year_filter,
        exclude_devueltos=exclude_devueltos,
    )

    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        leftMargin=1.5 * cm,
        rightMargin=1.5 * cm,
        topMargin=1.5 * cm,
        bottomMargin=1.5 * cm,
    )
    styles = getSampleStyleSheet()
    story = []

    period_label = ", ".join(str(year) for year in source_years) if source_years else "-"
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")

    story.append(Paragraph("Reporte iP (Índice de Producción Académica per cápita)", styles["Title"]))
    story.append(Spacer(1, 0.2 * cm))
    story.append(Paragraph(f"Año de evaluación: {evaluation_year}", styles["Normal"]))
    story.append(Paragraph(f"Facultad: {selected_faculty}", styles["Normal"]))
    story.append(Paragraph(f"Carrera: {selected_career}", styles["Normal"]))
    story.append(Paragraph(f"Período usado: {period_label}", styles["Normal"]))
    story.append(Paragraph(f"Filtro período publicaciones: {normalized_pub_year_filter}", styles["Normal"]))
    story.append(Paragraph(f"Excluir DEVUELTOS: {'Sí' if exclude_devueltos else 'No'}", styles["Normal"]))
    story.append(Paragraph(f"Fecha de generación: {generated_at}", styles["Normal"]))
    story.append(Spacer(1, 0.35 * cm))

    summary_table_data = [
        ["Indicador", "Valor"],
        ["Docentes considerados", str(summary["docentes_total"])],
        ["Docentes equivalentes", str(summary["docentes_equivalentes"])],
        ["Publicaciones con docente", str(summary["publicaciones_docentes"])],
        ["Artículos (total)", str(summary["articulos_total"])],
        ["Libros (total)", str(summary["libros_total"])],
        ["Capítulos (total)", str(summary["capitulos_total"])],
        ["Artículos ponderados", str(summary["articulos_ponderados"])],
        ["Capítulos ponderados", str(summary["capitulos_ponderados"])],
        ["Producción artística", str(summary["artistica_total"])],
        ["PIA (Producción intelectual aplicada)", str(summary["intelectual_total"])],
        ["Numerador iP", str(summary["numerador"])],
        ["iP", str(summary["pac"]) if summary["pac"] is not None else "-"],
    ]
    summary_table = Table(summary_table_data, colWidths=[10 * cm, 6 * cm])
    summary_table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#A90046")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#C9C9C9")),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.whitesmoke, colors.white]),
                ("ALIGN", (1, 1), (1, -1), "RIGHT"),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ]
        )
    )
    story.append(summary_table)
    story.append(Spacer(1, 0.35 * cm))

    denominator_table_data = [
        ["Componente denominador", "Total", "Factor"],
        ["Tiempo completo", str(denominator_components["tiempo_completo"]), "1.0"],
        ["Medio tiempo", str(denominator_components["medio_tiempo"]), "0.5"],
        ["Tiempo parcial", str(denominator_components["tiempo_parcial"]), "0.0"],
        ["No definida", str(denominator_components["no_definida"]), "0.0"],
    ]
    denominator_table = Table(denominator_table_data, colWidths=[8 * cm, 4 * cm, 4 * cm])
    denominator_table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#303132")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#C9C9C9")),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.whitesmoke, colors.white]),
                ("ALIGN", (1, 1), (-1, -1), "RIGHT"),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ]
        )
    )
    story.append(denominator_table)

    doc.build(story)
    buffer.seek(0)

    file_name = f"ip_{evaluation_year}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
    return send_file(buffer, mimetype="application/pdf", as_attachment=True, download_name=file_name)


@main_bp.route("/evaluacion/pa-pia", endpoint="evaluacion_pa_pia_mantenimiento")
@login_required
def evaluacion_pa_pia_mantenimiento():
    years_available = [
        int(row[0])
        for row in db.session.query(Publication.publication_year)
        .filter(Publication.publication_year.isnot(None))
        .distinct()
        .order_by(Publication.publication_year.desc())
        .all()
        if row[0]
    ]

    selected_year = request.args.get("year", type=int)
    if selected_year is None:
        selected_year = years_available[0] if years_available else datetime.now().year

    action = request.args.get("action", type=str, default="save").strip().lower()
    year_value = request.args.get("set_year", type=int)
    pa_raw = request.args.get("pa", type=str, default="")
    pia_raw = request.args.get("pia", type=str, default="")

    if year_value is not None and action in {"save", "delete"}:
        setting = PacArtisticSetting.query.filter_by(
            evaluation_year=year_value,
            faculty_scope="ALL",
            career_scope="ALL",
        ).first()

        if action == "delete":
            if setting is not None:
                db.session.delete(setting)
                db.session.commit()
                flash(f"Registro anual eliminado para {year_value}.", "success")
            else:
                flash(f"No existe registro anual para {year_value}.", "error")
            return redirect(url_for("main.evaluacion_pa_pia_mantenimiento", year=selected_year))

        pa_value = _parse_non_negative_float(pa_raw)
        pia_value = _parse_non_negative_float(pia_raw)
        if pa_value is None:
            pa_value = 0.0
        if pia_value is None:
            pia_value = 0.0

        if setting is None:
            setting = PacArtisticSetting(
                evaluation_year=year_value,
                faculty_scope="ALL",
                career_scope="ALL",
                artistic_value=round(pa_value, 4),
                intellectual_value=round(pia_value, 4),
            )
            db.session.add(setting)
        else:
            setting.artistic_value = round(pa_value, 4)
            setting.intellectual_value = round(pia_value, 4)
        db.session.commit()
        flash(f"Valores anuales guardados para {year_value}.", "success")
        return redirect(url_for("main.evaluacion_pa_pia_mantenimiento", year=year_value))

    yearly_rows = (
        PacArtisticSetting.query.filter_by(faculty_scope="ALL", career_scope="ALL")
        .order_by(PacArtisticSetting.evaluation_year.desc())
        .all()
    )
    yearly_data = [
        {
            "year": row.evaluation_year,
            "pa": round(float(row.artistic_value or 0.0), 4),
            "pia": round(float(row.intellectual_value or 0.0), 4),
            "updated_at": row.updated_at,
        }
        for row in yearly_rows
    ]

    current_setting = next((row for row in yearly_data if row["year"] == selected_year), None)
    selected_pa = current_setting["pa"] if current_setting else 0.0
    selected_pia = current_setting["pia"] if current_setting else 0.0

    return render_template(
        "main/evaluacion_pa_pia_mantenimiento.html",
        years_available=years_available,
        selected_year=selected_year,
        selected_pa=f"{selected_pa:g}",
        selected_pia=f"{selected_pia:g}",
        yearly_data=yearly_data,
    )


@main_bp.route("/evaluacion/ip", endpoint="evaluacion_ip")
@main_bp.route("/evaluacion/pac", endpoint="evaluacion_pac")
@login_required
def evaluacion_ip():
    p_artistica = 0.0
    p_intelectual = 0.0
    evaluation_year = request.args.get("year", type=int)
    selected_faculty = request.args.get("faculty", type=str, default="ALL")
    selected_career = request.args.get("career", type=str, default="ALL")
    publication_year_filter = request.args.get("pub_year", type=str, default="ALL")
    exclude_devueltos_values = request.args.getlist("exclude_devueltos")
    if exclude_devueltos_values:
        exclude_devueltos = "1" in exclude_devueltos_values
    else:
        exclude_devueltos = True

    years = [
        row[0]
        for row in db.session.query(Teacher.year)
        .distinct()
        .order_by(Teacher.year.desc())
        .all()
    ]
    if evaluation_year is None and years:
        evaluation_year = years[0]

    source_years = []
    publication_years_available = []
    faculty_options = []
    career_options = []
    selected_period_label = "-"

    dedication_counts: dict[str, int] = defaultdict(int)
    denominator_components = {
        "tiempo_completo": 0,
        "medio_tiempo": 0,
        "tiempo_parcial": 0,
        "no_definida": 0,
    }

    summary = {
        "docentes_total": 0,
        "docentes_equivalentes": 0.0,
        "publicaciones_docentes": 0,
        "articulos_total": 0,
        "libros_total": 0,
        "capitulos_total": 0,
        "artistica_total": 0,
        "intelectual_total": 0,
        "articulos_ponderados": 0.0,
        "capitulos_ponderados": 0.0,
        "numerador": 0.0,
        "pac": None,
    }

    quartile_stats: dict[str, dict[str, float | int]] = defaultdict(lambda: {"total": 0, "ponderado": 0.0})
    publication_type_stats: dict[str, dict[str, float | int]] = defaultdict(lambda: {"total": 0, "ponderado": 0.0})

    if evaluation_year is not None:
        default_source_years = [evaluation_year - 1, evaluation_year - 2, evaluation_year - 3]
        all_publication_years = [
            row[0]
            for row in db.session.query(Publication.publication_year)
            .filter(Publication.publication_year.isnot(None))
            .distinct()
            .order_by(Publication.publication_year.desc())
            .all()
        ]
        publication_years_available = sorted([int(year) for year in all_publication_years if year], reverse=True)

        if publication_year_filter != "ALL":
            try:
                publication_year_int = int(publication_year_filter)
                if publication_year_int in publication_years_available:
                    source_years = [publication_year_int]
                else:
                    source_years = default_source_years
                    publication_year_filter = "ALL"
            except (ValueError, TypeError):
                source_years = default_source_years
                publication_year_filter = "ALL"
        else:
            source_years = default_source_years

        if len(source_years) == 3:
            selected_period_label = f"Periodo evaluacion ({source_years[0]}, {source_years[1]}, {source_years[2]})"
        elif len(source_years) == 1:
            selected_period_label = str(source_years[0])

        faculty_options = [
            row[0]
            for row in db.session.query(Teacher.faculty)
            .filter(Teacher.year == evaluation_year, Teacher.faculty.isnot(None), Teacher.faculty != "")
            .distinct()
            .order_by(Teacher.faculty.asc())
            .all()
        ]

        if selected_faculty == "ALL":
            selected_career = "ALL"
            career_options = []
        else:
            career_options = [
                row[0]
                for row in db.session.query(Teacher.career)
                .filter(
                    Teacher.year == evaluation_year,
                    Teacher.faculty == selected_faculty,
                    Teacher.career.isnot(None),
                    Teacher.career != "",
                )
                .distinct()
                .order_by(Teacher.career.asc())
                .all()
            ]

            if selected_career != "ALL" and selected_career not in career_options:
                selected_career = "ALL"

        p_artistica, p_intelectual = _annual_pa_pia_totals(source_years)

        teachers_query = Teacher.query.filter(Teacher.year == evaluation_year)
        if selected_faculty != "ALL":
            teachers_query = teachers_query.filter(Teacher.faculty == selected_faculty)
        if selected_career != "ALL":
            teachers_query = teachers_query.filter(Teacher.career == selected_career)

        teacher_rows = teachers_query.all()
        teacher_ids = {str(teacher.teacher_id or "").strip() for teacher in teacher_rows if teacher.teacher_id}

        for teacher in teacher_rows:
            dedication = str(teacher.dedication or "").strip().upper()
            dedication_counts[dedication or "NO DEFINIDA"] += 1
            if dedication == "EXCLUSIVA O TIEMPO COMPLETO":
                denominator_components["tiempo_completo"] += 1
            elif dedication == "SEMI EXCLUSIVA O MEDIO TIEMPO":
                denominator_components["medio_tiempo"] += 1
            elif dedication == "TIEMPO PARCIAL":
                denominator_components["tiempo_parcial"] += 1
            else:
                denominator_components["no_definida"] += 1

        docentes_equivalentes = sum(_dedication_weight(teacher.dedication) for teacher in teacher_rows)

        publications = (
            Publication.query.filter(Publication.publication_year.in_(source_years))
            .order_by(Publication.created_at.desc(), Publication.id.desc())
            .all()
        )

        publication_ids = [publication.id for publication in publications]
        author_rows = []
        if publication_ids:
            author_rows = (
                PublicationAuthor.query.filter(PublicationAuthor.publication_id.in_(publication_ids))
                .order_by(PublicationAuthor.id.asc())
                .all()
            )

        authors_by_publication: dict[int, list[PublicationAuthor]] = defaultdict(list)
        for publication_author in author_rows:
            authors_by_publication[publication_author.publication_id].append(publication_author)

        grouped_by_sequence: dict[str, dict] = {}
        for publication in publications:
            publication_year_value = int(publication.publication_year or 0)
            if publication_year_value not in source_years:
                continue
            sequence = str(publication.publication_sequence or "").strip()
            if sequence:
                group_key = f"{publication_year_value}::{sequence}"
            else:
                group_key = f"{publication_year_value}::__NOSEQ__{publication.id}"
            grouped = grouped_by_sequence.setdefault(
                group_key,
                {
                    "publication": publication,
                    "authors": [],
                },
            )
            grouped["authors"].extend(authors_by_publication.get(publication.id, []))

        publicaciones_docentes = 0
        articulos_total = 0
        libros_total = 0
        capitulos_total = 0
        artistica_total = 0
        articulos_ponderados = 0.0
        capitulos_ponderados = 0.0

        for grouped in grouped_by_sequence.values():
            publication = grouped["publication"]
            publication_authors = grouped["authors"]

            if exclude_devueltos:
                has_devueltos = False
                for author in publication_authors:
                    row_json = author.source_row_json or {}
                    finalizar_value = str(row_json.get("FINALIZAR", "") or "").strip().upper()
                    if finalizar_value == "DEVUELTOS":
                        has_devueltos = True
                        break
                if has_devueltos:
                    continue

            ordered_author_ids = []
            seen_author_ids = set()
            for author in publication_authors:
                num_id, _ = _extract_author_identity(author)
                if not num_id or num_id in seen_author_ids:
                    continue
                seen_author_ids.add(num_id)
                ordered_author_ids.append(num_id)

            if not any(author_id in teacher_ids for author_id in ordered_author_ids):
                continue

            publicaciones_docentes += 1
            publication_type = str(publication.publication_type or "").strip().upper()

            if publication_type == "A":
                articulos_total += 1
                article_category, weight = _article_category_and_weight(
                    publication.quartile or "",
                    publication.source_base or "",
                )
                articulos_ponderados += weight
                quartile_stats[article_category]["total"] += 1
                quartile_stats[article_category]["ponderado"] += weight
                publication_type_stats["A"]["total"] += 1
                publication_type_stats["A"]["ponderado"] += weight
            elif publication_type == "L":
                libros_total += 1
                publication_type_stats["L"]["total"] += 1
                publication_type_stats["L"]["ponderado"] += 1.0
            elif publication_type == "C":
                capitulos_total += 1
                chapter_weight = _chapter_weight(publication_authors)
                capitulos_ponderados += chapter_weight
                publication_type_stats["C"]["total"] += 1
                publication_type_stats["C"]["ponderado"] += chapter_weight
            elif publication_type == "E":
                artistica_total += 1
                publication_type_stats["E"]["total"] += 1
                publication_type_stats["E"]["ponderado"] += 1.0

        numerador = articulos_ponderados + libros_total + capitulos_ponderados + p_artistica + p_intelectual
        ip_value = (numerador / docentes_equivalentes) if docentes_equivalentes > 0 else None

        summary = {
            "docentes_total": len(teacher_rows),
            "docentes_equivalentes": round(docentes_equivalentes, 4),
            "publicaciones_docentes": publicaciones_docentes,
            "articulos_total": articulos_total,
            "libros_total": libros_total,
            "capitulos_total": capitulos_total,
            "artistica_total": p_artistica,
            "intelectual_total": p_intelectual,
            "articulos_ponderados": round(articulos_ponderados, 4),
            "capitulos_ponderados": round(capitulos_ponderados, 4),
            "numerador": round(numerador, 4),
            "ip": round(ip_value, 6) if ip_value is not None else None,
            "pac": round(ip_value, 6) if ip_value is not None else None,
        }

    dedication_stats = sorted(dedication_counts.items(), key=lambda item: (-item[1], item[0]))
    article_order = {"Q1": 0, "Q2": 1, "Q3": 2, "Q4": 3, "ACI": 4, "REGIONAL": 5, "LATINDEX": 6}
    quartile_stats_rows = sorted(
        [(key, values["total"], round(float(values["ponderado"]), 4)) for key, values in quartile_stats.items()],
        key=lambda item: (article_order.get(item[0], 99), item[0]),
    )
    publication_type_stats_rows = sorted(
        [(key, values["total"], round(float(values["ponderado"]), 4)) for key, values in publication_type_stats.items()],
        key=lambda item: item[0],
    )

    return render_template(
        "main/evaluacion_ip.html",
        years=years,
        evaluation_year=evaluation_year,
        selected_faculty=selected_faculty,
        selected_career=selected_career,
        faculty_options=faculty_options,
        career_options=career_options,
        publication_year_filter=publication_year_filter,
        publication_years_available=publication_years_available,
        source_years=source_years,
        selected_period_label=selected_period_label,
        exclude_devueltos=exclude_devueltos,
        summary=summary,
        dedication_stats=dedication_stats,
        denominator_components=denominator_components,
        quartile_stats=quartile_stats_rows,
        publication_type_stats=publication_type_stats_rows,
    )


@main_bp.route("/evaluacion/ip/historico", endpoint="evaluacion_ip_historico")
@main_bp.route("/evaluacion/pac/historico", endpoint="evaluacion_pac_historico")
@login_required
def evaluacion_ip_historico():
    years = [
        row[0]
        for row in db.session.query(Teacher.year)
        .distinct()
        .order_by(Teacher.year.desc())
        .all()
    ]

    if not years:
        return render_template(
            "main/evaluacion_ip_historico.html",
            years=[],
            selected_year_start=None,
            selected_year_end=None,
            selected_faculty="ALL",
            selected_career="ALL",
            faculty_options=[],
            career_options=[],
            publication_year_filter="ALL",
            exclude_devueltos=True,
            history_rows=[],
            best_row=None,
            worst_row=None,
            average_pac=None,
            chart_labels=[],
            chart_values=[],
        )

    selected_year_end = request.args.get("year_end", type=int, default=years[0])
    selected_year_start = request.args.get("year_start", type=int, default=years[-1])

    if selected_year_end not in years:
        selected_year_end = years[0]
    if selected_year_start not in years:
        selected_year_start = years[-1]
    if selected_year_start > selected_year_end:
        selected_year_start, selected_year_end = selected_year_end, selected_year_start

    selected_faculty = request.args.get("faculty", type=str, default="ALL")
    selected_career = request.args.get("career", type=str, default="ALL")
    publication_year_filter = request.args.get("pub_year", type=str, default="ALL")

    exclude_devueltos_values = request.args.getlist("exclude_devueltos")
    if exclude_devueltos_values:
        exclude_devueltos = "1" in exclude_devueltos_values
    else:
        exclude_devueltos = True

    faculty_options = [
        row[0]
        for row in db.session.query(Teacher.faculty)
        .filter(Teacher.year == selected_year_end, Teacher.faculty.isnot(None), Teacher.faculty != "")
        .distinct()
        .order_by(Teacher.faculty.asc())
        .all()
    ]

    if selected_faculty == "ALL":
        selected_career = "ALL"
        career_options = []
    else:
        career_options = [
            row[0]
            for row in db.session.query(Teacher.career)
            .filter(
                Teacher.year == selected_year_end,
                Teacher.faculty == selected_faculty,
                Teacher.career.isnot(None),
                Teacher.career != "",
            )
            .distinct()
            .order_by(Teacher.career.asc())
            .all()
        ]
        if selected_career != "ALL" and selected_career not in career_options:
            selected_career = "ALL"

    target_years = [year for year in years if selected_year_start <= year <= selected_year_end]
    target_years = sorted(target_years)

    history_rows = []
    for evaluation_year in target_years:
        summary, source_years, normalized_pub_year_filter, _ = _compute_ip_summary(
            evaluation_year=evaluation_year,
            selected_faculty=selected_faculty,
            selected_career=selected_career,
            publication_year_filter=publication_year_filter,
            exclude_devueltos=exclude_devueltos,
        )

        history_rows.append(
            {
                "evaluation_year": evaluation_year,
                "period": ", ".join(str(y) for y in source_years),
                "docentes_total": summary["docentes_total"],
                "docentes_equivalentes": summary["docentes_equivalentes"],
                "publicaciones_docentes": summary["publicaciones_docentes"],
                "articulos_ponderados": summary["articulos_ponderados"],
                "libros_total": summary["libros_total"],
                "capitulos_ponderados": summary["capitulos_ponderados"],
                "numerador": summary["numerador"],
                "artistica": summary["artistica_total"],
                "intelectual": summary["intelectual_total"],
                "pac": summary["pac"],
                "publication_year_filter": normalized_pub_year_filter,
            }
        )

    valid_rows = [row for row in history_rows if row["pac"] is not None]
    best_row = max(valid_rows, key=lambda row: row["pac"]) if valid_rows else None
    worst_row = min(valid_rows, key=lambda row: row["pac"]) if valid_rows else None
    average_pac = round(sum(row["pac"] for row in valid_rows) / len(valid_rows), 6) if valid_rows else None

    chart_labels = [str(row["evaluation_year"]) for row in history_rows]
    chart_values = [row["pac"] if row["pac"] is not None else 0 for row in history_rows]

    return render_template(
        "main/evaluacion_ip_historico.html",
        years=years,
        selected_year_start=selected_year_start,
        selected_year_end=selected_year_end,
        selected_faculty=selected_faculty,
        selected_career=selected_career,
        faculty_options=faculty_options,
        career_options=career_options,
        publication_year_filter=publication_year_filter,
        exclude_devueltos=exclude_devueltos,
        history_rows=history_rows,
        best_row=best_row,
        worst_row=worst_row,
        average_pac=average_pac,
        chart_labels=chart_labels,
        chart_values=chart_values,
    )


def _collect_grouped_publication_view_state(
    selected_year: int | None,
    request_args,
    selected_faculty: str = "ALL",
    selected_career: str = "ALL",
    exclude_devueltos: bool = False,
) -> dict:
    available_columns: list[str] = []
    selected_columns: list[str] = []
    visible_filter_values: dict[str, str] = {}
    filtered_rows: list[dict] = []
    export_base_params: dict[str, list[str]] = {}
    column_param_map: dict[str, str] = {}
    search_query = request_args.get("search", type=str, default="").strip()

    if selected_year is None:
        return {
            "available_columns": available_columns,
            "selected_columns": selected_columns,
            "visible_filter_values": visible_filter_values,
            "filtered_rows": filtered_rows,
            "search_query": search_query,
            "export_csv_query": "",
            "export_xlsx_query": "",
            "column_param_map": column_param_map,
        }

    publications = (
        Publication.query.filter(Publication.publication_year == selected_year)
        .order_by(Publication.created_at.desc(), Publication.id.desc())
        .all()
    )

    teacher_scope_ids = None
    if selected_faculty != "ALL" or selected_career != "ALL":
        teacher_scope_query = db.session.query(Teacher.teacher_id).filter(Teacher.year == selected_year)
        if selected_faculty != "ALL":
            teacher_scope_query = teacher_scope_query.filter(Teacher.faculty == selected_faculty)
        if selected_career != "ALL":
            teacher_scope_query = teacher_scope_query.filter(Teacher.career == selected_career)

        teacher_scope_ids = {
            str(teacher_id or "").strip()
            for (teacher_id,) in teacher_scope_query.all()
            if teacher_id
        }

    publication_ids = [publication.id for publication in publications]
    author_rows = []
    if publication_ids:
        author_rows = (
            PublicationAuthor.query.filter(PublicationAuthor.publication_id.in_(publication_ids))
            .order_by(PublicationAuthor.id.asc())
            .all()
        )

    authors_by_publication: dict[int, list[PublicationAuthor]] = defaultdict(list)
    for publication_author in author_rows:
        authors_by_publication[publication_author.publication_id].append(publication_author)

    prepared_rows = []
    column_seen = set()

    excluded_grouped_columns = {
        "PARTICIPACION",
        "COD UNIDAD",
        "COD SUBUNIDAD",
        "COD PAR EMPLEAD",
        "TIPO DOCUMENTO",
        "NUMERO IDENTIFICACION",
        "PRIMER NOMBRE",
        "SEGUNDO NOMBRE",
        "APELLIDO PAT",
        "APELLIDO MAT",
        "FACULTAD",
        "TIPO EMPLEADO",
    }

    for publication in publications:
        publication_authors = authors_by_publication.get(publication.id, [])
        first_author = publication_authors[0] if publication_authors else None
        base_payload = _publication_row_payload(publication, first_author) if first_author else {
            "SECUENCIA": publication.publication_sequence or "",
            "COD TIPO": publication.publication_type or "",
            "ANIO PUBLICACION": str(publication.publication_year or ""),
            "DESCRIPCION": publication.title or "",
            "BASE": publication.source_base or "",
            "INDICE Q": publication.quartile or "",
            "NOMBRE REVISTA": publication.journal_name or "",
        }

        ordered_author_ids = []
        ordered_author_names = []
        seen_author_ids = set()
        for author in publication_authors:
            num_id, full_name = _extract_author_identity(author)
            if not num_id or num_id in seen_author_ids:
                continue
            seen_author_ids.add(num_id)
            ordered_author_ids.append(num_id)
            ordered_author_names.append(full_name)

        if teacher_scope_ids is not None:
            if not ordered_author_ids:
                continue
            if not any(author_id in teacher_scope_ids for author_id in ordered_author_ids):
                continue

        base_payload["AUTORES"] = "; ".join(ordered_author_ids)
        base_payload["AUTORES NOMBRES"] = "; ".join(ordered_author_names)
        base_payload["TOTAL AUTORES"] = str(len(ordered_author_ids))

        finalizar_value = str(base_payload.get("FINALIZAR", "") or "").strip().upper()
        if exclude_devueltos and finalizar_value == "DEVUELTOS":
            continue

        for excluded_column in excluded_grouped_columns:
            base_payload.pop(excluded_column, None)

        for key in base_payload.keys():
            if key not in column_seen:
                column_seen.add(key)
                available_columns.append(key)

        prepared_rows.append({"publication": publication, "values": base_payload})

    grouped_default_columns = [
        "SECUENCIA",
        "COD TIPO",
        "ANIO PUBLICACION",
        "DESCRIPCION",
        "AUTORES",
        "AUTORES NOMBRES",
        "TOTAL AUTORES",
        "BASE",
        "INDICE Q",
        "NOMBRE REVISTA",
    ]

    available_column_set = set(available_columns)
    ordered_defaults = [column for column in grouped_default_columns if column in available_column_set]
    remaining_columns = [column for column in available_columns if column not in ordered_defaults]
    available_columns = ordered_defaults + remaining_columns

    requested_columns = [column for column in request_args.getlist("columns") if column in available_column_set]
    if requested_columns:
        selected_columns = requested_columns
    else:
        selected_columns = [column for column in grouped_default_columns if column in available_column_set]
        if not selected_columns:
            selected_columns = available_columns[: min(8, len(available_columns))]

    for column in available_columns:
        column_param_map[column] = _publication_column_param(column)

    for column in selected_columns:
        visible_filter_values[column] = request_args.get(column_param_map[column], type=str, default="").strip()

    for row in prepared_rows:
        values = row["values"]

        if search_query:
            haystack = " ".join(values.get(column, "") for column in selected_columns).lower()
            if search_query.lower() not in haystack:
                continue

        matches_all_filters = True
        for column, filter_value in visible_filter_values.items():
            if filter_value and filter_value.lower() not in values.get(column, "").lower():
                matches_all_filters = False
                break

        if not matches_all_filters:
            continue

        filtered_rows.append(row)

    export_base_params = {"year": [str(selected_year)]}
    if selected_faculty != "ALL":
        export_base_params["faculty"] = [selected_faculty]
    if selected_career != "ALL":
        export_base_params["career"] = [selected_career]
    if exclude_devueltos:
        export_base_params["exclude_devueltos"] = ["1"]
    if search_query:
        export_base_params["search"] = [search_query]
    for column in selected_columns:
        export_base_params.setdefault("columns", []).append(column)
    for column, filter_value in visible_filter_values.items():
        if filter_value:
            export_base_params[column_param_map[column]] = [filter_value]

    csv_params = dict(export_base_params)
    csv_params["format"] = ["csv"]

    xlsx_params = dict(export_base_params)
    xlsx_params["format"] = ["xlsx"]

    return {
        "available_columns": available_columns,
        "selected_columns": selected_columns,
        "visible_filter_values": visible_filter_values,
        "filtered_rows": filtered_rows,
        "search_query": search_query,
        "export_csv_query": urlencode(csv_params, doseq=True),
        "export_xlsx_query": urlencode(xlsx_params, doseq=True),
        "column_param_map": column_param_map,
    }


@main_bp.route("/")
def index():
    if current_user.is_authenticated:
        return redirect(url_for("main.dashboard"))
    return redirect(url_for("auth.login"))


@main_bp.route("/dashboard")
@login_required
def dashboard():
    return render_template("main/dashboard.html")


@main_bp.route("/docentes/carga", methods=["GET", "POST"])
@limiter.limit("10/minute", methods=["POST"])
@login_required
def docentes_carga():
    form = TeacherUploadForm()
    selected_year = request.args.get("year", type=int)
    years = [row[0] for row in db.session.query(Teacher.year).distinct().order_by(Teacher.year.desc()).all()]
    recent_batches = (
        ImportBatch.query.filter_by(import_type="docentes")
        .order_by(ImportBatch.created_at.desc())
        .limit(10)
        .all()
    )

    if form.validate_on_submit():
        source_file = form.source_file.data
        year = form.year.data

        existing_year_count = Teacher.query.filter_by(year=year).count()
        if existing_year_count > 0 and form.merge_confirmed.data != "1":
            flash(
                f"Ya existen {existing_year_count} docentes para {year}. Confirma para fusionar y omitir repetidos.",
                "error",
            )
            return redirect(url_for("main.docentes_carga", year=year))

        upload_dir = Path("data/uploads/docentes")
        upload_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        source_filename = secure_filename(source_file.filename)
        disk_path = upload_dir / f"{timestamp}_{source_filename}"
        source_file.save(disk_path)

        import_batch = ImportBatch(
            import_type="docentes",
            year=year,
            source_file_name=source_filename,
            source_file_path=str(disk_path),
            status="processing",
            created_by=current_user.id,
        )
        db.session.add(import_batch)
        db.session.flush()

        try:
            parsed_rows, errors, defaulted_dedication_count = parse_teachers_file(disk_path)

            parsed_ids = {row.teacher_id for row in parsed_rows}
            existing_ids = set()
            if parsed_ids:
                existing_ids = {
                    teacher_id
                    for (teacher_id,) in db.session.query(Teacher.teacher_id)
                    .filter(Teacher.year == year, Teacher.teacher_id.in_(parsed_ids))
                    .all()
                }

            inserted_count = 0
            skipped_duplicates = 0

            import_batch.total_rows = len(parsed_rows) + len(errors)
            import_batch.valid_rows = 0
            import_batch.invalid_rows = len(errors)

            for row in parsed_rows:
                if row.teacher_id in existing_ids:
                    skipped_duplicates += 1
                    continue

                db.session.add(
                    Teacher(
                        import_batch_id=import_batch.id,
                        year=year,
                        teacher_id=row.teacher_id,
                        teacher_name=row.teacher_name,
                        category=row.category,
                        dedication=row.dedication,
                        faculty=row.faculty,
                        career=row.career,
                    )
                )
                existing_ids.add(row.teacher_id)
                inserted_count += 1

            import_batch.valid_rows = inserted_count

            import_batch.status = "completed"
            db.session.commit()

            if errors:
                flash(
                    f"Carga completada con observaciones: {inserted_count} insertadas, {len(errors)} inválidas.",
                    "error",
                )
            else:
                flash(f"Docentes cargados correctamente para {year}: {inserted_count} insertadas.", "success")

            if skipped_duplicates:
                flash(
                    f"Se omitieron {skipped_duplicates} docentes repetidos del año {year}.",
                    "error",
                )

            if defaulted_dedication_count:
                flash(
                    f"{defaulted_dedication_count} registros se cargaron con dedicación 'NO DEFINIDA' por ausencia de columna DEDICACION DOCENTE.",
                    "error",
                )

            return redirect(url_for("main.docentes_carga", year=year))
        except Exception:
            db.session.rollback()
            flash("Ocurrió un error durante la carga de docentes.", "error")

    return render_template(
        "main/docentes_carga.html",
        form=form,
        selected_year=selected_year,
        years=years,
        recent_batches=recent_batches,
    )


@main_bp.route("/docentes", methods=["GET"])
@login_required
def docentes():
    selected_year = request.args.get("year", type=int)
    selected_faculty = request.args.get("faculty", type=str, default="ALL")
    selected_career = request.args.get("career", type=str, default="ALL")
    active_tab = request.args.get("tab", type=str, default="registros")

    if active_tab not in {"registros", "frecuencias"}:
        active_tab = "registros"

    if selected_year is None:
        latest = Teacher.query.order_by(Teacher.year.desc()).first()
        selected_year = latest.year if latest else None

    teachers = []
    teachers_page_state = _paginate_items([], request.args, "docentes")
    faculty_options = []
    career_options = []
    filtered_total = 0
    if selected_year is not None:
        faculty_options = [
            row[0]
            for row in db.session.query(Teacher.faculty)
            .filter(Teacher.year == selected_year, Teacher.faculty.isnot(None), Teacher.faculty != "")
            .distinct()
            .order_by(Teacher.faculty.asc())
            .all()
        ]

        # El filtro de carrera depende de la facultad elegida.
        if selected_faculty == "ALL":
            selected_career = "ALL"
            career_options = []
        else:
            career_options = [
                row[0]
                for row in db.session.query(Teacher.career)
                .filter(
                    Teacher.year == selected_year,
                    Teacher.faculty == selected_faculty,
                    Teacher.career.isnot(None),
                    Teacher.career != "",
                )
                .distinct()
                .order_by(Teacher.career.asc())
                .all()
            ]

            if selected_career != "ALL" and selected_career not in career_options:
                selected_career = "ALL"

        filtered_query = Teacher.query.filter(Teacher.year == selected_year)
        if selected_faculty != "ALL":
            filtered_query = filtered_query.filter(Teacher.faculty == selected_faculty)
        if selected_career != "ALL":
            filtered_query = filtered_query.filter(Teacher.career == selected_career)

        ordered_teachers = filtered_query.order_by(Teacher.teacher_name.asc()).all()
        teachers_page_state = _paginate_items(ordered_teachers, request.args, "docentes")
        filtered_total = teachers_page_state["total_filtered"]
        teachers = teachers_page_state["rows"]

    years = [row[0] for row in db.session.query(Teacher.year).distinct().order_by(Teacher.year.desc()).all()]
    dedication_stats = []
    category_stats = []
    faculty_stats = []
    career_stats = []
    faculty_dedication_stats = []
    career_dedication_stats = []

    if selected_year is not None:
        stats_query = db.session.query(Teacher).filter(Teacher.year == selected_year)
        if selected_faculty != "ALL":
            stats_query = stats_query.filter(Teacher.faculty == selected_faculty)
        if selected_career != "ALL":
            stats_query = stats_query.filter(Teacher.career == selected_career)

        dedication_stats = (
            db.session.query(Teacher.dedication, db.func.count(Teacher.id))
            .filter(Teacher.id.in_(stats_query.with_entities(Teacher.id)))
            .group_by(Teacher.dedication)
            .order_by(db.func.count(Teacher.id).desc())
            .all()
        )

        category_stats = (
            db.session.query(Teacher.category, db.func.count(Teacher.id))
            .filter(Teacher.id.in_(stats_query.with_entities(Teacher.id)))
            .group_by(Teacher.category)
            .order_by(db.func.count(Teacher.id).desc())
            .all()
        )

        faculty_stats = (
            db.session.query(Teacher.faculty, db.func.count(Teacher.id))
            .filter(Teacher.id.in_(stats_query.with_entities(Teacher.id)))
            .group_by(Teacher.faculty)
            .order_by(db.func.count(Teacher.id).desc())
            .all()
        )

        career_stats = (
            db.session.query(Teacher.career, db.func.count(Teacher.id))
            .filter(Teacher.id.in_(stats_query.with_entities(Teacher.id)))
            .group_by(Teacher.career)
            .order_by(db.func.count(Teacher.id).desc())
            .all()
        )

        faculty_dedication_raw = (
            db.session.query(Teacher.faculty, Teacher.dedication, db.func.count(Teacher.id))
            .filter(Teacher.id.in_(stats_query.with_entities(Teacher.id)))
            .group_by(Teacher.faculty, Teacher.dedication)
            .order_by(Teacher.faculty.asc(), db.func.count(Teacher.id).desc())
            .all()
        )

        career_dedication_raw = (
            db.session.query(Teacher.career, Teacher.dedication, db.func.count(Teacher.id))
            .filter(Teacher.id.in_(stats_query.with_entities(Teacher.id)))
            .group_by(Teacher.career, Teacher.dedication)
            .order_by(Teacher.career.asc(), db.func.count(Teacher.id).desc())
            .all()
        )

        faculty_grouped = defaultdict(list)
        for faculty, dedication, total in faculty_dedication_raw:
            faculty_grouped[faculty or "(Sin facultad)"].append((dedication or "(Sin dedicación)", total))

        for faculty_name, rows in faculty_grouped.items():
            faculty_dedication_stats.append(
                {
                    "name": faculty_name,
                    "total": sum(total for _, total in rows),
                    "by_dedication": rows,
                }
            )

        faculty_dedication_stats.sort(key=lambda item: item["total"], reverse=True)

        career_grouped = defaultdict(list)
        for career, dedication, total in career_dedication_raw:
            career_grouped[career or "(Sin carrera)"] .append((dedication or "(Sin dedicación)", total))

        for career_name, rows in career_grouped.items():
            career_dedication_stats.append(
                {
                    "name": career_name,
                    "total": sum(total for _, total in rows),
                    "by_dedication": rows,
                }
            )

        career_dedication_stats.sort(key=lambda item: item["total"], reverse=True)

    return render_template(
        "main/docentes.html",
        teachers=teachers,
        years=years,
        selected_year=selected_year,
        selected_faculty=selected_faculty,
        selected_career=selected_career,
        faculty_options=faculty_options,
        career_options=career_options,
        active_tab=active_tab,
        filtered_total=filtered_total,
        teachers_page_state=teachers_page_state,
        dedication_stats=dedication_stats,
        category_stats=category_stats,
        faculty_stats=faculty_stats,
        career_stats=career_stats,
        faculty_dedication_stats=faculty_dedication_stats,
        career_dedication_stats=career_dedication_stats,
    )


@main_bp.route("/docentes/delete-year", methods=["POST"])
@limiter.limit("5/minute")
@login_required
def docentes_delete_year():
    year = request.form.get("year", type=int)
    if not year:
        flash("Debes indicar un año válido para eliminar.", "error")
        return redirect(url_for("main.docentes", tab="registros"))

    total_teachers = Teacher.query.filter_by(year=year).count()
    if total_teachers == 0:
        flash(f"No hay docentes cargados para el año {year}.", "error")
        return redirect(url_for("main.docentes", tab="registros"))

    related_batches = ImportBatch.query.filter_by(import_type="docentes", year=year).all()

    for batch in related_batches:
        file_path = Path(batch.source_file_path)
        if file_path.exists() and file_path.is_file():
            file_path.unlink()

    Teacher.query.filter_by(year=year).delete(synchronize_session=False)
    ImportBatch.query.filter_by(import_type="docentes", year=year).delete(synchronize_session=False)
    db.session.commit()

    flash(f"Se eliminó la carga de docentes del año {year} ({total_teachers} registros).", "success")
    return redirect(url_for("main.docentes", year=year, tab="registros"))


@main_bp.route("/docentes/export")
@login_required
def docentes_export():
    year = request.args.get("year", type=int)
    faculty = request.args.get("faculty", type=str, default="ALL")
    career = request.args.get("career", type=str, default="ALL")
    export_format = request.args.get("format", type=str, default="csv").lower()

    
    if not year:
        flash("Debes seleccionar un año para exportar.", "error")
        return redirect(url_for("main.docentes"))

    query = Teacher.query.filter(Teacher.year == year)
    if faculty != "ALL":
        query = query.filter(Teacher.faculty == faculty)
    if career != "ALL":
        query = query.filter(Teacher.career == career)

    teachers = query.order_by(Teacher.teacher_name.asc()).all()

    rows = [
        {
            "ID": t.teacher_id,
            "DOCENTE": t.teacher_name,
            "CATEGORIA": t.category or "",
            "FACULTAD": t.faculty or "",
            "CARRERA": t.career or "",
            "DEDICACION": t.dedication or "",
        }
        for t in teachers
    ]

    if export_format == "xlsx":
        df = pd.DataFrame(rows, columns=["ID", "DOCENTE", "CATEGORIA", "FACULTAD", "CARRERA", "DEDICACION"])
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Docentes")
        output.seek(0)
        return Response(
            output.getvalue(),
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename=docentes_{year}.xlsx"},
        )

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["ID", "DOCENTE", "CATEGORIA", "FACULTAD", "CARRERA", "DEDICACION"])
    for row in rows:
        writer.writerow([row["ID"], row["DOCENTE"], row["CATEGORIA"], row["FACULTAD"], row["CARRERA"], row["DEDICACION"]])

    response = Response(
        "\ufeff" + output.getvalue(),
        mimetype="text/csv; charset=utf-8",
        headers={"Content-Disposition": f"attachment; filename=docentes_{year}.csv"},
    )
    return response


@main_bp.route("/docentes/template")
@login_required
def docentes_template():
    template_path = Path(current_app.root_path) / "static" / "templates" / "docentes_template.csv"
    return send_file(
        template_path,
        as_attachment=True,
        download_name="docentes_template.csv",
        mimetype="text/csv",
    )


@main_bp.route("/publicaciones/carga", methods=["GET", "POST"])
@limiter.limit("10/minute", methods=["POST"])
@login_required
def publicaciones_carga():
    form = PublicationUploadForm()
    years = [
        row[0]
        for row in db.session.query(Publication.publication_year)
        .distinct()
        .order_by(Publication.publication_year.desc())
        .all()
    ]
    recent_batches = (
        ImportBatch.query.filter_by(import_type="publicaciones")
        .order_by(ImportBatch.created_at.desc())
        .limit(10)
        .all()
    )

    if form.validate_on_submit():
        source_file = form.source_file.data

        upload_dir = Path("data/uploads/publicaciones")
        upload_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        source_filename = secure_filename(source_file.filename)
        disk_path = upload_dir / f"{timestamp}_{source_filename}"
        source_file.save(disk_path)

        try:
            parsed_rows, errors = parse_publications_file(disk_path)

            if not parsed_rows:
                flash("No se encontraron filas válidas de publicaciones en el archivo.", "error")
                return redirect(url_for("main.publicaciones_carga"))

            rows_by_year = defaultdict(list)
            for row in parsed_rows:
                rows_by_year[row.publication_year].append(row)

            inserted_relations_total = 0
            skipped_duplicates_total = 0
            created_publications_total = 0
            processed_years = []

            for idx, (current_year, year_rows) in enumerate(sorted(rows_by_year.items())):
                import_batch = ImportBatch(
                    import_type="publicaciones",
                    year=current_year,
                    source_file_name=source_filename,
                    source_file_path=str(disk_path),
                    status="processing",
                    created_by=current_user.id,
                )
                db.session.add(import_batch)
                db.session.flush()

                existing_publications = Publication.query.filter(Publication.publication_year == current_year).all()
                publication_map = {}
                for pub in existing_publications:
                    key = (
                        (pub.publication_sequence or "").strip(),
                        (pub.publication_type or "").strip().upper(),
                        int(pub.publication_year),
                        " ".join((pub.title or "").split()).upper(),
                        (pub.source_base or "").strip().upper(),
                        (pub.quartile or "").strip().upper(),
                        (pub.journal_name or "").strip().upper(),
                    )
                    publication_map[key] = pub.id

                existing_author_keys = set()
                existing_author_rows = (
                    db.session.query(
                        Publication.publication_sequence,
                        Publication.publication_type,
                        Publication.publication_year,
                        Publication.title,
                        Publication.source_base,
                        Publication.quartile,
                        Publication.journal_name,
                        PublicationAuthor.teacher_id,
                    )
                    .join(PublicationAuthor, PublicationAuthor.publication_id == Publication.id)
                    .filter(Publication.publication_year == current_year)
                    .all()
                )
                for existing_row in existing_author_rows:
                    pub_key = (
                        (existing_row[0] or "").strip(),
                        (existing_row[1] or "").strip().upper(),
                        int(existing_row[2]),
                        " ".join((existing_row[3] or "").split()).upper(),
                        (existing_row[4] or "").strip().upper(),
                        (existing_row[5] or "").strip().upper(),
                        (existing_row[6] or "").strip().upper(),
                    )
                    existing_author_keys.add((pub_key, (existing_row[7] or "").strip()))

                inserted_relations = 0
                skipped_duplicates = 0
                created_publications = 0

                for row in year_rows:
                    pub_key = (
                        row.sequence.strip(),
                        row.publication_type.strip().upper(),
                        int(row.publication_year),
                        " ".join(row.title.split()).upper(),
                        (row.source_base or "").strip().upper(),
                        (row.quartile or "").strip().upper(),
                        (row.journal_name or "").strip().upper(),
                    )

                    publication_id = publication_map.get(pub_key)
                    if publication_id is None:
                        publication = Publication(
                            import_batch_id=import_batch.id,
                            publication_sequence=row.sequence,
                            publication_type=row.publication_type,
                            title=row.title,
                            publication_year=row.publication_year,
                            source_base=row.source_base,
                            quartile=(row.quartile[:32] if row.quartile else None),
                            journal_name=row.journal_name,
                        )
                        db.session.add(publication)
                        db.session.flush()
                        publication_id = publication.id
                        publication_map[pub_key] = publication_id
                        created_publications += 1

                    author_key = (pub_key, row.author_teacher_id.strip())
                    if author_key in existing_author_keys:
                        skipped_duplicates += 1
                        continue

                    db.session.add(
                        PublicationAuthor(
                            publication_id=publication_id,
                            teacher_id=row.author_teacher_id.strip(),
                            source_row_json=row.raw_fields,
                        )
                    )
                    existing_author_keys.add(author_key)
                    inserted_relations += 1

                # Si hay errores de parseo, se registran una sola vez en el primer lote generado.
                parse_errors_for_batch = len(errors) if idx == 0 else 0
                import_batch.valid_rows = inserted_relations
                import_batch.invalid_rows = parse_errors_for_batch
                import_batch.total_rows = len(year_rows) + parse_errors_for_batch
                import_batch.status = "completed"

                inserted_relations_total += inserted_relations
                skipped_duplicates_total += skipped_duplicates
                created_publications_total += created_publications
                processed_years.append(current_year)

            db.session.commit()

            if errors and inserted_relations_total == 0:
                flash(
                    f"No se pudo completar la carga: {len(errors)} filas inválidas.",
                    "error",
                )
            elif errors:
                flash(
                    f"Carga completada con observaciones: {inserted_relations_total} relaciones insertadas, {len(errors)} inválidas.",
                    "success",
                )
            else:
                flash(
                    f"Publicaciones cargadas correctamente: {inserted_relations_total} relaciones insertadas.",
                    "success",
                )

            if created_publications_total:
                flash(f"Se crearon {created_publications_total} publicaciones nuevas.", "success")

            if skipped_duplicates_total:
                flash(f"Se omitieron {skipped_duplicates_total} filas repetidas.", "error")

            if processed_years:
                years_label = ", ".join(str(y) for y in sorted(processed_years))
                flash(f"Años procesados en esta carga: {years_label}.", "success")

            return redirect(url_for("main.publicaciones_carga"))
        except Exception as exc:
            db.session.rollback()
            current_app.logger.exception("Error en carga de publicaciones")
            flash(f"Ocurrió un error durante la carga de publicaciones: {exc}", "error")

    return render_template(
        "main/publicaciones_carga.html",
        form=form,
        years=years,
        recent_batches=recent_batches,
    )


@main_bp.route("/publicaciones")
@login_required
def publicaciones():
    active_tab = request.args.get("tab", type=str, default="listado").strip().lower()
    if active_tab not in {"listado", "analisis"}:
        active_tab = "listado"

    selected_year = request.args.get("year", type=int)

    if selected_year is None:
        latest = Publication.query.order_by(Publication.publication_year.desc()).first()
        selected_year = latest.publication_year if latest else None

    years = [
        row[0]
        for row in db.session.query(Publication.publication_year)
        .distinct()
        .order_by(Publication.publication_year.desc())
        .all()
    ]

    publication_rows = []
    filtered_total = 0

    type_stats = []
    base_stats = []
    quartile_stats = []
    available_columns = []
    selected_columns = []
    visible_filter_values = {}
    column_param_map = {}
    search_query = ""
    export_csv_query = ""
    export_xlsx_query = ""
    publication_page_state = _paginate_items([], request.args, "pubreg")

    if selected_year is not None:
        view_state = _collect_publication_view_state(selected_year, request.args)
        available_columns = view_state["available_columns"]
        selected_columns = view_state["selected_columns"]
        visible_filter_values = view_state["visible_filter_values"]
        column_param_map = view_state["column_param_map"]
        search_query = view_state["search_query"]
        export_csv_query = view_state["export_csv_query"]
        export_xlsx_query = view_state["export_xlsx_query"]
        publication_page_state = _paginate_items(view_state["filtered_rows"], request.args, "pubreg")
        filtered_total = publication_page_state["total_filtered"]
        publication_rows = publication_page_state["rows"]

        publication_ids = {row["publication"].id for row in view_state["filtered_rows"]}
        stats_query = db.session.query(Publication).filter(Publication.id.in_(publication_ids))

        type_stats = (
            db.session.query(Publication.publication_type, db.func.count(Publication.id))
            .filter(Publication.id.in_(stats_query.with_entities(Publication.id)))
            .group_by(Publication.publication_type)
            .order_by(db.func.count(Publication.id).desc())
            .all()
        )

        base_stats = (
            db.session.query(Publication.source_base, db.func.count(Publication.id))
            .filter(Publication.id.in_(stats_query.with_entities(Publication.id)))
            .group_by(Publication.source_base)
            .order_by(db.func.count(Publication.id).desc())
            .all()
        )

        quartile_stats = (
            db.session.query(Publication.quartile, db.func.count(Publication.id))
            .filter(Publication.id.in_(stats_query.with_entities(Publication.id)))
            .group_by(Publication.quartile)
            .order_by(db.func.count(Publication.id).desc())
            .all()
        )

    return render_template(
        "main/publicaciones.html",
        active_tab=active_tab,
        years=years,
        selected_year=selected_year,
        publication_rows=publication_rows,
        filtered_total=filtered_total,
        type_stats=type_stats,
        base_stats=base_stats,
        quartile_stats=quartile_stats,
        available_columns=available_columns,
        selected_columns=selected_columns,
        visible_filter_values=visible_filter_values,
        column_param_map=column_param_map,
        search_query=search_query,
        export_csv_query=export_csv_query,
        export_xlsx_query=export_xlsx_query,
        publication_page_state=publication_page_state,
    )


@main_bp.route("/publicaciones/delete-year", methods=["POST"])
@limiter.limit("5/minute")
@login_required
def publicaciones_delete_year():
    year = request.form.get("year", type=int)
    if not year:
        flash("Debes indicar un año válido para eliminar.", "error")
        return redirect(url_for("main.publicaciones"))

    publications_for_year = Publication.query.filter_by(publication_year=year).all()
    total_publications = len(publications_for_year)

    if total_publications == 0:
        flash(f"No hay publicaciones cargadas para el año {year}.", "error")
        return redirect(url_for("main.publicaciones", year=year))

    related_batches = ImportBatch.query.filter_by(import_type="publicaciones", year=year).all()
    for batch in related_batches:
        file_path = Path(batch.source_file_path)
        if file_path.exists() and file_path.is_file():
            file_path.unlink()

    for publication in publications_for_year:
        db.session.delete(publication)

    ImportBatch.query.filter_by(import_type="publicaciones", year=year).delete(synchronize_session=False)
    db.session.commit()

    flash(f"Se eliminó la carga de publicaciones del año {year} ({total_publications} registros).", "success")
    return redirect(url_for("main.publicaciones", year=year))


@main_bp.route("/publicaciones/export")
@login_required
def publicaciones_export():
    year = request.args.get("year", type=int)
    export_format = request.args.get("format", type=str, default="csv").lower()

    if not year:
        flash("Debes seleccionar un año para exportar.", "error")
        return redirect(url_for("main.publicaciones"))

    view_state = _collect_publication_view_state(year, request.args)
    selected_columns = view_state["selected_columns"]
    export_rows = [
        {column: row["values"].get(column, "") for column in selected_columns}
        for row in view_state["filtered_rows"]
    ]

    if not selected_columns:
        flash("No hay columnas visibles para exportar.", "error")
        return redirect(url_for("main.publicaciones", year=year))

    if export_format == "xlsx":
        df = pd.DataFrame(export_rows, columns=selected_columns)
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Publicaciones")
        output.seek(0)
        return Response(
            output.getvalue(),
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename=publicaciones_{year}.xlsx"},
        )

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(selected_columns)
    for row in export_rows:
        writer.writerow([row.get(column, "") for column in selected_columns])

    return Response(
        "\ufeff" + output.getvalue(),
        mimetype="text/csv; charset=utf-8",
        headers={"Content-Disposition": f"attachment; filename=publicaciones_{year}.csv"},
    )


@main_bp.route("/publicaciones/consulta")
@login_required
def publicaciones_consulta():
    active_tab = request.args.get("tab", type=str, default="listado").strip().lower()
    if active_tab not in {"listado", "analisis"}:
        active_tab = "listado"

    selected_year = request.args.get("year", type=int)
    selected_faculty = request.args.get("faculty", type=str, default="ALL")
    selected_career = request.args.get("career", type=str, default="ALL")
    exclude_devueltos_values = request.args.getlist("exclude_devueltos")
    if exclude_devueltos_values:
        exclude_devueltos = "1" in exclude_devueltos_values
    else:
        exclude_devueltos = True

    if selected_year is None:
        latest = Publication.query.order_by(Publication.publication_year.desc()).first()
        selected_year = latest.publication_year if latest else None

    years = [
        row[0]
        for row in db.session.query(Publication.publication_year)
        .distinct()
        .order_by(Publication.publication_year.desc())
        .all()
    ]

    faculty_options = []
    career_options = []

    publication_rows = []
    filtered_total = 0
    available_columns = []
    selected_columns = []
    visible_filter_values = {}
    column_param_map = {}
    search_query = ""
    export_csv_query = ""
    export_xlsx_query = ""
    publication_page_state = _paginate_items([], request.args, "pubcon")
    type_stats = []
    base_stats = []
    quartile_stats = []

    if selected_year is not None:
        faculty_options = [
            row[0]
            for row in db.session.query(Teacher.faculty)
            .filter(Teacher.year == selected_year, Teacher.faculty.isnot(None), Teacher.faculty != "")
            .distinct()
            .order_by(Teacher.faculty.asc())
            .all()
        ]

        if selected_faculty == "ALL":
            selected_career = "ALL"
            career_options = []
        else:
            career_options = [
                row[0]
                for row in db.session.query(Teacher.career)
                .filter(
                    Teacher.year == selected_year,
                    Teacher.faculty == selected_faculty,
                    Teacher.career.isnot(None),
                    Teacher.career != "",
                )
                .distinct()
                .order_by(Teacher.career.asc())
                .all()
            ]

            if selected_career != "ALL" and selected_career not in career_options:
                selected_career = "ALL"

        view_state = _collect_grouped_publication_view_state(
            selected_year,
            request.args,
            selected_faculty=selected_faculty,
            selected_career=selected_career,
            exclude_devueltos=exclude_devueltos,
        )
        available_columns = view_state["available_columns"]
        selected_columns = view_state["selected_columns"]
        visible_filter_values = view_state["visible_filter_values"]
        column_param_map = view_state["column_param_map"]
        search_query = view_state["search_query"]
        export_csv_query = view_state["export_csv_query"]
        export_xlsx_query = view_state["export_xlsx_query"]
        publication_page_state = _paginate_items(view_state["filtered_rows"], request.args, "pubcon")
        filtered_total = publication_page_state["total_filtered"]
        publication_rows = publication_page_state["rows"]

        type_counter = defaultdict(int)
        base_counter = defaultdict(int)
        quartile_counter = defaultdict(int)

        for row in view_state["filtered_rows"]:
            values = row["values"]

            type_key = (values.get("COD TIPO", "") or "").strip() or "(Sin tipo)"
            base_key = (values.get("BASE", "") or "").strip() or "(Sin base)"
            quartile_key = (values.get("INDICE Q", "") or "").strip() or "(Sin cuartil)"

            type_counter[type_key] += 1
            base_counter[base_key] += 1
            quartile_counter[quartile_key] += 1

        type_stats = sorted(type_counter.items(), key=lambda item: (-item[1], item[0]))
        base_stats = sorted(base_counter.items(), key=lambda item: (-item[1], item[0]))
        quartile_stats = sorted(quartile_counter.items(), key=lambda item: (-item[1], item[0]))

    return render_template(
        "main/publicaciones_consulta.html",
        active_tab=active_tab,
        years=years,
        selected_year=selected_year,
        selected_faculty=selected_faculty,
        selected_career=selected_career,
        exclude_devueltos=exclude_devueltos,
        faculty_options=faculty_options,
        career_options=career_options,
        publication_rows=publication_rows,
        filtered_total=filtered_total,
        type_stats=type_stats,
        base_stats=base_stats,
        quartile_stats=quartile_stats,
        available_columns=available_columns,
        selected_columns=selected_columns,
        visible_filter_values=visible_filter_values,
        column_param_map=column_param_map,
        search_query=search_query,
        export_csv_query=export_csv_query,
        export_xlsx_query=export_xlsx_query,
        publication_page_state=publication_page_state,
    )


@main_bp.route("/publicaciones/consulta/export")
@login_required
def publicaciones_consulta_export():
    year = request.args.get("year", type=int)
    selected_faculty = request.args.get("faculty", type=str, default="ALL")
    selected_career = request.args.get("career", type=str, default="ALL")
    exclude_devueltos_values = request.args.getlist("exclude_devueltos")
    if exclude_devueltos_values:
        exclude_devueltos = "1" in exclude_devueltos_values
    else:
        exclude_devueltos = True
    export_format = request.args.get("format", type=str, default="csv").lower()

    if not year:
        flash("Debes seleccionar un año para exportar.", "error")
        return redirect(url_for("main.publicaciones_consulta"))

    view_state = _collect_grouped_publication_view_state(
        year,
        request.args,
        selected_faculty=selected_faculty,
        selected_career=selected_career,
        exclude_devueltos=exclude_devueltos,
    )
    selected_columns = view_state["selected_columns"]
    export_rows = [
        {column: row["values"].get(column, "") for column in selected_columns}
        for row in view_state["filtered_rows"]
    ]

    if not selected_columns:
        flash("No hay columnas visibles para exportar.", "error")
        return redirect(url_for("main.publicaciones_consulta", year=year))

    if export_format == "xlsx":
        df = pd.DataFrame(export_rows, columns=selected_columns)
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Publicaciones")
        output.seek(0)
        return Response(
            output.getvalue(),
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename=publicaciones_consulta_{year}.xlsx"},
        )

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(selected_columns)
    for row in export_rows:
        writer.writerow([row.get(column, "") for column in selected_columns])

    return Response(
        "\ufeff" + output.getvalue(),
        mimetype="text/csv; charset=utf-8",
        headers={"Content-Disposition": f"attachment; filename=publicaciones_consulta_{year}.csv"},
    )


@main_bp.route("/publicaciones/template")
@login_required
def publicaciones_template():
    template_path = Path(current_app.root_path) / "static" / "templates" / "publicaciones_template.csv"
    return send_file(
        template_path,
        as_attachment=True,
        download_name="publicaciones_template.csv",
        mimetype="text/csv",
    )


@main_bp.route("/cruce")
@login_required
def cruce():
    selected_year = request.args.get("year", type=int)
    active_tab = request.args.get("tab", type=str, default="con-docentes")

    if active_tab not in {"con-docentes", "sin-docentes"}:
        active_tab = "con-docentes"

    teacher_years = {
        row[0]
        for row in db.session.query(Teacher.year)
        .distinct()
        .order_by(Teacher.year.desc())
        .all()
    }
    publication_years = {
        row[0]
        for row in db.session.query(Publication.publication_year)
        .distinct()
        .order_by(Publication.publication_year.desc())
        .all()
    }
    years = sorted(teacher_years.union(publication_years), reverse=True)

    if selected_year is None and years:
        selected_year = years[0]

    with_teachers_rows = []
    without_teachers_rows = []
    summary = {
        "teachers_total": 0,
        "publications_total": 0,
        "publications_with_teachers": 0,
        "publications_without_teachers": 0,
        "teacher_ids_with_publications": 0,
    }

    if selected_year is not None:
        teacher_rows = Teacher.query.filter(Teacher.year == selected_year).all()
        teacher_ids = {str(teacher.teacher_id or "").strip() for teacher in teacher_rows if teacher.teacher_id}
        teacher_name_by_id = {
            str(teacher.teacher_id or "").strip(): str(teacher.teacher_name or "").strip()
            for teacher in teacher_rows
            if teacher.teacher_id
        }

        publications = (
            Publication.query.filter(Publication.publication_year == selected_year)
            .order_by(Publication.created_at.desc(), Publication.id.desc())
            .all()
        )

        publication_ids = [publication.id for publication in publications]
        author_rows = []
        if publication_ids:
            author_rows = (
                PublicationAuthor.query.filter(PublicationAuthor.publication_id.in_(publication_ids))
                .order_by(PublicationAuthor.id.asc())
                .all()
            )

        authors_by_publication: dict[int, list[PublicationAuthor]] = defaultdict(list)
        for publication_author in author_rows:
            authors_by_publication[publication_author.publication_id].append(publication_author)

        grouped_by_sequence: dict[str, dict] = {}
        for publication in publications:
            sequence = str(publication.publication_sequence or "").strip()
            group_key = sequence if sequence else f"__NOSEQ__{publication.id}"
            grouped = grouped_by_sequence.setdefault(
                group_key,
                {
                    "sequence": sequence,
                    "publication": publication,
                    "authors": [],
                },
            )
            grouped["authors"].extend(authors_by_publication.get(publication.id, []))

        teacher_ids_with_publications = set()

        for grouped in grouped_by_sequence.values():
            publication = grouped["publication"]
            publication_authors = grouped["authors"]

            ordered_author_ids = []
            ordered_author_names = []
            seen_author_ids = set()

            for author in publication_authors:
                num_id, full_name = _extract_author_identity(author)
                if not num_id or num_id in seen_author_ids:
                    continue
                seen_author_ids.add(num_id)
                ordered_author_ids.append(num_id)
                ordered_author_names.append(full_name)

            matched_teacher_ids = [num_id for num_id in ordered_author_ids if num_id in teacher_ids]
            matched_teacher_labels = []
            for teacher_id in matched_teacher_ids:
                teacher_name = teacher_name_by_id.get(teacher_id, "")
                if teacher_name:
                    matched_teacher_labels.append(f"{teacher_id} - {teacher_name}")
                else:
                    matched_teacher_labels.append(teacher_id)

            teacher_ids_with_publications.update(matched_teacher_ids)

            row = {
                "SECUENCIA": publication.publication_sequence or "",
                "COD TIPO": publication.publication_type or "",
                "ANIO PUBLICACION": str(publication.publication_year or ""),
                "DESCRIPCION": publication.title or "",
                "AUTORES": "; ".join(ordered_author_ids),
                "AUTORES NOMBRES": "; ".join(ordered_author_names),
                "AUTORES DOCENTES": "; ".join(matched_teacher_labels),
                "TOTAL AUTORES": len(ordered_author_ids),
                "TOTAL DOCENTES": len(matched_teacher_ids),
                "BASE": publication.source_base or "",
                "INDICE Q": publication.quartile or "",
                "NOMBRE REVISTA": publication.journal_name or "",
            }

            if matched_teacher_ids:
                with_teachers_rows.append(row)
            else:
                without_teachers_rows.append(row)

        summary = {
            "teachers_total": len(teacher_ids),
            "publications_total": len(grouped_by_sequence),
            "publications_with_teachers": len(with_teachers_rows),
            "publications_without_teachers": len(without_teachers_rows),
            "teacher_ids_with_publications": len(teacher_ids_with_publications),
        }

    with_columns = [
        "SECUENCIA",
        "COD TIPO",
        "DESCRIPCION",
        "AUTORES",
        "AUTORES NOMBRES",
        "AUTORES DOCENTES",
        "TOTAL DOCENTES",
    ]
    without_columns = [
        "SECUENCIA",
        "COD TIPO",
        "DESCRIPCION",
        "AUTORES",
        "AUTORES NOMBRES",
        "TOTAL AUTORES",
    ]

    with_state = _filter_and_paginate_rows(with_teachers_rows, request.args, with_columns, "cruce_con")
    without_state = _filter_and_paginate_rows(without_teachers_rows, request.args, without_columns, "cruce_sin")

    return render_template(
        "main/cruce.html",
        years=years,
        selected_year=selected_year,
        active_tab=active_tab,
        with_teachers_rows=with_state["rows"],
        without_teachers_rows=without_state["rows"],
        with_state=with_state,
        without_state=without_state,
        summary=summary,
    )


@main_bp.route("/evaluacion/publicaciones")
@login_required
def publicaciones_evaluacion():
    active_tab = request.args.get("tab", type=str, default="con-docente").strip().lower()
    if active_tab not in {"con-docente", "sin-docente"}:
        active_tab = "con-docente"

    evaluation_year = request.args.get("year", type=int)
    selected_faculty = request.args.get("faculty", type=str, default="ALL")
    selected_career = request.args.get("career", type=str, default="ALL")
    publication_year_filter = request.args.get("pub_year", type=str, default="ALL")
    exclude_devueltos_values = request.args.getlist("exclude_devueltos")
    if exclude_devueltos_values:
        exclude_devueltos = "1" in exclude_devueltos_values
    else:
        exclude_devueltos = True

    teacher_years = [
        row[0]
        for row in db.session.query(Teacher.year)
        .distinct()
        .order_by(Teacher.year.desc())
        .all()
    ]
    publication_years = {
        row[0]
        for row in db.session.query(Publication.publication_year)
        .distinct()
        .order_by(Publication.publication_year.desc())
        .all()
    }

    years = teacher_years or sorted({year + 1 for year in publication_years}, reverse=True)
    if evaluation_year is None and years:
        evaluation_year = years[0]

    source_years = []
    with_teachers_rows = []
    without_teachers_rows = []
    faculty_options = []
    career_options = []
    publication_years_available = []
    selected_period_label = "-"
    summary = {
        "teachers_total": 0,
        "publications_total_window": 0,
        "publications_with_teachers": 0,
        "publications_without_teachers": 0,
        "teacher_ids_with_publications": 0,
    }

    if evaluation_year is not None:
        default_source_years = [evaluation_year - 1, evaluation_year - 2, evaluation_year - 3]
        
        # Obtener todos los años de publicaciones disponibles (no solo del período)
        all_publication_years = [
            row[0]
            for row in db.session.query(Publication.publication_year)
            .filter(Publication.publication_year.isnot(None))
            .distinct()
            .order_by(Publication.publication_year.desc())
            .all()
        ]
        publication_years_available = sorted([int(y) for y in all_publication_years if y], reverse=True)

        # Determinar qué años consultar según el filtro de publicaciones
        if publication_year_filter != "ALL":
            try:
                pub_year_int = int(publication_year_filter)
                if pub_year_int in publication_years_available:
                    source_years = [pub_year_int]
                else:
                    source_years = default_source_years
                    publication_year_filter = "ALL"
            except (ValueError, TypeError):
                source_years = default_source_years
                publication_year_filter = "ALL"
        else:
            source_years = default_source_years

        if len(source_years) == 3:
            selected_period_label = f"Periodo evaluacion ({source_years[0]}, {source_years[1]}, {source_years[2]})"
        elif len(source_years) == 1:
            selected_period_label = str(source_years[0])

        faculty_options = [
            row[0]
            for row in db.session.query(Teacher.faculty)
            .filter(Teacher.year == evaluation_year, Teacher.faculty.isnot(None), Teacher.faculty != "")
            .distinct()
            .order_by(Teacher.faculty.asc())
            .all()
        ]

        if selected_faculty == "ALL":
            selected_career = "ALL"
            career_options = []
        else:
            career_options = [
                row[0]
                for row in db.session.query(Teacher.career)
                .filter(
                    Teacher.year == evaluation_year,
                    Teacher.faculty == selected_faculty,
                    Teacher.career.isnot(None),
                    Teacher.career != "",
                )
                .distinct()
                .order_by(Teacher.career.asc())
                .all()
            ]

            if selected_career != "ALL" and selected_career not in career_options:
                selected_career = "ALL"

        # Cuando se filtra por facultad/carrera, la vista "Sin docente" no aplica.
        if selected_faculty != "ALL" or selected_career != "ALL":
            active_tab = "con-docente"

        teachers_query = Teacher.query.filter(Teacher.year == evaluation_year)
        if selected_faculty != "ALL":
            teachers_query = teachers_query.filter(Teacher.faculty == selected_faculty)
        if selected_career != "ALL":
            teachers_query = teachers_query.filter(Teacher.career == selected_career)

        teacher_rows = teachers_query.all()
        teacher_ids = {str(teacher.teacher_id or "").strip() for teacher in teacher_rows if teacher.teacher_id}
        teacher_name_by_id = {
            str(teacher.teacher_id or "").strip(): str(teacher.teacher_name or "").strip()
            for teacher in teacher_rows
            if teacher.teacher_id
        }

        publications = (
            Publication.query.filter(Publication.publication_year.in_(source_years))
            .order_by(Publication.created_at.desc(), Publication.id.desc())
            .all()
        )

        publication_ids = [publication.id for publication in publications]
        author_rows = []
        if publication_ids:
            author_rows = (
                PublicationAuthor.query.filter(PublicationAuthor.publication_id.in_(publication_ids))
                .order_by(PublicationAuthor.id.asc())
                .all()
            )

        authors_by_publication: dict[int, list[PublicationAuthor]] = defaultdict(list)
        for publication_author in author_rows:
            authors_by_publication[publication_author.publication_id].append(publication_author)

        grouped_by_sequence: dict[str, dict] = {}
        for publication in publications:
            publication_year_value = int(publication.publication_year or 0)
            if publication_year_value not in source_years:
                continue
            sequence = str(publication.publication_sequence or "").strip()
            if sequence:
                group_key = f"{publication_year_value}::{sequence}"
            else:
                group_key = f"{publication_year_value}::__NOSEQ__{publication.id}"
            grouped = grouped_by_sequence.setdefault(
                group_key,
                {
                    "publication_year": publication_year_value,
                    "sequence": sequence,
                    "publication": publication,
                    "authors": [],
                },
            )
            grouped["authors"].extend(authors_by_publication.get(publication.id, []))

        teacher_ids_with_publications = set()
        with_available_columns = []
        without_available_columns = []
        with_column_seen = set()
        without_column_seen = set()
        excluded_grouped_columns = {
            "PARTICIPACION",
            "COD UNIDAD",
            "COD SUBUNIDAD",
            "COD PAR EMPLEAD",
            "TIPO DOCUMENTO",
            "NUMERO IDENTIFICACION",
            "PRIMER NOMBRE",
            "SEGUNDO NOMBRE",
            "APELLIDO PAT",
            "APELLIDO MAT",
            "FACULTAD",
            "TIPO EMPLEADO",
        }

        for grouped in grouped_by_sequence.values():
            publication = grouped["publication"]
            publication_year_value = grouped["publication_year"]
            publication_authors = grouped["authors"]
            first_author = publication_authors[0] if publication_authors else None
            base_payload = _publication_row_payload(publication, first_author) if first_author else {
                "SECUENCIA": publication.publication_sequence or "",
                "COD TIPO": publication.publication_type or "",
                "ANIO PUBLICACION": str(publication.publication_year or ""),
                "DESCRIPCION": publication.title or "",
                "BASE": publication.source_base or "",
                "INDICE Q": publication.quartile or "",
                "NOMBRE REVISTA": publication.journal_name or "",
            }

            if exclude_devueltos:
                has_devueltos = False
                for author in publication_authors:
                    row_json = author.source_row_json or {}
                    finalizar_value = str(row_json.get("FINALIZAR", "") or "").strip().upper()
                    if finalizar_value == "DEVUELTOS":
                        has_devueltos = True
                        break
                if has_devueltos:
                    continue

            ordered_author_ids = []
            ordered_author_names = []
            seen_author_ids = set()
            for author in publication_authors:
                num_id, full_name = _extract_author_identity(author)
                if not num_id or num_id in seen_author_ids:
                    continue
                seen_author_ids.add(num_id)
                ordered_author_ids.append(num_id)
                ordered_author_names.append(full_name)

            matched_teacher_ids = [num_id for num_id in ordered_author_ids if num_id in teacher_ids]
            matched_teacher_labels = []
            for teacher_id in matched_teacher_ids:
                teacher_name = teacher_name_by_id.get(teacher_id, "")
                if teacher_name:
                    matched_teacher_labels.append(f"{teacher_id} - {teacher_name}")
                else:
                    matched_teacher_labels.append(teacher_id)

            base_payload["AUTORES"] = "; ".join(ordered_author_ids)
            base_payload["AUTORES NOMBRES"] = "; ".join(ordered_author_names)
            base_payload["AUTORES DOCENTES"] = "; ".join(matched_teacher_labels)
            base_payload["TOTAL AUTORES"] = str(len(ordered_author_ids))
            base_payload["TOTAL DOCENTES"] = str(len(matched_teacher_ids))

            for excluded_column in excluded_grouped_columns:
                base_payload.pop(excluded_column, None)

            row_payload = {str(key): "" if value is None else str(value) for key, value in base_payload.items()}

            if matched_teacher_ids:
                teacher_ids_with_publications.update(matched_teacher_ids)
                with_teachers_rows.append(row_payload)
                for key in row_payload.keys():
                    if key not in with_column_seen:
                        with_column_seen.add(key)
                        with_available_columns.append(key)
            else:
                without_teachers_rows.append(row_payload)
                for key in row_payload.keys():
                    if key not in without_column_seen:
                        without_column_seen.add(key)
                        without_available_columns.append(key)

        summary = {
            "teachers_total": len(teacher_ids),
            "publications_total_window": len(with_teachers_rows) + len(without_teachers_rows),
            "publications_with_teachers": len(with_teachers_rows),
            "publications_without_teachers": len(without_teachers_rows),
            "teacher_ids_with_publications": len(teacher_ids_with_publications),
        }

    eval_with_default_columns = [
        "SECUENCIA",
        "ANIO PUBLICACION",
        "COD TIPO",
        "DESCRIPCION",
        "AUTORES",
        "AUTORES NOMBRES",
        "AUTORES DOCENTES",
        "TOTAL DOCENTES",
        "BASE",
        "INDICE Q",
        "NOMBRE REVISTA",
    ]
    eval_without_default_columns = [
        "SECUENCIA",
        "ANIO PUBLICACION",
        "COD TIPO",
        "DESCRIPCION",
        "AUTORES",
        "AUTORES NOMBRES",
        "TOTAL AUTORES",
        "BASE",
        "INDICE Q",
        "NOMBRE REVISTA",
    ]

    if evaluation_year is not None:
        with_available_set = set(with_available_columns)
        eval_with_columns = [column for column in eval_with_default_columns if column in with_available_set] + [
            column for column in with_available_columns if column not in eval_with_default_columns
        ]
        if not eval_with_columns:
            eval_with_columns = list(eval_with_default_columns)

        without_available_set = set(without_available_columns)
        eval_without_columns = [column for column in eval_without_default_columns if column in without_available_set] + [
            column for column in without_available_columns if column not in eval_without_default_columns
        ]
        if not eval_without_columns:
            eval_without_columns = list(eval_without_default_columns)
    else:
        eval_with_columns = list(eval_with_default_columns)
        eval_without_columns = list(eval_without_default_columns)

    requested_eval_with_columns = [
        column for column in request.args.getlist("eval_con_columns") if column in eval_with_columns
    ]
    selected_eval_with_columns = requested_eval_with_columns or [
        "SECUENCIA",
        "ANIO PUBLICACION",
        "COD TIPO",
        "DESCRIPCION",
        "AUTORES",
        "AUTORES NOMBRES",
        "AUTORES DOCENTES",
        "TOTAL DOCENTES",
    ]

    requested_eval_without_columns = [
        column for column in request.args.getlist("eval_sin_columns") if column in eval_without_columns
    ]
    selected_eval_without_columns = requested_eval_without_columns or [
        "SECUENCIA",
        "ANIO PUBLICACION",
        "COD TIPO",
        "DESCRIPCION",
        "AUTORES",
        "AUTORES NOMBRES",
        "TOTAL AUTORES",
    ]

    eval_with_state = _filter_and_paginate_rows(with_teachers_rows, request.args, selected_eval_with_columns, "eval_con")
    eval_without_state = _filter_and_paginate_rows(without_teachers_rows, request.args, selected_eval_without_columns, "eval_sin")

    show_without_tab = selected_faculty == "ALL" and selected_career == "ALL"

    tab_base_params = {key: request.args.getlist(key) for key in request.args.keys() if request.args.getlist(key)}
    tab_with_params = {key: list(values) for key, values in tab_base_params.items()}
    tab_with_params["tab"] = ["con-docente"]
    tab_without_params = {key: list(values) for key, values in tab_base_params.items()}
    tab_without_params["tab"] = ["sin-docente"]
    tab_with_query = urlencode(tab_with_params, doseq=True)
    tab_without_query = urlencode(tab_without_params, doseq=True)

    export_csv_query = ""
    export_xlsx_query = ""
    if evaluation_year is not None:
        export_base_params: dict[str, list[str]] = {
            "year": [str(evaluation_year)],
            "tab": [active_tab],
            "pub_year": [str(publication_year_filter)],
            "exclude_devueltos": ["1" if exclude_devueltos else "0"],
        }
        if selected_faculty != "ALL":
            export_base_params["faculty"] = [selected_faculty]
        if selected_career != "ALL":
            export_base_params["career"] = [selected_career]

        active_state = eval_without_state if active_tab == "sin-docente" and show_without_tab else eval_with_state
        active_selected_columns = selected_eval_without_columns if active_tab == "sin-docente" and show_without_tab else selected_eval_with_columns
        active_columns_param = "eval_sin_columns" if active_tab == "sin-docente" and show_without_tab else "eval_con_columns"
        for column in active_selected_columns:
            export_base_params.setdefault(active_columns_param, []).append(column)
        for column, filter_value in active_state["filter_values"].items():
            if filter_value:
                export_base_params[active_state["column_param_map"][column]] = [filter_value]

        csv_params = {key: list(values) for key, values in export_base_params.items()}
        csv_params["format"] = ["csv"]
        xlsx_params = {key: list(values) for key, values in export_base_params.items()}
        xlsx_params["format"] = ["xlsx"]
        export_csv_query = urlencode(csv_params, doseq=True)
        export_xlsx_query = urlencode(xlsx_params, doseq=True)

    return render_template(
        "main/publicaciones_evaluacion.html",
        active_tab=active_tab,
        show_without_tab=show_without_tab,
        years=years,
        evaluation_year=evaluation_year,
        selected_faculty=selected_faculty,
        selected_career=selected_career,
        publication_year_filter=publication_year_filter,
        publication_years_available=publication_years_available,
        selected_period_label=selected_period_label,
        exclude_devueltos=exclude_devueltos,
        faculty_options=faculty_options,
        career_options=career_options,
        source_years=source_years,
        with_rows=eval_with_state["rows"],
        without_rows=eval_without_state["rows"],
        eval_with_columns=eval_with_columns,
        eval_without_columns=eval_without_columns,
        selected_eval_with_columns=selected_eval_with_columns,
        selected_eval_without_columns=selected_eval_without_columns,
        eval_with_state=eval_with_state,
        eval_without_state=eval_without_state,
        tab_with_query=tab_with_query,
        tab_without_query=tab_without_query,
        export_csv_query=export_csv_query,
        export_xlsx_query=export_xlsx_query,
        summary=summary,
    )


@main_bp.route("/evaluacion/publicaciones/export")
@login_required
def publicaciones_evaluacion_export():
    active_tab = request.args.get("tab", type=str, default="con-docente").strip().lower()
    if active_tab not in {"con-docente", "sin-docente"}:
        active_tab = "con-docente"

    evaluation_year = request.args.get("year", type=int)
    selected_faculty = request.args.get("faculty", type=str, default="ALL")
    selected_career = request.args.get("career", type=str, default="ALL")
    publication_year_filter = request.args.get("pub_year", type=str, default="ALL")
    exclude_devueltos_values = request.args.getlist("exclude_devueltos")
    if exclude_devueltos_values:
        exclude_devueltos = "1" in exclude_devueltos_values
    else:
        exclude_devueltos = True
    export_format = request.args.get("format", type=str, default="csv").lower()

    if not evaluation_year:
        flash("Debes seleccionar un año para exportar.", "error")
        return redirect(url_for("main.publicaciones_evaluacion"))

    source_years = []
    with_teachers_rows = []
    without_teachers_rows = []
    publication_years_available = []

    default_source_years = [evaluation_year - 1, evaluation_year - 2, evaluation_year - 3]
    all_publication_years = [
        row[0]
        for row in db.session.query(Publication.publication_year)
        .filter(Publication.publication_year.isnot(None))
        .distinct()
        .order_by(Publication.publication_year.desc())
        .all()
    ]
    publication_years_available = sorted([int(year) for year in all_publication_years if year], reverse=True)

    if publication_year_filter != "ALL":
        try:
            pub_year_int = int(publication_year_filter)
            if pub_year_int in publication_years_available:
                source_years = [pub_year_int]
            else:
                source_years = default_source_years
                publication_year_filter = "ALL"
        except (ValueError, TypeError):
            source_years = default_source_years
            publication_year_filter = "ALL"
    else:
        source_years = default_source_years

    if selected_faculty != "ALL" or selected_career != "ALL":
        active_tab = "con-docente"

    teachers_query = Teacher.query.filter(Teacher.year == evaluation_year)
    if selected_faculty != "ALL":
        teachers_query = teachers_query.filter(Teacher.faculty == selected_faculty)
    if selected_career != "ALL":
        teachers_query = teachers_query.filter(Teacher.career == selected_career)

    teacher_rows = teachers_query.all()
    teacher_ids = {str(teacher.teacher_id or "").strip() for teacher in teacher_rows if teacher.teacher_id}
    teacher_name_by_id = {
        str(teacher.teacher_id or "").strip(): str(teacher.teacher_name or "").strip()
        for teacher in teacher_rows
        if teacher.teacher_id
    }

    publications = (
        Publication.query.filter(Publication.publication_year.in_(source_years))
        .order_by(Publication.created_at.desc(), Publication.id.desc())
        .all()
    )

    publication_ids = [publication.id for publication in publications]
    author_rows = []
    if publication_ids:
        author_rows = (
            PublicationAuthor.query.filter(PublicationAuthor.publication_id.in_(publication_ids))
            .order_by(PublicationAuthor.id.asc())
            .all()
        )

    authors_by_publication: dict[int, list[PublicationAuthor]] = defaultdict(list)
    for publication_author in author_rows:
        authors_by_publication[publication_author.publication_id].append(publication_author)

    grouped_by_sequence: dict[str, dict] = {}
    for publication in publications:
        publication_year_value = int(publication.publication_year or 0)
        if publication_year_value not in source_years:
            continue
        sequence = str(publication.publication_sequence or "").strip()
        if sequence:
            group_key = f"{publication_year_value}::{sequence}"
        else:
            group_key = f"{publication_year_value}::__NOSEQ__{publication.id}"
        grouped = grouped_by_sequence.setdefault(
            group_key,
            {
                "publication_year": publication_year_value,
                "publication": publication,
                "authors": [],
            },
        )
        grouped["authors"].extend(authors_by_publication.get(publication.id, []))

    with_available_columns = []
    without_available_columns = []
    with_column_seen = set()
    without_column_seen = set()
    excluded_grouped_columns = {
        "PARTICIPACION",
        "COD UNIDAD",
        "COD SUBUNIDAD",
        "COD PAR EMPLEAD",
        "TIPO DOCUMENTO",
        "NUMERO IDENTIFICACION",
        "PRIMER NOMBRE",
        "SEGUNDO NOMBRE",
        "APELLIDO PAT",
        "APELLIDO MAT",
        "FACULTAD",
        "TIPO EMPLEADO",
    }

    for grouped in grouped_by_sequence.values():
        publication = grouped["publication"]
        publication_year_value = grouped["publication_year"]
        publication_authors = grouped["authors"]
        first_author = publication_authors[0] if publication_authors else None
        base_payload = _publication_row_payload(publication, first_author) if first_author else {
            "SECUENCIA": publication.publication_sequence or "",
            "COD TIPO": publication.publication_type or "",
            "ANIO PUBLICACION": str(publication.publication_year or ""),
            "DESCRIPCION": publication.title or "",
            "BASE": publication.source_base or "",
            "INDICE Q": publication.quartile or "",
            "NOMBRE REVISTA": publication.journal_name or "",
        }

        if exclude_devueltos:
            has_devueltos = False
            for author in publication_authors:
                row_json = author.source_row_json or {}
                finalizar_value = str(row_json.get("FINALIZAR", "") or "").strip().upper()
                if finalizar_value == "DEVUELTOS":
                    has_devueltos = True
                    break
            if has_devueltos:
                continue

        ordered_author_ids = []
        ordered_author_names = []
        seen_author_ids = set()
        for author in publication_authors:
            num_id, full_name = _extract_author_identity(author)
            if not num_id or num_id in seen_author_ids:
                continue
            seen_author_ids.add(num_id)
            ordered_author_ids.append(num_id)
            ordered_author_names.append(full_name)

        matched_teacher_ids = [num_id for num_id in ordered_author_ids if num_id in teacher_ids]
        matched_teacher_labels = []
        for teacher_id in matched_teacher_ids:
            teacher_name = teacher_name_by_id.get(teacher_id, "")
            if teacher_name:
                matched_teacher_labels.append(f"{teacher_id} - {teacher_name}")
            else:
                matched_teacher_labels.append(teacher_id)

        base_payload["AUTORES"] = "; ".join(ordered_author_ids)
        base_payload["AUTORES NOMBRES"] = "; ".join(ordered_author_names)
        base_payload["AUTORES DOCENTES"] = "; ".join(matched_teacher_labels)
        base_payload["TOTAL AUTORES"] = str(len(ordered_author_ids))
        base_payload["TOTAL DOCENTES"] = str(len(matched_teacher_ids))

        for excluded_column in excluded_grouped_columns:
            base_payload.pop(excluded_column, None)

        row_payload = {str(key): "" if value is None else str(value) for key, value in base_payload.items()}

        if matched_teacher_ids:
            with_teachers_rows.append(row_payload)
            for key in row_payload.keys():
                if key not in with_column_seen:
                    with_column_seen.add(key)
                    with_available_columns.append(key)
        else:
            without_teachers_rows.append(row_payload)
            for key in row_payload.keys():
                if key not in without_column_seen:
                    without_column_seen.add(key)
                    without_available_columns.append(key)

    eval_with_default_columns = [
        "SECUENCIA",
        "ANIO PUBLICACION",
        "COD TIPO",
        "DESCRIPCION",
        "AUTORES",
        "AUTORES NOMBRES",
        "AUTORES DOCENTES",
        "TOTAL DOCENTES",
        "BASE",
        "INDICE Q",
        "NOMBRE REVISTA",
    ]
    eval_without_default_columns = [
        "SECUENCIA",
        "ANIO PUBLICACION",
        "COD TIPO",
        "DESCRIPCION",
        "AUTORES",
        "AUTORES NOMBRES",
        "TOTAL AUTORES",
        "BASE",
        "INDICE Q",
        "NOMBRE REVISTA",
    ]

    with_available_set = set(with_available_columns)
    eval_with_columns = [column for column in eval_with_default_columns if column in with_available_set] + [
        column for column in with_available_columns if column not in eval_with_default_columns
    ]
    if not eval_with_columns:
        eval_with_columns = list(eval_with_default_columns)

    without_available_set = set(without_available_columns)
    eval_without_columns = [column for column in eval_without_default_columns if column in without_available_set] + [
        column for column in without_available_columns if column not in eval_without_default_columns
    ]
    if not eval_without_columns:
        eval_without_columns = list(eval_without_default_columns)

    requested_eval_with_columns = [
        column for column in request.args.getlist("eval_con_columns") if column in eval_with_columns
    ]
    selected_eval_with_columns = requested_eval_with_columns or [
        "SECUENCIA",
        "ANIO PUBLICACION",
        "COD TIPO",
        "DESCRIPCION",
        "AUTORES",
        "AUTORES NOMBRES",
        "AUTORES DOCENTES",
        "TOTAL DOCENTES",
    ]
    requested_eval_without_columns = [
        column for column in request.args.getlist("eval_sin_columns") if column in eval_without_columns
    ]
    selected_eval_without_columns = requested_eval_without_columns or [
        "SECUENCIA",
        "ANIO PUBLICACION",
        "COD TIPO",
        "DESCRIPCION",
        "AUTORES",
        "AUTORES NOMBRES",
        "TOTAL AUTORES",
    ]

    eval_with_state = _filter_and_paginate_rows(with_teachers_rows, request.args, selected_eval_with_columns, "eval_con")
    eval_without_state = _filter_and_paginate_rows(without_teachers_rows, request.args, selected_eval_without_columns, "eval_sin")

    export_columns = selected_eval_without_columns if active_tab == "sin-docente" else selected_eval_with_columns
    export_rows = eval_without_state["filtered_rows"] if active_tab == "sin-docente" else eval_with_state["filtered_rows"]

    if export_format == "xlsx":
        df = pd.DataFrame(export_rows, columns=export_columns)
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Publicaciones")
        output.seek(0)
        return Response(
            output.getvalue(),
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename=publicaciones_evaluacion_{active_tab}_{evaluation_year}.xlsx"},
        )

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(export_columns)
    for row in export_rows:
        writer.writerow([row.get(column, "") for column in export_columns])

    return Response(
        "\ufeff" + output.getvalue(),
        mimetype="text/csv; charset=utf-8",
        headers={"Content-Disposition": f"attachment; filename=publicaciones_evaluacion_{active_tab}_{evaluation_year}.csv"},
    )


@main_bp.route("/configuraciones/tipos-publicacion", methods=["GET", "POST", "DELETE"])
@login_required
@csrf.exempt
def config_publication_types():
    """Manage publication type labels equivalences."""
    if request.method == "DELETE":
        try:
            data = request.get_json() or {}
            type_code = (data.get("type_code") or "").strip().upper()
            if not type_code:
                return jsonify({"error": "Código es requerido"}), 400

            # Mark code as excluded so it disappears from list even if it exists in publications
            excluded = PublicationTypeExcluded.query.filter_by(type_code=type_code).first()
            if not excluded:
                db.session.add(PublicationTypeExcluded(type_code=type_code))

            # Remove label mapping if present
            pt = PublicationTypeLabel.query.filter_by(type_code=type_code).first()
            if pt:
                db.session.delete(pt)

            db.session.commit()
            return jsonify({"success": True})
        except Exception as e:
            db.session.rollback()
            return jsonify({"error": str(e)}), 500
    
    if request.method == "POST":
        try:
            data = request.get_json() or {}
            type_code = data.get("type_code", "").strip().upper()
            label = data.get("label", "").strip()

            if not type_code or not label:
                return jsonify({"error": "Código y etiqueta son requeridos"}), 400

            pt = PublicationTypeLabel.query.filter_by(type_code=type_code).first()
            if pt:
                pt.label = label
                pt.description = data.get("description", "").strip() or None
                pt.updated_at = datetime.utcnow()
            else:
                pt = PublicationTypeLabel(
                    type_code=type_code,
                    label=label,
                    description=data.get("description", "").strip() or None,
                )
                db.session.add(pt)

            # If code was excluded before, remove exclusion when user saves/adds again
            excluded = PublicationTypeExcluded.query.filter_by(type_code=type_code).first()
            if excluded:
                db.session.delete(excluded)

            db.session.commit()
            return jsonify({"success": True, "id": pt.id})
        except Exception as e:
            db.session.rollback()
            return jsonify({"error": str(e)}), 500

    excluded_codes = {
        row.type_code
        for row in PublicationTypeExcluded.query.with_entities(PublicationTypeExcluded.type_code).all()
        if row.type_code
    }

    # Get all distinct publication types from publications table
    all_types_in_db = db.session.query(Publication.publication_type).distinct().order_by(Publication.publication_type).all()
    all_types = sorted(set([t[0] for t in all_types_in_db if t[0] and t[0] not in excluded_codes]))

    # Get existing labels
    labels = {
        pt.type_code: pt
        for pt in PublicationTypeLabel.query.all()
        if pt.type_code not in excluded_codes
    }

    # Include custom codes created manually even if they are not in publications
    all_codes = sorted(set(all_types) | set(labels.keys()))

    # Build list of all types with their labels
    types_list = []
    for type_code in all_codes:
        if type_code in labels:
            pt = labels[type_code]
            types_list.append({"type_code": type_code, "label": pt.label, "description": pt.description or "", "id": pt.id})
        else:
            types_list.append({"type_code": type_code, "label": "", "description": "", "id": None})

    return render_template("main/config_publication_types.html", types_list=types_list)


@main_bp.route("/configuraciones/bases", methods=["GET", "POST", "DELETE"])
@login_required
@csrf.exempt
def config_bases():
    """Manage base labels equivalences."""
    if request.method == "DELETE":
        try:
            data = request.get_json() or {}
            base_code = (data.get("base_code") or "").strip()
            if not base_code:
                return jsonify({"error": "Código es requerido"}), 400

            # Mark code as excluded so it disappears from list even if it exists in publications
            excluded = BaseExcluded.query.filter_by(base_code=base_code).first()
            if not excluded:
                db.session.add(BaseExcluded(base_code=base_code))

            # Remove label mapping if present
            base = BaseLabel.query.filter_by(base_code=base_code).first()
            if base:
                db.session.delete(base)

            db.session.commit()
            return jsonify({"success": True})
        except Exception as e:
            db.session.rollback()
            return jsonify({"error": str(e)}), 500
    
    if request.method == "POST":
        try:
            data = request.get_json() or {}
            base_code = data.get("base_code", "").strip()
            label = data.get("label", "").strip()

            if not base_code or not label:
                return jsonify({"error": "Código y etiqueta son requeridos"}), 400

            base = BaseLabel.query.filter_by(base_code=base_code).first()
            if base:
                base.label = label
                base.description = data.get("description", "").strip() or None
                base.updated_at = datetime.utcnow()
            else:
                base = BaseLabel(
                    base_code=base_code,
                    label=label,
                    description=data.get("description", "").strip() or None,
                )
                db.session.add(base)

            # If code was excluded before, remove exclusion when user saves/adds again
            excluded = BaseExcluded.query.filter_by(base_code=base_code).first()
            if excluded:
                db.session.delete(excluded)

            db.session.commit()
            return jsonify({"success": True, "id": base.id})
        except Exception as e:
            db.session.rollback()
            return jsonify({"error": str(e)}), 500

    excluded_codes = {
        row.base_code
        for row in BaseExcluded.query.with_entities(BaseExcluded.base_code).all()
        if row.base_code
    }

    # Get all distinct bases from publications table
    all_bases_in_db = db.session.query(Publication.source_base).distinct().order_by(Publication.source_base).all()
    all_bases = sorted(set([b[0] for b in all_bases_in_db if b[0] and b[0] not in excluded_codes]))

    # Get existing labels
    labels = {
        base.base_code: base
        for base in BaseLabel.query.all()
        if base.base_code not in excluded_codes
    }

    # Include custom codes created manually even if they are not in publications
    all_codes = sorted(set(all_bases) | set(labels.keys()))

    # Build list of all bases with their labels
    bases_list = []
    for base_code in all_codes:
        if base_code in labels:
            base = labels[base_code]
            bases_list.append({"base_code": base_code, "label": base.label, "description": base.description or "", "id": base.id})
        else:
            bases_list.append({"base_code": base_code, "label": "", "description": "", "id": None})

    return render_template("main/config_bases.html", bases_list=bases_list)


@main_bp.route("/evaluacion/matrices")
@login_required
def matrices():
    """Pantalla principal de matrices de publicaciones por docente/año."""
    years_available = sorted([
        int(row[0])
        for row in db.session.query(Publication.publication_year)
        .filter(Publication.publication_year.isnot(None))
        .distinct()
        .all()
        if row[0]
    ], reverse=True)
    
    selected_year = request.args.get("year", type=int)
    if selected_year is None and years_available:
        selected_year = years_available[0]

    matrix_types = [
        ("articulos", "Articulos"),
        ("libros", "Libros"),
        ("capitulos", "Capitulos de libros"),
        ("eventos", "Eventos"),
    ]
    allowed_matrix_types = {code for code, _ in matrix_types}
    selected_matrix_type = request.args.get("matrix_type", type=str, default="articulos").strip().lower()
    if selected_matrix_type not in allowed_matrix_types:
        selected_matrix_type = "articulos"
    
    return render_template(
        "main/matrices.html",
        years_available=years_available,
        selected_year=selected_year,
        matrix_types=matrix_types,
        selected_matrix_type=selected_matrix_type,
    )


def _build_articles_matrix(year: int) -> list[dict]:
    """
    Construye la matriz de artículos por docente/año.
    Campos: CODIGO_IES, TIPO_PUBLICACION, TIPO_ARTICULO, CODIGO_PUBLICA_CION,
            TITULO_PUBLICACION, BASE_DATOS_INDEXADA, CODIGO_ISSN, NOMBRE_REVISTA,
            FECHA_PUBLICACION, CAMPO_DETALLADO, ESTADO, LINK_PUBLICACION,
            LINK_REVISTA, FILIACION, IDENTIFICACION_PARTICIPANTE, PARTICIPACION,
            CUARTIL, LINEA_INVESTIGACION, INTERCULTURAL
    """
    publications = (
        Publication.query.filter(
            Publication.publication_year == year,
            Publication.publication_type == "A"
        )
        .order_by(Publication.created_at.desc(), Publication.id.desc())
        .all()
    )
    
    publication_ids = [pub.id for pub in publications]
    if not publication_ids:
        return []
    
    authors = (
        PublicationAuthor.query.filter(PublicationAuthor.publication_id.in_(publication_ids))
        .order_by(PublicationAuthor.publication_id.asc(), PublicationAuthor.id.asc())
        .all()
    )
    
    authors_by_pub = defaultdict(list)
    for author in authors:
        authors_by_pub[author.publication_id].append(author)
    
    matrix_rows = []
    
    for publication in publications:
        pub_authors = authors_by_pub.get(publication.id, [])
        
        # Un registro por cada autoría
        for author in pub_authors:
            row_json = author.source_row_json or {}
            
            # Descartar si FINALIZAR="DEVUELTOS"
            finalizar = str(row_json.get("FINALIZAR", "")).strip().upper()
            if finalizar == "DEVUELTOS":
                continue
            
            # Extraer identificación del participante
            num_id = (
                row_json.get("NUMERO IDENTIFICACION") or
                row_json.get("NUMERO_IDENTIFICACION") or
                author.teacher_id or
                ""
            )
            
            # DOI o SECUENCIA como código de publicación
            codigo_pub = (
                row_json.get("DOI") or 
                row_json.get("doi") or 
                publication.publication_sequence or 
                ""
            )
            
            # Mapear campos del JSON a la estructura de la matriz
            matrix_rows.append({
                "CODIGO_IES": "1028",  # UCSG
                "TIPO_PUBLICACION": publication.publication_type or "A",
                "TIPO_ARTICULO": row_json.get("TIPO ARTICULO") or row_json.get("TIPO_ARTICULO") or "REVISTA",
                "CODIGO_PUBLICA_CION": codigo_pub,
                "TITULO_PUBLICACION": publication.title or "",
                "BASE_DATOS_INDEXADA": publication.source_base or row_json.get("BASE DATOS INDEXADA") or "",
                "CODIGO_ISSN": row_json.get("CODIGO ISSN") or row_json.get("CODIGO_ISSN") or "",
                "NOMBRE_REVISTA": publication.journal_name or row_json.get("NOMBRE REVISTA") or "",
                "FECHA_PUBLICACION": row_json.get("FECHA PUBLICACION") or row_json.get("FECHA_PUBLICACION") or "",
                "CAMPO_DETALLADO": row_json.get("CAMPO DETALLADO") or row_json.get("CAMPO_DETALLADO") or "",
                "ESTADO": row_json.get("ESTADO") or "ACTIVO",
                "LINK_PUBLICACION": row_json.get("LINK PUBLICACION") or row_json.get("LINK_PUBLICACION") or "",
                "LINK_REVISTA": row_json.get("LINK REVISTA") or row_json.get("LINK_REVISTA") or "",
                "FILIACION": row_json.get("FILIACION") or row_json.get("FILIACION") or "",
                "IDENTIFICACION_PARTICIPANTE": num_id,
                "PARTICIPACION": row_json.get("PARTICIPACION") or row_json.get("PARTICIPACION") or "1",
                "CUARTIL": publication.quartile or row_json.get("INDICE Q") or row_json.get("CUARTIL") or "",
                "LINEA_INVESTIGACION": row_json.get("LINEA INVESTIGACION") or row_json.get("LINEA_INVESTIGACION") or "",
                "INTERCULTURAL": row_json.get("INTERCULTURAL") or "",
            })
    
    return matrix_rows


@main_bp.route("/evaluacion/matrices/export/articulos")
@login_required
def matrices_export_articulos():
    """Exporta la matriz de artículos en CSV o XLSX."""
    year = request.args.get("year", type=int)
    export_format = request.args.get("format", type=str, default="csv").lower()
    matrix_type = request.args.get("matrix_type", type=str, default="articulos").strip().lower()
    if not year:
        flash("Debes seleccionar un año para exportar.", "error")
        return redirect(url_for("main.matrices"))

    if matrix_type in {"libros", "capitulos", "eventos"}:
        flash("Funcion en desarrollo para el tipo de matriz seleccionado.", "error")
        return redirect(url_for("main.matrices", year=year, matrix_type=matrix_type))

    if matrix_type != "articulos":
        matrix_type = "articulos"
    
    matrix_rows = _build_articles_matrix(year)
    
    if not matrix_rows:
        flash(f"No hay artículos disponibles para el año {year}.", "error")
        return redirect(url_for("main.matrices", year=year))
    
    # Orden de columnas según el esquema
    columns = [
        "CODIGO_IES",
        "TIPO_PUBLICACION",
        "TIPO_ARTICULO",
        "CODIGO_PUBLICA_CION",
        "TITULO_PUBLICACION",
        "BASE_DATOS_INDEXADA",
        "CODIGO_ISSN",
        "NOMBRE_REVISTA",
        "FECHA_PUBLICACION",
        "CAMPO_DETALLADO",
        "ESTADO",
        "LINK_PUBLICACION",
        "LINK_REVISTA",
        "FILIACION",
        "IDENTIFICACION_PARTICIPANTE",
        "PARTICIPACION",
        "CUARTIL",
        "LINEA_INVESTIGACION",
        "INTERCULTURAL",
    ]
    
    file_name = f"matriz_articulos_{year}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    if export_format == "xlsx":
        df = pd.DataFrame(matrix_rows, columns=columns)
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Artículos")
        output.seek(0)
        return Response(
            output.getvalue(),
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={file_name}.xlsx"},
        )
    
    # CSV (default)
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(columns)
    
    for row in matrix_rows:
        writer.writerow([row.get(col, "") for col in columns])
    
    return Response(
        "\ufeff" + output.getvalue(),
        mimetype="text/csv; charset=utf-8",
        headers={"Content-Disposition": f"attachment; filename={file_name}.csv"},
    )
