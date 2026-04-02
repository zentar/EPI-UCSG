import csv
import io
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from urllib.parse import urlencode

import pandas as pd
from flask import Blueprint, Response, current_app, flash, redirect, render_template, request, send_file, url_for
from flask_login import current_user, login_required
from werkzeug.utils import secure_filename

from app.extensions import db, limiter
from app.main.forms import PublicationUploadForm, TeacherUploadForm
from app.models import ImportBatch, Publication, PublicationAuthor, Teacher
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
            "visible_filter_values": visible_filter_values,
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


def _collect_grouped_publication_view_state(selected_year: int | None, request_args) -> dict:
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

        base_payload["AUTORES"] = "; ".join(ordered_author_ids)
        base_payload["AUTORES NOMBRES"] = "; ".join(ordered_author_names)
        base_payload["TOTAL AUTORES"] = str(len(ordered_author_ids))

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

        filtered_total = filtered_query.count()
        teachers = filtered_query.order_by(Teacher.teacher_name.asc()).limit(300).all()

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
    template_path = Path("app/static/templates/docentes_template.csv")
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

    if selected_year is not None:
        view_state = _collect_publication_view_state(selected_year, request.args)
        available_columns = view_state["available_columns"]
        selected_columns = view_state["selected_columns"]
        visible_filter_values = view_state["visible_filter_values"]
        column_param_map = view_state["column_param_map"]
        search_query = view_state["search_query"]
        export_csv_query = view_state["export_csv_query"]
        export_xlsx_query = view_state["export_xlsx_query"]
        filtered_total = len(view_state["filtered_rows"])
        publication_rows = view_state["filtered_rows"][:300]

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
    available_columns = []
    selected_columns = []
    visible_filter_values = {}
    column_param_map = {}
    search_query = ""
    export_csv_query = ""
    export_xlsx_query = ""

    if selected_year is not None:
        view_state = _collect_grouped_publication_view_state(selected_year, request.args)
        available_columns = view_state["available_columns"]
        selected_columns = view_state["selected_columns"]
        visible_filter_values = view_state["visible_filter_values"]
        column_param_map = view_state["column_param_map"]
        search_query = view_state["search_query"]
        export_csv_query = view_state["export_csv_query"]
        export_xlsx_query = view_state["export_xlsx_query"]
        filtered_total = len(view_state["filtered_rows"])
        publication_rows = view_state["filtered_rows"][:300]

    return render_template(
        "main/publicaciones_consulta.html",
        years=years,
        selected_year=selected_year,
        publication_rows=publication_rows,
        filtered_total=filtered_total,
        available_columns=available_columns,
        selected_columns=selected_columns,
        visible_filter_values=visible_filter_values,
        column_param_map=column_param_map,
        search_query=search_query,
        export_csv_query=export_csv_query,
        export_xlsx_query=export_xlsx_query,
    )


@main_bp.route("/publicaciones/consulta/export")
@login_required
def publicaciones_consulta_export():
    year = request.args.get("year", type=int)
    export_format = request.args.get("format", type=str, default="csv").lower()

    if not year:
        flash("Debes seleccionar un año para exportar.", "error")
        return redirect(url_for("main.publicaciones_consulta"))

    view_state = _collect_grouped_publication_view_state(year, request.args)
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
    template_path = Path("app/static/templates/publicaciones_template.csv")
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
    evaluation_year = request.args.get("year", type=int)

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
    summary = {
        "teachers_total": 0,
        "publications_total_window": 0,
        "publications_with_teachers": 0,
        "teacher_ids_with_publications": 0,
    }

    if evaluation_year is not None:
        source_years = [evaluation_year - 1, evaluation_year - 2, evaluation_year - 3]

        teacher_rows = Teacher.query.filter(Teacher.year == evaluation_year).all()
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

        for grouped in grouped_by_sequence.values():
            publication = grouped["publication"]
            publication_year_value = grouped["publication_year"]
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
            if not matched_teacher_ids:
                continue

            matched_teacher_labels = []
            for teacher_id in matched_teacher_ids:
                teacher_name = teacher_name_by_id.get(teacher_id, "")
                if teacher_name:
                    matched_teacher_labels.append(f"{teacher_id} - {teacher_name}")
                else:
                    matched_teacher_labels.append(teacher_id)

            teacher_ids_with_publications.update(matched_teacher_ids)

            with_teachers_rows.append(
                {
                    "SECUENCIA": publication.publication_sequence or "",
                    "ANIO PUBLICACION": str(publication_year_value or ""),
                    "COD TIPO": publication.publication_type or "",
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
            )

        summary = {
            "teachers_total": len(teacher_ids),
            "publications_total_window": len(grouped_by_sequence),
            "publications_with_teachers": len(with_teachers_rows),
            "teacher_ids_with_publications": len(teacher_ids_with_publications),
        }

    eval_columns = [
        "SECUENCIA",
        "ANIO PUBLICACION",
        "COD TIPO",
        "DESCRIPCION",
        "AUTORES",
        "AUTORES NOMBRES",
        "AUTORES DOCENTES",
        "TOTAL DOCENTES",
    ]
    eval_state = _filter_and_paginate_rows(with_teachers_rows, request.args, eval_columns, "eval")

    return render_template(
        "main/publicaciones_evaluacion.html",
        years=years,
        evaluation_year=evaluation_year,
        source_years=source_years,
        rows=eval_state["rows"],
        eval_state=eval_state,
        summary=summary,
    )
