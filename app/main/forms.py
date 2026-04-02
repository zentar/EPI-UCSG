from flask_wtf import FlaskForm
from flask_wtf.file import FileAllowed, FileRequired
from wtforms import FileField, HiddenField, IntegerField, SubmitField
from wtforms.validators import DataRequired, NumberRange


class TeacherUploadForm(FlaskForm):
    year = IntegerField(
        "Año de corte",
        validators=[DataRequired(), NumberRange(min=2000, max=2100)],
    )
    source_file = FileField(
        "Archivo docentes (CSV o XLSX)",
        validators=[
            FileRequired(),
            FileAllowed(["csv", "xlsx"], "Solo se permiten archivos CSV o XLSX."),
        ],
    )
    merge_confirmed = HiddenField(default="0")
    submit = SubmitField("Cargar docentes")


class PublicationUploadForm(FlaskForm):
    source_file = FileField(
        "Archivo publicaciones (CSV o XLSX)",
        validators=[
            FileRequired(),
            FileAllowed(["csv", "xlsx"], "Solo se permiten archivos CSV o XLSX."),
        ],
    )
    merge_confirmed = HiddenField(default="0")
    submit = SubmitField("Cargar publicaciones")
