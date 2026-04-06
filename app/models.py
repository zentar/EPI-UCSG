from datetime import datetime

from flask_login import UserMixin
from werkzeug.security import check_password_hash, generate_password_hash

from app.extensions import db, login_manager


class User(UserMixin, db.Model):
    __tablename__ = "users"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), nullable=False)
    email = db.Column(db.String(190), unique=True, nullable=False, index=True)
    password_hash = db.Column(db.String(255), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

    def set_password(self, raw_password: str) -> None:
        self.password_hash = generate_password_hash(raw_password)

    def check_password(self, raw_password: str) -> bool:
        return check_password_hash(self.password_hash, raw_password)


@login_manager.user_loader
def load_user(user_id: str):
    return User.query.get(int(user_id))


class ImportBatch(db.Model):
    __tablename__ = "import_batches"

    id = db.Column(db.Integer, primary_key=True)
    import_type = db.Column(db.String(30), nullable=False, index=True)  # docentes/publicaciones
    year = db.Column(db.Integer, nullable=False, index=True)
    source_file_name = db.Column(db.String(255), nullable=False)
    source_file_path = db.Column(db.String(255), nullable=False)
    total_rows = db.Column(db.Integer, default=0, nullable=False)
    valid_rows = db.Column(db.Integer, default=0, nullable=False)
    invalid_rows = db.Column(db.Integer, default=0, nullable=False)
    status = db.Column(db.String(30), default="pending", nullable=False)
    created_by = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)


class Teacher(db.Model):
    __tablename__ = "teachers"

    id = db.Column(db.Integer, primary_key=True)
    import_batch_id = db.Column(db.Integer, db.ForeignKey("import_batches.id"), nullable=False)
    year = db.Column(db.Integer, nullable=False, index=True)
    teacher_id = db.Column(db.String(64), nullable=False, index=True)
    teacher_name = db.Column(db.String(255), nullable=False)
    category = db.Column(db.String(120), nullable=False)
    dedication = db.Column(db.String(120), nullable=False)
    faculty = db.Column(db.String(120), nullable=True)
    career = db.Column(db.String(120), nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

    __table_args__ = (
        db.UniqueConstraint("year", "teacher_id", name="uq_teacher_year_teacher_id"),
    )


class Publication(db.Model):
    __tablename__ = "publications"

    id = db.Column(db.Integer, primary_key=True)
    import_batch_id = db.Column(db.Integer, db.ForeignKey("import_batches.id"), nullable=False)
    publication_sequence = db.Column(db.String(64), nullable=False, index=True)
    publication_type = db.Column(db.String(3), nullable=False)
    title = db.Column(db.Text, nullable=False)
    publication_year = db.Column(db.Integer, nullable=False, index=True)
    source_base = db.Column(db.String(32), nullable=True)
    quartile = db.Column(db.String(32), nullable=True)
    journal_name = db.Column(db.String(255), nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

    authors = db.relationship("PublicationAuthor", backref="publication", cascade="all, delete-orphan")


class PublicationAuthor(db.Model):
    __tablename__ = "publication_authors"

    id = db.Column(db.Integer, primary_key=True)
    publication_id = db.Column(db.Integer, db.ForeignKey("publications.id"), nullable=False, index=True)
    teacher_id = db.Column(db.String(64), nullable=False, index=True)
    source_row_json = db.Column(db.JSON, nullable=True)


class ProcessLog(db.Model):
    __tablename__ = "process_logs"

    id = db.Column(db.Integer, primary_key=True)
    process_type = db.Column(db.String(50), nullable=False)
    evaluation_year = db.Column(db.Integer, nullable=True)
    metadata_json = db.Column(db.JSON, nullable=True)
    created_by = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)


class PacArtisticSetting(db.Model):
    __tablename__ = "pac_artistic_settings"

    id = db.Column(db.Integer, primary_key=True)
    evaluation_year = db.Column(db.Integer, nullable=False, index=True)
    faculty_scope = db.Column(db.String(120), nullable=False, default="ALL")
    career_scope = db.Column(db.String(120), nullable=False, default="ALL")
    artistic_value = db.Column(db.Float, nullable=False, default=10.0)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    __table_args__ = (
        db.UniqueConstraint(
            "evaluation_year",
            "faculty_scope",
            "career_scope",
            name="uq_pac_artistic_scope",
        ),
    )


class PublicationTypeLabel(db.Model):
    __tablename__ = "publication_type_labels"

    id = db.Column(db.Integer, primary_key=True)
    type_code = db.Column(db.String(10), unique=True, nullable=False, index=True)
    label = db.Column(db.String(120), nullable=False)
    description = db.Column(db.Text, nullable=True)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class BaseLabel(db.Model):
    __tablename__ = "base_labels"

    id = db.Column(db.Integer, primary_key=True)
    base_code = db.Column(db.String(32), unique=True, nullable=False, index=True)
    label = db.Column(db.String(120), nullable=False)
    description = db.Column(db.Text, nullable=True)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class PublicationTypeExcluded(db.Model):
    __tablename__ = "publication_type_excluded"

    id = db.Column(db.Integer, primary_key=True)
    type_code = db.Column(db.String(10), unique=True, nullable=False, index=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)


class BaseExcluded(db.Model):
    __tablename__ = "base_excluded"

    id = db.Column(db.Integer, primary_key=True)
    base_code = db.Column(db.String(32), unique=True, nullable=False, index=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
