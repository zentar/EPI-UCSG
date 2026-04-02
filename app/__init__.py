import os
from datetime import timedelta

from flask import Flask
from sqlalchemy import inspect, text

from app.extensions import csrf, db, limiter, login_manager, migrate


def ensure_runtime_schema() -> None:
    inspector = inspect(db.engine)

    if "publication_authors" in inspector.get_table_names():
        publication_author_columns = {column["name"] for column in inspector.get_columns("publication_authors")}
        if "source_row_json" not in publication_author_columns:
            with db.engine.begin() as connection:
                connection.execute(text("ALTER TABLE publication_authors ADD COLUMN source_row_json JSON NULL"))

    if "publications" in inspector.get_table_names():
        with db.engine.begin() as connection:
            # Ensure enough room for quartile strings coming from institutional exports.
            connection.execute(text("ALTER TABLE publications MODIFY COLUMN quartile VARCHAR(32) NULL"))


def create_app() -> Flask:
    app = Flask(__name__)

    app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "change-this-secret")
    app.config["SQLALCHEMY_DATABASE_URI"] = os.getenv(
        "DATABASE_URL",
        "mysql+pymysql://epi_user:epi_password@localhost:3306/epi_ucsg",
    )
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    app.config["SESSION_COOKIE_SECURE"] = os.getenv("SESSION_COOKIE_SECURE", "false").lower() == "true"
    app.config["SESSION_COOKIE_HTTPONLY"] = True
    app.config["SESSION_COOKIE_SAMESITE"] = os.getenv("SESSION_COOKIE_SAMESITE", "Lax")
    app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(
        minutes=int(os.getenv("SESSION_LIFETIME_MINUTES", "120"))
    )

    db.init_app(app)
    migrate.init_app(app, db)
    csrf.init_app(app)
    login_manager.init_app(app)
    limiter.init_app(app)

    login_manager.login_view = "auth.login"
    login_manager.login_message = "Inicia sesión para continuar."

    from app.auth.routes import auth_bp
    from app.main.routes import main_bp

    app.register_blueprint(auth_bp)
    app.register_blueprint(main_bp)

    with app.app_context():
        from app import models  # noqa: F401

        db.create_all()
        ensure_runtime_schema()

    return app
