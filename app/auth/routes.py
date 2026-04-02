from flask import Blueprint, flash, redirect, render_template, url_for
from flask_login import current_user, login_user, logout_user

from app.auth.forms import LoginForm, SetupAdminForm
from app.extensions import db, limiter
from app.models import User


auth_bp = Blueprint("auth", __name__, url_prefix="/auth")


@auth_bp.route("/setup", methods=["GET", "POST"])
@limiter.limit("5/minute")
def setup_admin():
    if User.query.count() > 0:
        return redirect(url_for("auth.login"))

    form = SetupAdminForm()
    if form.validate_on_submit():
        admin = User(
            name=form.name.data.strip(),
            email=form.email.data.strip().lower(),
        )
        admin.set_password(form.password.data)
        db.session.add(admin)
        db.session.commit()
        flash("Usuario inicial creado. Ya puedes iniciar sesión.", "success")
        return redirect(url_for("auth.login"))

    return render_template("auth/setup.html", form=form)


@auth_bp.route("/login", methods=["GET", "POST"])
@limiter.limit("10/minute")
def login():
    if User.query.count() == 0:
        return redirect(url_for("auth.setup_admin"))

    if current_user.is_authenticated:
        return redirect(url_for("main.dashboard"))

    form = LoginForm()
    if form.validate_on_submit():
        user = User.query.filter_by(email=form.email.data.strip().lower()).first()

        if not user or not user.check_password(form.password.data):
            flash("Credenciales inválidas.", "error")
            return render_template("auth/login.html", form=form)

        login_user(user)
        flash("Sesión iniciada correctamente.", "success")
        return redirect(url_for("main.dashboard"))

    return render_template("auth/login.html", form=form)


@auth_bp.route("/logout")
def logout():
    logout_user()
    flash("Sesión cerrada.", "success")
    return redirect(url_for("auth.login"))
