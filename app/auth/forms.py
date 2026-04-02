from flask_wtf import FlaskForm
from wtforms import PasswordField, StringField, SubmitField
from wtforms.validators import DataRequired, Email, Length


class LoginForm(FlaskForm):
    email = StringField("Correo electrónico", validators=[DataRequired(), Email(), Length(max=190)])
    password = PasswordField("Contraseña", validators=[DataRequired(), Length(min=6)])
    submit = SubmitField("Entrar")


class SetupAdminForm(FlaskForm):
    name = StringField("Nombre", validators=[DataRequired(), Length(max=120)])
    email = StringField("Correo electrónico", validators=[DataRequired(), Email(), Length(max=190)])
    password = PasswordField("Contraseña", validators=[DataRequired(), Length(min=8)])
    submit = SubmitField("Crear usuario inicial")
