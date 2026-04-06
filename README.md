# EPI-UCSG

Aplicación independiente para gestión EPI UCSG.

## Estado actual

Primera implementación base completada (Fase A+B):

- App web en Flask con autenticación.
- Base de datos MySQL 8 para persistencia.
- Modelos iniciales para usuarios, lotes de carga, docentes, publicaciones y auditoría.
- Seguridad base: CSRF, sesión segura y rate limiting.
- Dashboard inicial con módulos: Docentes, Publicaciones y Cruce 3 años.

## Stack tecnológico

- Flask
- MySQL 8 (InnoDB, utf8mb4)
- SQLAlchemy + Flask-Migrate
- Flask-Login, Flask-WTF, Flask-Limiter
- Docker + Docker Compose

## Ejecutar con Docker (usando MySQL existente)

Esta app usa el servidor MySQL ya existente del entorno editorial (`db-editorial`) en la red Docker externa `red_editorial`.

Antes del primer arranque, crea base y usuario para EPI-UCSG en ese servidor:

```sql
CREATE DATABASE IF NOT EXISTS epi_ucsg CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER IF NOT EXISTS 'epi_user'@'%' IDENTIFIED BY 'epi_password';
GRANT ALL PRIVILEGES ON epi_ucsg.* TO 'epi_user'@'%';
FLUSH PRIVILEGES;
```

1. Copiar variables de entorno:

```bash
cp .env.example .env
```

2. Levantar servicios:

```bash
docker compose up -d --build
```

3. Abrir en navegador:

```text
http://127.0.0.1:8200
```

4. Primer ingreso:

- Si no existe usuario, se abrirá automáticamente la pantalla de creación del usuario inicial.

## Variables importantes

- `DATABASE_URL` debe apuntar a `db-editorial:3306`.
- `APP_BASE_PATH=/epi` para publicar detrás de Nginx Proxy Manager en subruta.
- `MYSQL_DATABASE`, `MYSQL_USER`, `MYSQL_PASSWORD` se usan como referencia para la administración y deben coincidir con lo creado en el servidor MySQL.

## Ejecutar localmente (sin Docker)

Requisitos:

- Python 3.12+
- MySQL 8 en ejecución

Pasos:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
python run.py
```

## Migraciones de base de datos

El proyecto usa Flask-Migrate (Alembic) para versionar cambios de esquema.

Comandos habituales con Docker:

```bash
# Ver revisión actual
docker compose exec web flask --app run.py db current

# Crear una nueva migración desde cambios en modelos
docker compose exec web flask --app run.py db migrate -m "descripcion del cambio"

# Aplicar migraciones pendientes
docker compose exec web flask --app run.py db upgrade

# Ver historial de revisiones
docker compose exec web flask --app run.py db history
```

Comandos equivalentes sin Docker:

```bash
flask --app run.py db current
flask --app run.py db migrate -m "descripcion del cambio"
flask --app run.py db upgrade
flask --app run.py db history
```

## Estructura inicial

```text
EPI-UCSG/
├── app/
│   ├── __init__.py
│   ├── extensions.py
│   ├── models.py
│   ├── auth/
│   │   ├── forms.py
│   │   └── routes.py
│   ├── main/
│   │   └── routes.py
│   └── templates/
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── run.py
```

## Próximo paso de implementación

- Carga de Docentes por año (CSV/XLSX) con validación de campos obligatorios y persistencia consultable.
