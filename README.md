# EPI-UCSG

Aplicación web para gestión de datos de Evaluación de Producción Investigativa de la UCSG.

El sistema permite cargar archivos de docentes y publicaciones, consultarlos por año, calcular el indicador iP y exportar matrices institucionales en CSV/XLSX para artículos, libros, capítulos de libro y eventos académicos.

## Funcionalidades actuales

- Autenticación y creación de usuario inicial.
- Carga de docentes por año desde CSV/XLSX.
- Carga de publicaciones por año desde CSV/XLSX.
- Consulta de docentes con filtros por año, facultad y carrera.
- Consulta de publicaciones en formato registro y agrupado.
- Configuración de equivalencias para tipos de publicación y bases.
- Evaluación de publicaciones e indicador iP.
- Histórico comparativo del iP.
- Exportación PDF del indicador iP.
- Mantenimiento anual de PA/PIA.
- Exportación de matrices institucionales:
	- Artículos
	- Libros
	- Capítulos de libro
	- Eventos académicos

## Stack tecnológico

- Flask 3
- SQLAlchemy + Flask-Migrate
- Flask-Login
- Flask-WTF
- Flask-Limiter
- MySQL 8
- pandas
- openpyxl
- ReportLab
- Docker + Docker Compose

## Arquitectura general

El proyecto está organizado en un módulo principal de Flask con tres áreas claras:

- `auth`: autenticación y sesión.
- `main`: pantallas operativas, carga, consultas, evaluación y matrices.
- `services`: parsing e ingestión de archivos de docentes y publicaciones.

Persistencia principal:

- `users`: usuarios del sistema.
- `import_batches`: lotes de carga de archivos.
- `teachers`: docentes por año.
- `publications`: publicaciones consolidadas por registro.
- `publication_authors`: una fila por autoría con el JSON original de la carga.
- `process_logs`: trazabilidad de procesos.

## Estructura del proyecto

```text
EPI-UCSG/
├── app/
│   ├── auth/
│   ├── main/
│   ├── services/
│   ├── templates/
│   ├── extensions.py
│   ├── models.py
│   └── __init__.py
├── data/
│   └── uploads/
├── migrations/
├── docker-compose.yml
├── Dockerfile
├── .env.example
├── requirements.txt
├── README.md
└── run.py
```

## Requisitos

- Docker y Docker Compose, o Python 3.10+ con acceso a MySQL 8.
- Red Docker externa `red_editorial` disponible si se ejecuta con Docker.
- Servidor MySQL accesible desde la aplicación.

## Configuración con Docker

La aplicación está preparada para usar el servidor MySQL existente del entorno editorial, normalmente `db-editorial`, dentro de la red Docker externa `red_editorial`.

### 1. Crear base y usuario en MySQL

```sql
CREATE DATABASE IF NOT EXISTS epi_ucsg CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER IF NOT EXISTS 'epi_user'@'%' IDENTIFIED BY 'epi_password';
GRANT ALL PRIVILEGES ON epi_ucsg.* TO 'epi_user'@'%';
FLUSH PRIVILEGES;
```

### 2. Preparar variables de entorno

```bash
cp .env.example .env
```

Variables principales:

- `APP_PORT`: puerto local del contenedor web. Por defecto `8200`.
- `SECRET_KEY`: clave de sesión de Flask.
- `DATABASE_URL`: cadena de conexión SQLAlchemy hacia MySQL.
- `APP_BASE_PATH`: útil si la app se publica en subruta, por ejemplo `/epi`.
- `SESSION_COOKIE_SECURE`: usar `true` detrás de HTTPS real.
- `RATELIMIT_STORAGE_URI`: backend del rate limiting.

### 3. Levantar la aplicación

```bash
docker compose up -d --build
```

### 4. Abrir en navegador

```text
http://127.0.0.1:8200
```

### 5. Primer ingreso

Si todavía no existe ningún usuario, la aplicación redirige automáticamente a la pantalla de creación del usuario inicial.

## Ejecución local sin Docker

Requisitos:

- Python 3.10 o superior.
- MySQL 8 en ejecución.

Pasos:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
python3 run.py
```

## Migraciones de base de datos

El proyecto usa Flask-Migrate/Alembic para versionar el esquema.

Con Docker:

```bash
docker compose exec web flask --app run.py db current
docker compose exec web flask --app run.py db migrate -m "descripcion del cambio"
docker compose exec web flask --app run.py db upgrade
docker compose exec web flask --app run.py db history
```

Sin Docker:

```bash
flask --app run.py db current
flask --app run.py db migrate -m "descripcion del cambio"
flask --app run.py db upgrade
flask --app run.py db history
```

## Uso funcional

### Docentes

- Carga anual desde archivo.
- Validación de campos requeridos.
- Consulta filtrable por año, facultad y carrera.
- Exportación de resultados.

### Publicaciones

- Carga anual desde archivo.
- Persistencia del registro principal y de cada autoría.
- Exclusión lógica de registros con `FINALIZAR = DEVUELTOS` en varios procesos analíticos.
- Consulta en vista detallada y vista agrupada.

### Evaluación iP

- Cálculo del indicador iP.
- Suma de PA y PIA desde mantenimiento anual.
- Histórico comparativo por años.
- Exportación PDF.

### Matrices institucionales

La pantalla de matrices permite seleccionar:

- Año
- Tipo de matriz
- Formato de salida: CSV o XLSX

Tipos soportados actualmente:

- Artículos
- Libros
- Capítulos de libro
- Eventos académicos

Cada exportación genera un registro por autoría y usa como fuente los datos cargados en publicaciones.

## Archivos de carga

Los archivos originales de carga se almacenan bajo `data/uploads/` para uso operativo local.

Consideraciones:

- Esa ruta está ignorada en Git.
- No debe versionarse contenido real de carga.
- La información persistente queda en la base de datos tras la ingestión.

## Operación diaria útil

Levantar servicio:

```bash
docker compose up -d
```

Reiniciar servicio web:

```bash
docker restart epi-ucsg-web
```

Ver logs:

```bash
docker logs -f epi-ucsg-web
```

Ver estado git:

```bash
git status
```

## Notas de mantenimiento

- Los cambios de esquema deben ir con migración.
- Las nuevas matrices deben mapearse desde `source_row_json` en `publication_authors`.
- Para cambios de negocio sobre publicaciones, revisar primero `app/main/routes.py` y `app/services/publication_ingestion.py`.
- Para nuevos formatos de exportación, mantener paridad entre CSV y XLSX.

## Estado actual

El proyecto ya no está en una fase base. Actualmente cubre carga, consulta, evaluación iP, histórico, PDF y matrices institucionales operativas.
