import os

from werkzeug.middleware.dispatcher import DispatcherMiddleware

from app import create_app


flask_app = create_app()
base_path = os.getenv("APP_BASE_PATH", "").strip()

if base_path and base_path != "/":
	if not base_path.startswith("/"):
		base_path = f"/{base_path}"
	app = DispatcherMiddleware(flask_app, {base_path: flask_app})
else:
	app = flask_app
