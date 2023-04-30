from flask.app import Flask
from flask_cors import CORS
from flask_migrate import Migrate
from flask_sqlalchemy import SQLAlchemy

from .config import config_by_name

db = SQLAlchemy()
migrate = Migrate()
app = Flask(__name__)
CORS(app)


def create_app(config_name: str) -> Flask:
    app.config.from_object(config_by_name[config_name])
    db.init_app(app)
    migrate.init_app(app, db)

    return app
