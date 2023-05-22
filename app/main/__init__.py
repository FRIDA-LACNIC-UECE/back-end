from flask import Flask
from flask.app import Flask
from flask_cors import CORS
from flask_mail import Mail
from flask_migrate import Migrate
from flask_sqlalchemy import SQLAlchemy

from app.main.config import config_by_name

db = SQLAlchemy()
migrate = Migrate(compare_type=True)
app = Flask(__name__)
mail = Mail()
CORS(app)


def create_app(config_name: str) -> Flask:
    app.config.from_object(config_by_name[config_name])
    db.init_app(app)
    migrate.init_app(app, db)
    mail.init_app(app)

    return app
