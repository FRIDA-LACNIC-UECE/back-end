import os

from sqlalchemy_utils import create_database, database_exists, drop_database
from werkzeug.exceptions import HTTPException

from app import api
from app.main import create_app, db
from app.main.model import User
from app.main.seeders.create_seed import create_seed
from app.main.service import (
    create_default_anonymization_type,
    create_default_valid_database,
)

env_name = os.environ.get("ENV_NAME", "dev")

app = create_app(env_name)
api.init_app(app)


# Global Exception Error
@app.errorhandler(Exception)
def handle_exception(error):
    print(error)
    if isinstance(error, HTTPException):
        return {
            "error": {"message": str(error)},
        }, error.code
    return {
        "error": {
            "message": str(error),
        }
    }, 200


app.app_context().push()


@app.cli.command("create_db")
def create_db():
    existing_tables = db.engine.table_names()

    if not set(db.Model.metadata.tables.keys()) <= set(existing_tables):
        db.drop_all()
        db.create_all()
        db.session.commit()

        create_default_valid_database()
        create_default_anonymization_type()

        if env_name in ["dev", "staging"]:
            create_seed(env_name=env_name)


@app.cli.command("reset_db")
def create_db():
    if database_exists(url=db.engine.url):
        drop_database(url=db.engine.url)

    create_database(url=db.engine.url)

    db.drop_all()
    db.create_all()
    db.session.commit()

    create_default_valid_database()
    create_default_anonymization_type()

    if env_name in ["dev", "staging"]:
        create_seed(env_name=env_name)


@app.cli.command("seeder_db")
def seeder_db():
    if env_name in ["dev", "staging"]:
        user = User.query.filter_by(email="admin@example.com").first()
        if not user:
            create_default_valid_database()
            create_default_anonymization_type()
            create_seed(env_name=env_name)


if __name__ == "__main__":
    app.run(host="0.0.0.0")
