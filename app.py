from flask_migrate import Migrate
from sqlalchemy import create_engine
from sqlalchemy_utils import create_database, database_exists

from api.config import *
from api.controller import *
from api.model import *
from api.seeders.create_seeder import _create_seeder

Migrate(app, db)


@app.shell_context_processor
def make_shell_context():
    return dict(app=app, db=db, User=User)


@app.cli.command("create_db")
def create_db():
    # Define public database path
    src_public_db_path = "{}://{}:{}@{}:{}/{}".format(
        TYPE_DATABASE, USER_DATABASE, PASSWORD_DATABASE, HOST, PORT, NAME_DATABASE
    )

    # Create frida database
    engine_dest_db = create_engine(src_public_db_path)
    if not database_exists(engine_dest_db.url):
        create_database(engine_dest_db.url)

    # Clean frida database if exist
    db.drop_all()

    # Create databases tables
    db.create_all()
    db.session.commit()

    # Create objects
    _create_seeder()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
