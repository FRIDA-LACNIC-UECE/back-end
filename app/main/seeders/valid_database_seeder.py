from app.main import db
from app.main.model import ValidDatabase


def add_valid_database():
    new_valid_database = ValidDatabase(name="mysql")
    db.session.add(new_valid_database)
    db.session.flush()

    new_valid_database = ValidDatabase(name="postgresql")
    db.session.add(new_valid_database)
    db.session.flush()
