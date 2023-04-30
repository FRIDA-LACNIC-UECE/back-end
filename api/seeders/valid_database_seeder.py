from api import db

from ..model import ValidDatabase


def _add_valid_database():
    mysql = ValidDatabase(name="mysql")
    db.session.add(mysql)
    db.session.flush()
