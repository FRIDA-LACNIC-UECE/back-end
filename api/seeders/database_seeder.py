from api import db

from ..model import Database


def _add_database():
    database = Database(
        id_user=2,
        id_db_type=1,
        name="test_frida_db",
        host="db",
        user="root",
        port=3306,
        password="",
        ssh="",
    )

    db.session.add(database)
    db.session.flush()
