from app.main import db
from app.main.model import DatabaseKey
from app.main.service import generate_keys


def add_database_keys():
    public_key_str, private_key_str = generate_keys()

    database_keys = DatabaseKey(
        database_id=1, public_key=public_key_str, private_key=private_key_str
    )
    db.session.add(database_keys)

    db.session.flush()
