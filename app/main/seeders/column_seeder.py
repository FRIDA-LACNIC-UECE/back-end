from app.main import db
from app.main.model import Column, Database


def add_columns():
    database = Database.query.get(1)

    new_column = Column(
        table_id=1,
        anonymization_type_id=1,
        name="quantidade",
        type="int",
    )
    db.session.add(new_column)

    new_column = Column(
        table_id=1,
        anonymization_type_id=1,
        name="nome",
        type="string",
    )
    db.session.add(new_column)

    db.session.flush()
