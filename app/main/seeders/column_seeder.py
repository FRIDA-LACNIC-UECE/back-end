"""
from app.main import db
from app.main.model import Column


def add_columns():
    new_column = Column(
        table_id=1,
        anonymization_type_id=1,
        name="cpf",
        type="VARCHAR",
    )
    db.session.add(new_column)

    new_column = Column(
        table_id=1,
        anonymization_type_id=3,
        name="email",
        type="VARCHAR",
    )
    db.session.add(new_column)

    db.session.flush()
"""
