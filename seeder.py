from app.main import db
from app.main.model import AnonymizationType, User


def create_seed():
    _add_user()
    _add_anonymization_type()
    db.session.commit()


def _add_user():
    new_user = User(
        username="admin", email="admin@example.br", password="admin123", is_admin=1
    )

    db.session.add(new_user)
    db.session.flush()

    new_user = User(
        username="convidado",
        email="convidado@example.br",
        password="convidado123",
        is_admin=0,
    )

    db.session.add(new_user)
    db.session.flush()


def _add_anonymization_type():
    new_anonymization_type = AnonymizationType(
        name="test_anonymizer",
    )

    db.session.add(new_anonymization_type)
    db.session.flush()
