from werkzeug.security import generate_password_hash

from app.main import db
from app.main.model import User


def add_user():
    admin_user = User(
        username="admin",
        email="admin@example.com",
        password=generate_password_hash("admin123"),
        is_admin=1,
    )

    db.session.add(admin_user)
    db.session.flush()

    new_user = User(
        username="convidado",
        email="convidado@example.com",
        password=generate_password_hash("convidado123"),
        is_admin=0,
    )

    db.session.add(new_user)
    db.session.flush()

    new_user = User(
        username="convidado2",
        email="convidado2@example.com",
        password=generate_password_hash("convidado2123"),
        is_admin=0,
    )

    db.session.add(new_user)
    db.session.flush()
