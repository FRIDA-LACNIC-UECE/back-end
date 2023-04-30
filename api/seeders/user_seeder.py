from api import db

from ..model import User


def _add_user():
    admin_user = User(
        name="admin",
        email="admin@gmail.com",
        password="12345",
        is_admin=1,
    )

    db.session.add(admin_user)
    db.session.flush()

    user = User(
        name="convidado",
        email="convidado@gmail.com",
        password="12345",
        is_admin=0,
    )

    db.session.add(user)
    db.session.flush()
