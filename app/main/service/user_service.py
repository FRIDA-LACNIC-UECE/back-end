from math import ceil
from time import time

from sqlalchemy import or_
from werkzeug.datastructures import ImmutableMultiDict
from werkzeug.security import generate_password_hash

from app.main import db
from app.main.config import Config
from app.main.exceptions import DefaultException
from app.main.model import Database, User
from app.main.service import token_generate
from app.main.service.email_service import send_email_activation

_DEFAULT_CONTENT_PER_PAGE = Config.DEFAULT_CONTENT_PER_PAGE
_EXP_ACTIVATION = Config.ACTIVATION_EXP_SECONDS


def get_users(params: ImmutableMultiDict) -> dict[str, any]:
    page = params.get("page", type=int, default=1)
    per_page = params.get("per_page", type=int, default=_DEFAULT_CONTENT_PER_PAGE)
    username = params.get("username", type=str)

    filters = []

    if username is not None:
        filters.append(User.username.ilike(f"%{username}%"))

    pagination = (
        User.query.filter(*filters)
        .order_by(User.id)
        .paginate(page=page, per_page=per_page, error_out=False)
    )

    return {
        "current_page": page,
        "total_items": pagination.total,
        "total_pages": ceil(pagination.total / per_page),
        "items": pagination.items,
    }


def get_user_by_id(user_id: int) -> User:
    return get_user(user_id=user_id)


def save_new_user(data: dict[str, str]) -> None:
    username = data.get("username")
    email = data.get("email")
    password = data.get("password")

    _validate_user_constraint(username=username, email=email)

    new_user = User(
        username=username,
        email=email,
        password=generate_password_hash(password),
        is_admin=0,
        activation_token=token_generate(email),
        activation_token_exp=int(time()) + _EXP_ACTIVATION,
        reset_password_token=None,
        reset_password_token_exp=None,
    )

    db.session.add(new_user)
    db.session.commit()

    send_email_activation(
        to=new_user.email,
        username=new_user.username,
        token=new_user.activation_token,
    )


def delete_user(user_id: int) -> None:
    user = get_user(user_id=user_id)

    _verify_user_relationship(user_id=user_id)

    db.session.delete(user)
    db.session.commit()


def get_user(user_id: int, options: list = None) -> User:
    query = User.query

    if options is not None:
        query = query.options(*options)

    user = query.get(user_id)

    if user is None:
        raise DefaultException("user_not_found", code=404)

    return user


def _validate_user_constraint(username: str, email: str) -> None:
    filters = [or_(User.username == username, User.email == email)]

    if (
        user := User.query.with_entities(User.username, User.email)
        .filter(*filters)
        .first()
    ):
        if user.username == username:
            raise DefaultException("username_in_use", code=409)
        else:
            raise DefaultException("email_in_use", code=409)


def _verify_user_relationship(user_id: int) -> None:
    if Database.query.filter(Database.user_id == user_id).first():
        raise DefaultException("registered_database_with_user_id", code=409)


def get_user(user_id: int, options: list = None) -> User:
    query = User.query

    if options is not None:
        query = query.options(*options)

    user = query.get(user_id)

    if user is None:
        raise DefaultException("user_not_found", code=404)

    return user
