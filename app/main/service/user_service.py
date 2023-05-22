import math
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


def get_users(params: ImmutableMultiDict) -> dict:
    page = params.get("page", type=int, default=1)
    per_page = params.get("per_page", type=int, default=_DEFAULT_CONTENT_PER_PAGE)
    username = params.get("username", type=str)

    filters = []

    if username:
        filters.append(User.username.ilike(f"%{username}%"))

    pagination = (
        User.query.filter(*filters)
        .order_by(User.id)
        .paginate(page=page, per_page=per_page, error_out=False)
    )
    total, users = pagination.total, pagination.items

    return {
        "current_page": page,
        "total_items": pagination.total,
        "total_pages": math.ceil(total / per_page),
        "items": users,
    }


def get_user_by_email(user_email: str) -> User:
    user = (
        User.query.with_entities(User.id, User.email)
        .filter(User.email == user_email)
        .first()
    )

    return user


def get_user_by_id(user_id: int) -> User:
    user = get_user(user_id=user_id)

    return user


def get_user_by_username(user_username: str) -> User:
    user = (
        User.query.with_entities(User.id, User.username)
        .filter(User.username == user_username)
        .first()
    )

    return user


def save_new_user(data: dict[str, str]) -> None:
    username = data.get("username")
    email = data.get("email")
    password = data.get("password")

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

    if Database.query.filter(Database.user_id == user_id).first():
        raise DefaultException("registered_database_with_user_id", code=409)

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
