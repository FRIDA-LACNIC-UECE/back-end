from time import time

from app.main import db
from app.main.config import Config
from app.main.exceptions import DefaultException
from app.main.model import User
from app.main.service.email_service import send_email_activation
from app.main.service.auth_service import token_generate

_EXP_ACTIVATION = Config.ACTIVATION_EXP_SECONDS


def check_activation_token(token: str) -> User:
    user = User.query.filter_by(activation_token=token).scalar()

    if not user:
        raise DefaultException("token_invalid", code=409)

    if user.activation_token_exp < int(time()):
        raise DefaultException("token_expired", code=401)

    return user


def activate_user(token: str) -> None:
    user = check_activation_token(token)

    if user.status == "active":
        raise DefaultException("user_already_activated", code=409)

    if user.status == "blocked":
        raise DefaultException("user_is_blocked", code=409)

    user.status = "active"

    db.session.commit()


def resend_activation_email(user_id: int) -> None:
    user = User.query.filter_by(id=user_id).scalar()

    if not user:
        raise DefaultException("user_not_found", code=404)

    if user.status == "active":
        raise DefaultException("user_already_activated", code=409)

    if user.status == "blocked":
        raise DefaultException("user_is_blocked", code=409)

    new_token = token_generate(user.email)

    user.activation_token = new_token
    user.activation_token_exp = int(time()) + _EXP_ACTIVATION

    db.session.commit()

    send_email_activation(to=user.email, username=user.username, token=new_token)
