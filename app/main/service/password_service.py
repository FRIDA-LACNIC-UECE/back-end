from time import time

from app.main import db
from app.main.config import Config
from app.main.exceptions import DefaultException
from app.main.model import User
from app.main.service.activation_service import token_generate
from app.main.service.email_service import send_email_recovery
from app.main.service.user_service import get_user
from werkzeug.security import generate_password_hash, check_password_hash

_EXP_ACTIVATION = Config.ACTIVATION_EXP_SECONDS


def change_password(data: dict[str, str], current_user: User) -> None:
    current_password = data.get("current_password")
    new_password = data.get("new_password")
    repeat_new_password = data.get("repeat_new_password")

    if not new_password == repeat_new_password:
        raise DefaultException("passwords_not_match", code=409)

    if not check_password_hash(current_user.password, current_password):
        raise DefaultException("password_incorrect_information", code=409)

    current_user.password = generate_password_hash(new_password)

    db.session.commit()


def forgot_password(data: dict[str, str]) -> None:
    user = User.query.filter_by(email=data.get("email")).scalar()

    if not user:
        raise DefaultException("user_not_found", code=404)

    if user.status != "active":
        raise DefaultException("user_not_actived", code=409)

    token = token_generate(user.email)
    user.reset_password_token = token
    user.reset_password_token_exp = int(time()) + _EXP_ACTIVATION

    db.session.commit()

    send_email_recovery(to=user.email, username=user.username, token=token)


def check_password_reset_token(token: str) -> User:
    user = User.query.filter_by(reset_password_token=token).scalar()

    if not user:
        raise DefaultException("token_invalid", code=409)

    if user.reset_password_token_exp < int(time()):
        raise DefaultException("token_expired", code=401)

    return user


def redefine_password(token: str, data: dict[str, str]) -> None:
    user = check_password_reset_token(token)

    if user.status != "active":
        raise DefaultException("user_not_active", code=409)

    new_password = data.get("new_password")
    repeat_new_password = data.get("repeat_new_password")

    if not new_password == repeat_new_password:
        raise DefaultException("passwords_not_match", code=409)

    user.password = generate_password_hash(new_password)

    db.session.commit()
