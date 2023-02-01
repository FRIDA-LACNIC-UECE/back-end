from datetime import datetime, timedelta
from functools import wraps
from typing import Dict

import bcrypt
import jwt
from flask import request
from werkzeug.security import check_password_hash

from app.main.config import app_config
from app.main.exceptions.default_exception import DefaultException
from app.main.model import User

_secret_key = app_config.SECRET_KEY
_jwt_exp = app_config.JWT_EXP


def hash_password(password: str) -> str:
    password = password.encode("utf-8")
    return bcrypt.hashpw(password, bcrypt.gensalt()).decode("utf-8")


def check_pw(p1: str, p2: str) -> bool:
    p1 = p1.encode("utf-8")
    p2 = p2.encode("utf-8")
    return bcrypt.checkpw(p1, p2)


def login(data: Dict[str, any]) -> tuple[str, User]:
    user = User.query.filter_by(email=data.get("email")).first()

    if not user:
        raise DefaultException(message="user_not_found", code=404)

    login_pwd = data.get("password")
    user_pwd = user.password

    if not check_password_hash(user_pwd, login_pwd):
        raise DefaultException("password_incorrect_information", code=401)

    token = create_jwt(user.id)

    return (token, user)


def jwt_user_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        token = None

        if "Authorization" in request.headers:
            token = request.headers.get("Authorization")
            token = token.replace("Bearer ", "")

        if not token:
            raise DefaultException("token_not_found", code=401)

        try:
            data = jwt.decode(token, _secret_key, algorithms=["HS256"])
            current_user = User.query.get(data.get("id"))
        except:
            raise DefaultException("token_invalid", code=401)

        return f(current_user=current_user, *args, **kwargs)

    return wrapper


def jwt_admin_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        token = None

        if "Authorization" in request.headers:
            token = request.headers["Authorization"]
            token = token.replace("Bearer ", "")

        if not token:
            raise DefaultException("token_not_found", code=401)

        try:
            data = jwt.decode(token, _secret_key, algorithms=["HS256"])
            current_user = User.query.get(data.get("id"))
        except:
            raise DefaultException("token_invalid", code=401)

        if not current_user.is_admin:
            raise DefaultException("required_administrator_privileges", code=403)

        return f(*args, **kwargs)

    return wrapper


def create_jwt(id: int):
    return jwt.encode(
        {"id": id, "exp": datetime.utcnow() + timedelta(hours=_jwt_exp)}, _secret_key
    )
