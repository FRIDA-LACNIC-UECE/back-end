from datetime import datetime, timedelta
import random
import string
import bcrypt
import jwt

from functools import wraps
from typing import Dict

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


def token_generate(seed=None, size=64) -> str:
    if seed is not None:
        random.seed(seed)
    return "".join(
        random.SystemRandom().choice(
            string.ascii_uppercase + string.ascii_lowercase + string.digits
        )
        for _ in range(size)
    )


def login(data: Dict[str, any]) -> tuple[str, User]:
    user = User.query.filter_by(email=data.get("email")).first()

    if not user:
        raise DefaultException(message="user_not_found", code=404)

    if user.status == "wait_activation":
        raise DefaultException("wait_activation", code=409)

    if user.status == "blocked":
        raise DefaultException("user_is_blocked", code=409)

    login_pwd = data.get("password")
    user_pwd = user.password

    if not check_password_hash(user_pwd, login_pwd):
        raise DefaultException("password_incorrect_information", code=401)

    token = create_jwt(user.id)

    return (token, user)


def token_required(admin_privileges_required=False, return_user=True):
    def wrapper(f):
        @wraps(f)
        def decorator(*args, **kwargs):
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

            if admin_privileges_required:
                if not current_user.is_admin:
                    raise DefaultException(
                        "required_administrator_privileges", code=403
                    )

            if return_user:
                return f(current_user=current_user, *args, **kwargs)

            return f(*args, **kwargs)

        return decorator

    return wrapper


def jwt_user_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        token = None

        if "Authorization" in request.headers:
            token = request.headers.get("Authorization")

            if token[:7] != "Bearer ":
                raise DefaultException("token_invalid", code=401)

            token = token[7:]

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
