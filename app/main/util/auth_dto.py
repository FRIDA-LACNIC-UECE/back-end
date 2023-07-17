from flask_restx import Namespace, fields


class AuthDTO:
    api = Namespace("auth", description="Authentication related operations")

    auth_id = {
        "id": fields.Integer(description="auth user id"),
    }

    auth_username = {"username": fields.String(description="auth user username")}

    auth_email = {
        "email": fields.String(
            required=True,
            description="auth user email",
            example="convidado@example.com",
        ),
    }

    auth_password = {
        "password": fields.String(
            required=True,
            description="auth user password",
            example="Convidado@123",
        ),
    }

    auth_is_admin = {
        "is_admin": fields.Boolean(
            required=True,
            description="Flag indicating if the user is an admin (True) or not (False).",
        )
    }

    auth_token = {
        "token": fields.String(
            description="auth user token",
        ),
    }

    auth_login = api.model("auth_login", auth_email | auth_password)

    auth_response = api.model(
        "auth_response", auth_id | auth_username | auth_is_admin | auth_token
    )
