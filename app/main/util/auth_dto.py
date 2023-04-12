from flask_restx import Namespace, fields


class AuthDTO:
    api = Namespace("auth", description="Authentication related operations")

    auth_login = api.model(
        "auth_login",
        {
            "email": fields.String(
                required=True, description="user email", example="convidado@example.com"
            ),
            "password": fields.String(
                required=True,
                description="user password",
                min_length=8,
                max_length=1024,
                example="convidado123",
            ),
        },
    )

    auth_response = api.model(
        "auth_response",
        {
            "id": fields.Integer(description="user id"),
            "username": fields.String(description="user username"),
            "is_admin": fields.Integer(description="user admin flag"),
            "token": fields.String(
                description="user token",
            ),
        },
    )
