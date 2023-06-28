from flask_restx import Namespace, fields


class PasswordDTO:
    api = Namespace("password", description="password related operations")

    password_change = api.model(
        "password_change",
        {
            "current_password": fields.String(
                required=True, description="current user password", min_length=8
            ),
            "new_password": fields.String(
                required=True, description="new user password", min_length=8
            ),
            "repeat_new_password": fields.String(
                required=True, description="repeat new user password", min_length=8
            ),
        },
    )

    password_forgot = api.model(
        "password_forgot",
        {"email": fields.String(required=True, description="professional mail")},
    )

    password_redefine = api.model(
        "password_redefine",
        {
            "new_password": fields.String(
                required=True, description="new user password", min_length=8
            ),
            "repeat_new_password": fields.String(
                required=True, description="repeat new user password", min_length=8
            ),
        },
    )
