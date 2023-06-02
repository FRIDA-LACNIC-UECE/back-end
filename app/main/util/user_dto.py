from flask_restx import Namespace, fields

from app.main.util.custom_field import Email, Password


class UserDTO:
    api = Namespace("user", description="User related operations")

    user_id = {"id": fields.Integer(description="user id")}

    user_username = {
        "username": fields.String(
            required=True, description="user username", min_length=1, max_length=30
        )
    }

    user_email = {"email": Email(required=True, description="user email")}

    user_password = {"password": Password(required=True, description="user password")}

    user_is_admin = {
        "is_admin": fields.Boolean(
            required=True,
            description="Flag indicating if the user is an admin (True) or not (False).",
        )
    }

    user_post = api.model("user_post", user_username | user_email | user_password)

    user_response = api.model(
        "user_response", user_id | user_username | user_email | user_is_admin
    )

    user_list = api.model(
        "user_list",
        {
            "current_page": fields.Integer(),
            "total_items": fields.Integer(),
            "total_pages": fields.Integer(),
            "items": fields.List(fields.Nested(user_response)),
        },
    )
