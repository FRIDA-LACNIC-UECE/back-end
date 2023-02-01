from flask_restx import Namespace, fields


class UserDTO:
    api = Namespace("user", description="User related operations")

    user_post = api.model(
        "user_post",
        {
            "username": fields.String(
                required=True, description="user username", min_length=3
            ),
            "email": fields.String(required=True, description="user email"),
            "password": fields.String(required=True, description="user password"),
        },
    )

    user_response = api.model(
        "user_response",
        {
            "id": fields.Integer(description="user id"),
            "username": fields.String(description="user username"),
            "email": fields.String(description="user email"),
            "is_admin": fields.Boolean(description="user flag admin"),
        },
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
