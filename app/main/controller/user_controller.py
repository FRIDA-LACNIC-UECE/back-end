from flask import request
from flask_restx import Resource

from app.main.config import Config
from app.main.service import (
    delete_user,
    get_user_by_id,
    get_users,
    jwt_admin_required,
    jwt_user_required,
    save_new_user,
)
from app.main.util import DefaultResponsesDTO, UserDTO

user_ns = UserDTO.api
api = user_ns
_user_post = UserDTO.user_post
_user_list = UserDTO.user_list
_user_response = UserDTO.user_response

_default_message_response = DefaultResponsesDTO.message_response

_CONTENT_PER_PAGE = Config.CONTENT_PER_PAGE
_DEFAULT_CONTENT_PER_PAGE = Config.DEFAULT_CONTENT_PER_PAGE


@api.route("")
class User(Resource):
    @api.doc(
        "List of registered users",
        params={
            "page": {"description": "Page number", "default": 1, "type": int},
            "per_page": {
                "description": "Items per page",
                "default": _DEFAULT_CONTENT_PER_PAGE,
                "enum": _CONTENT_PER_PAGE,
                "type": int,
            },
            "username": {"description": "User username", "type": str},
        },
        description=f"List of registered users with pagination. {_DEFAULT_CONTENT_PER_PAGE} users per page.",
    )
    @api.marshal_with(_user_list, code=200, description="users_list")
    @api.response(401, "token_not_found\ntoken_invalid", _default_message_response)
    @api.response(403, "required_administrator_privileges", _default_message_response)
    @jwt_admin_required
    def get(self):
        """List all registered users"""
        params = request.args
        return get_users(params=params)

    @api.doc("Create a new user")
    @api.expect(_user_post, validate=True)
    @api.response(201, "user_created", _default_message_response)
    @api.response(400, "Input payload validation failed", _default_message_response)
    @api.response(409, "username_in_use\nemail_in_use", _default_message_response)
    def post(self) -> tuple[dict[str, str], int]:
        """Create a new user"""
        data = request.json
        save_new_user(data=data)
        return {"message": "user_created"}, 201


@api.route("/current")
class CurrentUser(Resource):
    @api.doc("Get current user")
    @api.marshal_with(_user_response, code=200, description="user_info")
    @api.response(401, "token_not_found\ntoken_invalid", _default_message_response)
    @api.response(404, "user_not_found", _default_message_response)
    @jwt_user_required
    def get(self, current_user):
        """Get current user"""
        return get_user_by_id(user_id=current_user.id)


@api.route("/<int:user_id>")
class UserById(Resource):
    @api.doc("Get user by id")
    @api.marshal_with(_user_response, code=200, description="user_info")
    @api.response(401, "token_not_found\ntoken_invalid", _default_message_response)
    @api.response(403, "required_administrator_privileges", _default_message_response)
    @api.response(404, "user_not_found", _default_message_response)
    @jwt_admin_required
    def get(self, user_id: int):
        """Get user by id"""
        return get_user_by_id(user_id=user_id)

    @api.doc("Delete a user")
    @api.response(200, "user_deleted", _default_message_response)
    @api.response(401, "token_not_found\ntoken_invalid", _default_message_response)
    @api.response(403, "required_administrator_privileges", _default_message_response)
    @api.response(404, "user_not_found", _default_message_response)
    @jwt_admin_required
    def delete(self, user_id: int) -> tuple[dict[str, str], int]:
        """Delete a user"""
        delete_user(user_id=user_id)
        return {"message": "user_deleted"}, 200