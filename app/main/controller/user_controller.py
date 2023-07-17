from flask import request
from flask_restx import Resource

from app.main.config import Config
from app.main.model import User
from app.main.service import (
    delete_user,
    get_user_by_id,
    get_users,
    save_new_user,
    token_required,
)
from app.main.util import DefaultResponsesDTO, UserDTO

user_ns = UserDTO.api
api = user_ns

_user_post = UserDTO.user_post
_user_list = UserDTO.user_list
_user_response = UserDTO.user_response

_default_message_response = DefaultResponsesDTO.message_response
_validation_error_response = DefaultResponsesDTO.validation_error

_CONTENT_PER_PAGE = Config.CONTENT_PER_PAGE
_DEFAULT_CONTENT_PER_PAGE = Config.DEFAULT_CONTENT_PER_PAGE


@api.route("")
class User(Resource):
    @api.doc(
        "List all registered users",
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
        description=f"List all registered users with pagination. {_DEFAULT_CONTENT_PER_PAGE} users per page.",
    )
    @api.marshal_with(_user_list, code=200, description="users_list")
    @api.response(401, "token_not_found\ntoken_invalid", _default_message_response)
    @api.response(403, "required_administrator_privileges", _default_message_response)
    @token_required(admin_privileges_required=True)
    def get(self, current_user) -> tuple[dict[str, any], int]:
        """List all registered users"""
        params = request.args
        return get_users(params=params, current_user=current_user)

    @api.doc("Create a new user")
    @api.expect(_user_post, validate=True)
    @api.response(201, "user_created", _default_message_response)
    @api.response(400, "Input payload validation failed", _validation_error_response)
    @api.response(409, "username_in_use\nemail_in_use", _default_message_response)
    def post(self) -> tuple[dict[str, str], int]:
        """Create a new user"""
        data = request.json
        save_new_user(data=data)
        return {"message": "user_created"}, 201


@api.route("/current")
class CurrentUser(Resource):
    @api.doc("Get a current user")
    @api.marshal_with(_user_response, code=200, description="user_info")
    @api.response(401, "token_not_found\ntoken_invalid", _default_message_response)
    @api.response(404, "user_not_found", _default_message_response)
    @token_required()
    def get(self, current_user: User) -> tuple[dict[str, any], int]:
        """Get current user"""
        return get_user_by_id(user_id=current_user.id, current_user=current_user)


@api.route("/<int:user_id>")
class UserById(Resource):
    @api.doc("Get user by id")
    @api.marshal_with(_user_response, code=200, description="user_info")
    @api.response(401, "token_not_found\ntoken_invalid", _default_message_response)
    @api.response(403, "required_administrator_privileges", _default_message_response)
    @api.response(404, "user_not_found", _default_message_response)
    @token_required(admin_privileges_required=True)
    def get(
        self,
        user_id: int,
        current_user,
    ) -> tuple[dict[str, any], int]:
        """Get user by id"""
        return get_user_by_id(user_id=user_id, current_user=current_user)

    @api.doc("Delete a user")
    @api.response(200, "user_deleted", _default_message_response)
    @api.response(401, "token_not_found\ntoken_invalid", _default_message_response)
    @api.response(403, "required_administrator_privileges", _default_message_response)
    @api.response(404, "user_not_found", _default_message_response)
    @token_required(admin_privileges_required=True)
    def delete(self, user_id: int, current_user) -> tuple[dict[str, str], int]:
        """Delete a user"""
        delete_user(user_id=user_id, current_user=current_user)
        return {"message": "user_deleted"}, 200
