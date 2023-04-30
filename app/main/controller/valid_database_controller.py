from flask import request
from flask_restx import Resource

from app.main.config import Config
from app.main.service import (
    delete_valid_database,
    get_valid_database_by_id,
    get_valid_databases,
    jwt_admin_required,
    jwt_user_required,
    save_new_valid_database,
    update_valid_database,
)
from app.main.util import DefaultResponsesDTO, ValidDatabaseDTO

valid_database_ns = ValidDatabaseDTO.api
api = valid_database_ns
_valid_database_post = ValidDatabaseDTO.valid_database_post
_valid_databaset_put = ValidDatabaseDTO.valid_database_put
_valid_database_response = ValidDatabaseDTO.valid_database_response
_valid_database_list = ValidDatabaseDTO.valid_database_list

_default_message_response = DefaultResponsesDTO.message_response
_validation_error_response = DefaultResponsesDTO.validation_error

_CONTENT_PER_PAGE = Config.CONTENT_PER_PAGE
_DEFAULT_CONTENT_PER_PAGE = Config.DEFAULT_CONTENT_PER_PAGE


@api.route("")
class ValidDatabase(Resource):
    @api.doc(
        "List of registered valid databases",
        params={
            "page": {"description": "Page number", "default": 1, "type": int},
            "per_page": {
                "description": "Items per page",
                "default": _DEFAULT_CONTENT_PER_PAGE,
                "enum": _CONTENT_PER_PAGE,
                "type": int,
            },
            "name": {"description": "Database name", "type": str},
        },
        description=f"List of registered valid databases with pagination. {_DEFAULT_CONTENT_PER_PAGE} valid databases per page.",
    )
    @api.marshal_with(
        _valid_database_list, code=200, description="valid_databases_list"
    )
    @api.response(401, "token_not_found\ntoken_invalid", _default_message_response)
    @jwt_user_required
    def get(self, current_user):
        """List of registered valid databases"""
        params = request.args
        return get_valid_databases(params=params)

    @api.doc("Create a new valid database")
    @api.expect(_valid_database_post, validate=True)
    @api.response(201, "valid_database_created", _default_message_response)
    @api.response(400, "Input payload validation failed", _validation_error_response)
    @api.response(401, "token_not_found\ntoken_invalid", _default_message_response)
    @api.response(403, "required_administrator_privileges", _default_message_response)
    @api.response(409, "valid_database_exists", _default_message_response)
    @jwt_admin_required
    def post(self) -> tuple[dict[str, str], int]:
        """Create a new valid database"""
        data = request.json
        save_new_valid_database(data=data)
        return {"message": "valid_database_created"}, 201


@api.route("/<int:valid_database_id>")
class ValidDatabaseById(Resource):
    @api.doc("Get valid database by id")
    @api.marshal_with(
        _valid_database_response, code=200, description="valid_database_info"
    )
    @api.response(401, "token_not_found\ntoken_invalid", _default_message_response)
    @api.response(403, "required_administrator_privileges", _default_message_response)
    @api.response(404, "valid_database_not_found", _default_message_response)
    @jwt_admin_required
    def get(self, valid_database_id: int):
        """Get valid database by id"""
        return get_valid_database_by_id(valid_database_id=valid_database_id)

    @api.doc("Update a valid database")
    @api.expect(_valid_databaset_put, validate=True)
    @api.response(200, "valid_database_updated", _default_message_response)
    @api.response(400, "Input payload validation failed", _validation_error_response)
    @api.response(401, "token_not_found\ntoken_invalid", _default_message_response)
    @api.response(403, "required_administrator_privileges", _default_message_response)
    @api.response(404, "valid_database_not_found", _default_message_response)
    @api.response(409, "valid_database_already_exists", _default_message_response)
    @jwt_admin_required
    def put(self, valid_database_id):
        """Update a valid database"""
        data = request.json
        update_valid_database(valid_database_id=valid_database_id, data=data)
        return {"message": "valid_database_updated"}, 200

    @api.doc("Delete a valid database")
    @api.response(200, "valid_database_deleted", _default_message_response)
    @api.response(401, "token_not_found\ntoken_invalid", _default_message_response)
    @api.response(403, "required_administrator_privileges", _default_message_response)
    @api.response(404, "valid_database_not_found", _default_message_response)
    @jwt_admin_required
    def delete(self, valid_database_id: int) -> tuple[dict[str, str], int]:
        """Delete a valid database"""
        delete_valid_database(valid_database_id=valid_database_id)
        return {"message": "valid_database_deleted"}, 200
