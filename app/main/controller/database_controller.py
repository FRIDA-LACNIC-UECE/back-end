from flask import request
from flask_restx import Resource

from app.main.config import Config
from app.main.model import User
from app.main.service import (
    delete_database,
    get_database_by_id,
    get_databases,
    get_route_sensitive_columns,
    save_new_database,
    test_database_connection,
    test_database_connection_by_url,
    token_required,
    update_database,
)
from app.main.util import DatabaseDTO, DefaultResponsesDTO

database_ns = DatabaseDTO.api
api = database_ns

_database_put = DatabaseDTO.database_put
_database_post = DatabaseDTO.database_post
_database_test_connection_by_url = DatabaseDTO.database_post
_database_response = DatabaseDTO.database_response
_database_list = DatabaseDTO.database_list

_default_message_response = DefaultResponsesDTO.message_response
_validation_error_response = DefaultResponsesDTO.validation_error

_CONTENT_PER_PAGE = Config.CONTENT_PER_PAGE
_DEFAULT_CONTENT_PER_PAGE = Config.DEFAULT_CONTENT_PER_PAGE


@api.route("")
class Database(Resource):
    @api.doc(
        "List all registered databases of each user",
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
        description=f"List all registered databases of each user with pagination. {_DEFAULT_CONTENT_PER_PAGE} databases per page.",
    )
    @api.marshal_with(_database_list, code=200, description="databases_list")
    @api.response(401, "token_not_found\ntoken_invalid", _default_message_response)
    @token_required()
    def get(self, current_user: User) -> tuple[dict[str, any], int]:
        """List all registered databases of each user"""
        params = request.args
        return get_databases(params=params, current_user=current_user)

    @api.doc("Create a new database")
    @api.expect(_database_post, validate=True)
    @api.response(201, "database_created", _default_message_response)
    @api.response(400, "Input_payload_validation_failed", _validation_error_response)
    @api.response(401, "token_not_found\ntoken_invalid", _default_message_response)
    @api.response(409, "database_already_exists", _default_message_response)
    @api.response(500, "database_not_created", _default_message_response)
    @token_required()
    def post(self, current_user: User) -> tuple[dict[str, str], int]:
        """Create a new database"""
        data = request.json
        save_new_database(data=data, current_user=current_user)
        return {"message": "database_created"}, 201


@api.route("/<int:database_id>")
class DatabaseById(Resource):
    @api.doc("Get database by id")
    @api.marshal_with(_database_response, code=200, description="database_info")
    @api.response(
        401,
        "token_not_found\ntoken_invalid\nunauthorized_user",
        _default_message_response,
    )
    @api.response(403, "required_administrator_privileges", _default_message_response)
    @api.response(404, "database_not_found", _default_message_response)
    @token_required()
    def get(self, database_id: int, current_user: User) -> tuple[dict[str, any], int]:
        """Get database by id"""
        return get_database_by_id(database_id=database_id, current_user=current_user)

    @api.doc("Update a database")
    @api.expect(_database_put, validate=True)
    @api.response(200, "database_updated", _default_message_response)
    @api.response(400, "Input_payload_validation_failed", _validation_error_response)
    @api.response(
        401,
        "token_not_found\ntoken_invalid\nunauthorized_user",
        _default_message_response,
    )
    @api.response(403, "required_administrator_privileges", _default_message_response)
    @api.response(
        404, "valid_database_not_found\ndatabase_not_found", _default_message_response
    )
    @token_required()
    def put(self, database_id, current_user) -> tuple[dict[str, str], int]:
        """Update a database"""
        data = request.json
        update_database(database_id=database_id, current_user=current_user, data=data)
        return {"message": "database_updated"}, 200

    @api.doc("Delete a database")
    @api.response(200, "database_deleted", _default_message_response)
    @api.response(401, "token_not_found\ntoken_invalid\nunauthorized_user")
    @api.response(404, "database_not_found", _default_message_response)
    @api.response(404, "database_not_found", _default_message_response)
    @token_required()
    def delete(
        self, database_id: int, current_user: User
    ) -> tuple[dict[str, str], int]:
        """Delete a database"""
        delete_database(database_id=database_id, current_user=current_user)
        return {"message": "database_deleted"}, 200


@api.route("/<int:database_id>/test_database_connection")
class TestDatabaseConnection(Resource):
    @api.doc("Test database connection")
    @api.response(200, "database_connected", _default_message_response)
    @api.response(400, "Input payload validation failed", _validation_error_response)
    @api.response(
        401,
        "token_not_found\ntoken_invalid\nunauthorized_user",
        _default_message_response,
    )
    @api.response(409, "database_not_connected", _default_message_response)
    @token_required()
    def post(self, database_id, current_user) -> tuple[dict[str, str], int]:
        """Test database connection"""
        test_database_connection(database_id=database_id, current_user=current_user)
        return {"message": "database_connected"}, 200


@api.route("/test_database_connection_by_url")
class TestDatabaseConnectionByUrl(Resource):
    @api.doc("Test database connection by url")
    @api.expect(_database_test_connection_by_url, validate=True)
    @api.response(200, "database_connected", _default_message_response)
    @api.response(400, "Input payload validation failed", _validation_error_response)
    @api.response(401, "token_not_found\ntoken_invalid", _default_message_response)
    @api.response(409, "database_not_connected", _default_message_response)
    @token_required(return_user=False)
    def post(self) -> tuple[dict[str, str], int]:
        """Test database connection by url"""
        data = request.json
        test_database_connection_by_url(data=data)
        return {"message": "database_connected"}, 200
