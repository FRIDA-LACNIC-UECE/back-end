from flask import request
from flask_restx import Resource

from app.main.config import Config
from app.main.model import User
from app.main.service import (
    delete_database,
    get_database_by_id,
    get_database_tables,
    get_databases,
    get_route_sensitive_columns,
    jwt_admin_required,
    jwt_user_required,
    route_get_database_columns,
    save_new_database,
    test_database_connection,
    update_database,
)
from app.main.util import DatabaseDTO, DefaultResponsesDTO

database_ns = DatabaseDTO.api
api = database_ns

_database_put = DatabaseDTO.database_put
_database_post = DatabaseDTO.database_post
_database_response = DatabaseDTO.database_response
_database_list = DatabaseDTO.database_list
_database_tables = DatabaseDTO.database_tables
_database_columns = DatabaseDTO.database_columns
_database_sensitive_columns = DatabaseDTO.database_sensitive_columns

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
    @jwt_user_required
    def get(self, current_user: User):
        """List all registered databases of each user"""
        params = request.args
        return get_databases(params=params, current_user=current_user)

    @api.doc("Create a new database")
    @api.expect(_database_post, validate=True)
    @api.response(201, "database_created", _default_message_response)
    @api.response(400, "Input payload validation failed", _default_message_response)
    @api.response(401, "token_not_found\ntoken_invalid", _default_message_response)
    @jwt_user_required
    def post(self, current_user) -> tuple[dict[str, str], int]:
        """Create a new database"""
        data = request.json
        data["user_id"] = current_user.id
        save_new_database(data=data)
        return {"message": "database_created"}, 201


@api.route("/<int:database_id>")
class DatabaseById(Resource):
    @api.doc("Get database by id")
    @api.marshal_with(_database_response, code=200, description="database_info")
    @api.response(401, "token_not_found\ntoken_invalid", _default_message_response)
    @api.response(403, "required_administrator_privileges", _default_message_response)
    @api.response(404, "database_not_found", _default_message_response)
    @jwt_admin_required
    def get(self, database_id: int):
        """Get database by id"""
        return get_database_by_id(database_id=database_id)

    @api.doc("Update a database")
    @api.expect(_database_put, validate=True)
    @api.response(200, "database_updated", _default_message_response)
    @api.response(400, "Input payload validation failed", _validation_error_response)
    @api.response(401, "token_not_found\ntoken_invalid", _default_message_response)
    @api.response(403, "required_administrator_privileges", _default_message_response)
    @api.response(404, "database_not_found", _default_message_response)
    @jwt_user_required
    def put(self, database_id, current_user):
        """Update a database"""
        data = request.json
        update_database(database_id=database_id, current_user=current_user, data=data)
        return {"message": "database_updated"}, 200

    @api.doc("Delete a database")
    @api.response(200, "database_deleted", _default_message_response)
    @api.response(401, "token_not_found\ntoken_invalid\nunauthorized_user")
    @api.response(404, "database_not_found", _default_message_response)
    @jwt_user_required
    def delete(self, database_id: int, current_user: User):
        """Delete a database"""
        delete_database(database_id=database_id, current_user=current_user)
        return {"message": "database_deleted"}, 200


@api.route("/table_names/<int:database_id>")
class DatabaseTables(Resource):
    @api.doc("Get database table names by id")
    @api.marshal_with(
        _database_tables, code=200, description="get_database_table_names"
    )
    @api.response(401, "token_not_found\ntoken_invalid", _default_message_response)
    @api.response(404, "database_not_found", _default_message_response)
    @jwt_user_required
    def get(self, database_id: int, current_user: User):
        """Get database table names by id"""
        return get_database_tables(database_id=database_id, current_user=current_user)


@api.route("/columns/<int:database_id>")
class DatabaseColumns(Resource):
    @api.doc()
    @api.doc(
        "Get database column names each table by id",
        params={
            "table_name": {
                "description": "Database table name",
                "type": str,
                "required": True,
            },
        },
        description="Get database column names each table by id.",
    )
    @api.marshal_with(
        _database_columns, code=200, description="get_database_column_names"
    )
    @api.response(401, "token_not_found\ntoken_invalid", _default_message_response)
    @api.response(404, "database_not_found", _default_message_response)
    @jwt_user_required
    def get(self, database_id: int, current_user: User):
        """Get database column names each table by id"""
        params = request.args
        return route_get_database_columns(
            database_id=database_id, current_user=current_user, params=params
        )


@api.route("/sensitive_columns/<int:database_id>")
class DatabaseSensitiveColumns(Resource):
    @api.doc("Get database sensitive columns names each table by id")
    @api.doc(
        "List all registered databases of each user",
        params={
            "table_name": {
                "description": "Database table name",
                "type": str,
                "required": True,
            },
        },
        description=f"Get database sensitive columns names each table by id",
    )
    @api.response(401, "token_not_found\ntoken_invalid", _default_message_response)
    @api.response(404, "database_not_found", _default_message_response)
    @api.marshal_with(
        _database_sensitive_columns,
        code=200,
        description="get_database_sensitive_columns_names",
    )
    @jwt_user_required
    def get(self, database_id: int, current_user: User):
        """Get database sensitive columns names each table by id"""
        params = request.args
        return get_route_sensitive_columns(
            database_id=database_id, params=params, current_user=current_user
        )


@api.route("/test_database_connection/<int:database_id>")
class TestDatabaseConnection(Resource):
    @api.doc("Test database connection")
    @api.response(200, "database_connected", _default_message_response)
    @api.response(400, "Input payload validation failed", _default_message_response)
    @api.response(401, "token_not_found\ntoken_invalid", _default_message_response)
    @jwt_user_required
    def post(self, database_id, current_user) -> tuple[dict[str, str], int]:
        """ "Test database connection"""
        test_database_connection(database_id=database_id, current_user=current_user)
        return {"message": "database_connected"}, 200
