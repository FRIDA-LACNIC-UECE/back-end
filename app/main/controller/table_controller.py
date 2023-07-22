from flask import request
from flask_restx import Resource

from app.main.config import Config
from app.main.model import User
from app.main.service import (
    delete_table,
    get_table_by_id,
    get_tables,
    save_new_table,
    token_required,
    update_table,
    get_route_sensitive_columns,
    get_table_columns,
)
from app.main.util import DatabaseDTO, DefaultResponsesDTO, TableDTO

database_ns = DatabaseDTO.api
api = database_ns

_table_put = TableDTO.table_put
_table_post = TableDTO.table_post
_table_response = TableDTO.table_response
_table_list = TableDTO.table_list
_table_columns = TableDTO.table_columns
_table_sensitive_columns = TableDTO.table_sensitive_columns


_default_message_response = DefaultResponsesDTO.message_response
_validation_error_response = DefaultResponsesDTO.validation_error

_CONTENT_PER_PAGE = Config.CONTENT_PER_PAGE
_DEFAULT_CONTENT_PER_PAGE = Config.DEFAULT_CONTENT_PER_PAGE


@api.route("/<int:database_id>/table")
class Table(Resource):
    @api.doc(
        "List all registered tables",
        params={
            "page": {"description": "Page number", "default": 1, "type": int},
            "per_page": {
                "description": "Items per page",
                "default": _DEFAULT_CONTENT_PER_PAGE,
                "enum": _CONTENT_PER_PAGE,
                "type": int,
            },
            "table_name": {"description": "Table name", "type": str},
        },
        description=f"List all registered tables with pagination. {_DEFAULT_CONTENT_PER_PAGE} tables per page.",
    )
    @api.marshal_with(_table_list, code=200, description="tables_list")
    @api.response(
        401,
        "token_not_found\ntoken_invalid\nunauthorized_user",
        _default_message_response,
    )
    @api.response(404, "database_not_found", _default_message_response)
    @token_required()
    def get(self, database_id: int, current_user: User) -> tuple[dict[str, any], int]:
        """List all registred tables"""
        params = request.args
        return get_tables(
            database_id=database_id, params=params, current_user=current_user
        )

    @api.doc("Create a new table")
    @api.expect(_table_post, validate=True)
    @api.response(201, "table_created", _default_message_response)
    @api.response(400, "Input payload validation failed", _validation_error_response)
    @api.response(
        401,
        "token_not_found\ntoken_invalid\nunauthorized_user",
        _default_message_response,
    )
    @api.response(404, "database_not_found", _default_message_response)
    @api.response(
        409,
        "table_already_exists\ntable_not_exists_on_client_database",
        _default_message_response,
    )
    @token_required()
    def post(self, database_id: int, current_user: User) -> tuple[dict[str, str], int]:
        """Create a new table"""
        data = request.json
        save_new_table(data=data, database_id=database_id, current_user=current_user)
        return {"message": "table_created"}, 201


@api.route("/<int:database_id>/table/<int:table_id>")
class TableById(Resource):
    @api.doc("Get table by id")
    @api.marshal_with(_table_response, code=200, description="table_info")
    @api.response(
        401,
        "token_not_found\ntoken_invalid\nunauthorized_user",
        _default_message_response,
    )
    @api.response(404, "table_not_found\ndatabase_not_found", _default_message_response)
    @token_required()
    def get(
        self, table_id: int, database_id: int, current_user: User
    ) -> tuple[dict[str, any], int]:
        """Get table by id"""
        return get_table_by_id(
            table_id=table_id, database_id=database_id, current_user=current_user
        )

    @api.doc("Update a table")
    @api.expect(_table_put, validate=True)
    @api.response(200, "table_updated", _default_message_response)
    @api.response(400, "Input payload validation failed", _validation_error_response)
    @api.response(
        401,
        "token_not_found\ntoken_invalid\nunauthorized_user",
        _default_message_response,
    )
    @api.response(404, "table_not_found\ndatabase_not_found", _default_message_response)
    @api.response(
        409,
        "table_already_exists\ntable_not_exists_on_client_database",
        _default_message_response,
    )
    @token_required()
    def put(
        self, table_id: int, database_id: int, current_user: User
    ) -> tuple[dict[str, str], int]:
        """Update a table"""
        data = request.json
        update_table(
            table_id=table_id,
            database_id=database_id,
            data=data,
            current_user=current_user,
        )
        return {"message": "table_updated"}, 200

    @api.doc("Delete a table")
    @api.response(200, "table_deleted", _default_message_response)
    @api.response(
        401,
        "token_not_found\ntoken_invalid\nunauthorized_user",
        _default_message_response,
    )
    @api.response(404, "table_not_found\ndatabase_not_found", _default_message_response)
    @token_required()
    def delete(
        self, table_id: int, database_id: int, current_user: User
    ) -> tuple[dict[str, str], int]:
        """Delete a table"""
        delete_table(
            table_id=table_id, database_id=database_id, current_user=current_user
        )
        return {"message": "table_deleted"}, 200


@api.route("/<int:database_id>/table/<int:table_id>/columns")
class TableColumns(Resource):
    @api.doc("Get columns names each table")
    @api.marshal_with(
        _table_columns,
        code=200,
        description="get_database_columns_names",
    )
    @api.response(
        401,
        "token_not_found\ntoken_invalid\nunauthorized_user",
        _default_message_response,
    )
    @api.response(404, "database_not_found", _default_message_response)
    @api.response(
        409, "database_not_conected\noutdated_table", _default_message_response
    )
    @api.response(500, "internal_error_getting_column_names", _default_message_response)
    @token_required()
    def get(self, database_id: int, table_id: int, current_user: User):
        """Get columns names each table"""
        return get_table_columns(
            database_id=database_id,
            table_id=table_id,
            current_user=current_user,
        )


@api.route("/<int:database_id>/table/<int:table_id>/sensitive_columns")
class TableSensitiveColumns(Resource):
    @api.doc("Get sensitive columns names each table")
    @api.marshal_with(
        _table_sensitive_columns,
        code=200,
        description="get_database_sensitive_columns_names",
    )
    @api.response(
        401,
        "token_not_found\ntoken_invalid\nunauthorized_user",
        _default_message_response,
    )
    @api.response(404, "database_not_found", _default_message_response)
    @api.response(
        409, "database_not_conected\noutdated_table", _default_message_response
    )
    @api.response(
        500, "internal_error_getting_sensitive_column_names", _default_message_response
    )
    @token_required()
    def get(self, database_id: int, table_id: int, current_user: User):
        """Get sensitive columns names each table"""
        return get_route_sensitive_columns(
            database_id=database_id,
            table_id=table_id,
            current_user=current_user,
        )
