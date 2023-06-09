from flask import request
from flask_restx import Resource

from app.main.config import Config
from app.main.model import User
from app.main.service import (
    delete_table,
    get_table_by_id,
    get_tables,
    jwt_admin_required,
    jwt_user_required,
    save_new_table,
    update_table,
)
from app.main.util import DatabaseDTO, DefaultResponsesDTO, TableDTO

database_ns = DatabaseDTO.api
api = database_ns


_table_put = TableDTO.table_put
_table_post = TableDTO.table_post
_table_list = TableDTO.table_list
_table_response = TableDTO.table_response

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
    @api.response(404, "table_not_found\ndatabase_not_found", _default_message_response)
    @api.response(
        401,
        "token_not_found\ntoken_invalid\nunauthorized_user",
        _default_message_response,
    )
    @jwt_user_required
    def get(self, database_id, current_user):
        """list all registred tables"""
        params = request.args
        return get_tables(
            params=params, database_id=database_id, current_user=current_user
        )

    @api.doc("Create a new table")
    @api.expect(_table_post, validate=True)
    @api.response(201, "table_created", _default_message_response)
    @api.response(400, "Input payload validation failed", _default_message_response)
    @api.response(404, "database_not_found", _default_message_response)
    @api.response(
        401,
        "token_not_found\ntoken_invalid\nunauthorized_user",
        _default_message_response,
    )
    @api.response(401, "token_not_found\ntoken_invalid", _default_message_response)
    @jwt_user_required
    def post(self, database_id, current_user) -> tuple[dict[str, str], int]:
        """Create a new table"""
        data = request.json
        save_new_table(data=data, database_id=database_id, current_user=current_user)
        return {"message": "table_created"}, 201


@api.route("/<int:database_id>/table/<int:table_id>")
class TableById(Resource):
    @api.doc("Get table by id")
    @api.marshal_with(_table_response, code=200, description="table_info")
    @api.response(404, "table_not_found\ndatabase_not_found", _default_message_response)
    @api.response(
        401,
        "token_not_found\ntoken_invalid\nunauthorized_user",
        _default_message_response,
    )
    @jwt_user_required
    def get(self, table_id: int, database_id: int, current_user: User):
        """Get table by id"""
        return get_table_by_id(
            table_id=table_id, database_id=database_id, current_user=current_user
        )

    @api.doc("Update a table")
    @api.expect(_table_put, validate=True)
    @api.response(200, "table_updated", _default_message_response)
    @api.response(400, "Input payload validation failed", _validation_error_response)
    @api.response(404, "table_not_found\ndatabase_not_found", _default_message_response)
    @api.response(
        401,
        "token_not_found\ntoken_invalid\nunauthorized_user",
        _default_message_response,
    )
    @jwt_user_required
    def put(self, table_id: int, database_id: int, current_user: User):
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
    @api.response(404, "table_not_found\ndatabase_not_found", _default_message_response)
    @api.response(
        401,
        "token_not_found\ntoken_invalid\nunauthorized_user",
        _default_message_response,
    )
    @jwt_user_required
    def delete(
        self, table_id: int, database_id: int, current_user: User
    ) -> tuple[dict[str, str], int]:
        """Delete a table"""
        delete_table(
            table_id=table_id, database_id=database_id, current_user=current_user
        )
        return {"message": "table_deleted"}, 200
