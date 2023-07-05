from flask import request
from flask_restx import Resource

from app.main.config import Config
from app.main.model import User
from app.main.service import (
    delete_column,
    get_column_by_id,
    get_columns,
    save_new_column,
    token_required,
    update_column,
)
from app.main.util import ColumnDTO, DatabaseDTO, DefaultResponsesDTO

database_ns = DatabaseDTO.api
api = database_ns


_column_update = ColumnDTO.column_update
_column_post = ColumnDTO.column_post
_column_list = ColumnDTO.column_list
_column_response = ColumnDTO.column_response

_default_message_response = DefaultResponsesDTO.message_response
_validation_error_response = DefaultResponsesDTO.validation_error

_CONTENT_PER_PAGE = Config.CONTENT_PER_PAGE
_DEFAULT_CONTENT_PER_PAGE = Config.DEFAULT_CONTENT_PER_PAGE


@api.route("/<database_id>/table/<table_id>/column")
class Column(Resource):
    @api.doc(
        "List all registered columns",
        params={
            "page": {"description": "Page number", "default": 1, "type": int},
            "per_page": {
                "description": "Items per page",
                "default": _DEFAULT_CONTENT_PER_PAGE,
                "enum": _CONTENT_PER_PAGE,
                "type": int,
            },
            "anonymization_type_id": {
                "description": "Anonymization type id",
                "type": int,
            },
            "column_name": {"description": "column name", "type": str},
        },
        description=f"List all registered columns with pagination. {_DEFAULT_CONTENT_PER_PAGE} columns per page.",
    )
    @api.marshal_with(_column_list, code=200, description="columns_list")
    @api.response(
        401,
        "token_not_found\ntoken_invalid\nunauthorized_user",
        _default_message_response,
    )
    @api.response(404, "table_not_found\ndatabase_not_found", _default_message_response)
    @token_required()
    def get(self, table_id: int, database_id: int, current_user: User):
        """List all registred columns"""
        params = request.args
        return get_columns(
            params=params,
            database_id=database_id,
            table_id=table_id,
            current_user=current_user,
        )

    @api.doc("Create a new column")
    @api.expect(_column_post, validate=True)
    @api.response(201, "column_created", _default_message_response)
    @api.response(400, "Input payload validation failed", _validation_error_response)
    @api.response(
        401,
        "token_not_found\ntoken_invalid\nunauthorized_user",
        _default_message_response,
    )
    @api.response(404, "table_not_found\ndatabase_not_found", _default_message_response)
    @api.response(
        409,
        "column_already_exists\ncolumn_not_exists_on_client_database",
        _default_message_response,
    )
    @token_required()
    def post(self, database_id, table_id, current_user) -> tuple[dict[str, str], int]:
        """Create a new column"""
        data = request.json
        save_new_column(
            data=data,
            database_id=database_id,
            table_id=table_id,
            current_user=current_user,
        )
        return {"message": "column_created"}, 201


@api.route("/<database_id>/table/<table_id>/column/<column_id>")
class ColumnById(Resource):
    @api.doc("Get column by id")
    @api.marshal_with(_column_response, code=200, description="column_info")
    @api.response(
        404,
        "column_not_found\ntable_not_found\ndatabase_not_found",
        _default_message_response,
    )
    @token_required()
    def get(self, column_id: int, database_id: int, table_id: int, current_user: User):
        """Get column by id"""
        return get_column_by_id(
            column_id=column_id,
            table_id=table_id,
            database_id=database_id,
            current_user=current_user,
        )

    @api.doc("Update a column")
    @api.expect(_column_update, validate=True)
    @api.response(200, "column_updated", _default_message_response)
    @api.response(400, "Input payload validation failed", _validation_error_response)
    @api.response(404, "column_not_found", _default_message_response)
    @token_required()
    def put(self, column_id: int, database_id: int, table_id: int, current_user: User):
        """Update a column"""
        data = request.json
        update_column(
            column_id=column_id,
            table_id=table_id,
            database_id=database_id,
            data=data,
            current_user=current_user,
        )
        return {"message": "column_updated"}, 200

    @api.doc("Delete a column")
    @api.response(200, "column_deleted", _default_message_response)
    @api.response(404, "column_not_found", _default_message_response)
    @token_required()
    def delete(
        self, column_id: int, table_id: int, database_id: int, current_user: User
    ) -> tuple[dict[str, str], int]:
        """Delete a column"""
        delete_column(
            column_id=column_id,
            table_id=table_id,
            database_id=database_id,
            current_user=current_user,
        )
        return {"message": "column_deleted"}, 200
