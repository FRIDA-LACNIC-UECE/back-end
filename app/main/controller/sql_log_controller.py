from flask import request
from flask_restx import Resource

from app.main.config import Config
from app.main.service import (
    delete_sql_log,
    get_sql_log_by_id,
    get_sql_logs,
    save_new_sql_log,
    token_required,
    update_sql_log,
)
from app.main.util import DefaultResponsesDTO
from app.main.util.sql_log_dto import SqlLogDTO
from app.main.model import User

sql_log_ns = SqlLogDTO.api
api = sql_log_ns

_sql_log_put = SqlLogDTO.sql_log_put
_sql_log_post = SqlLogDTO.sql_log_post
_sql_log_list = SqlLogDTO.sql_log_list
_sql_log_response = SqlLogDTO.sql_log_response

_default_message_response = DefaultResponsesDTO.message_response
_validation_error_response = DefaultResponsesDTO.validation_error

_CONTENT_PER_PAGE = Config.CONTENT_PER_PAGE
_DEFAULT_CONTENT_PER_PAGE = Config.DEFAULT_CONTENT_PER_PAGE


@api.route("/database/<int:database_id>")
class SqlLog(Resource):
    @api.doc(
        "List all registered sql logs",
        params={
            "page": {"description": "Page number", "default": 1, "type": int},
            "per_page": {
                "description": "Items per page",
                "default": _DEFAULT_CONTENT_PER_PAGE,
                "enum": _CONTENT_PER_PAGE,
                "type": int,
            },
            "sql_command": {"description": "SQL command", "type": str},
        },
        description=f"List all registered sql logs with pagination. {_DEFAULT_CONTENT_PER_PAGE} sql logs per page.",
    )
    @api.marshal_with(_sql_log_list, code=200, description="_sql_log_list")
    @token_required()
    def get(self, database_id: int, current_user: User):
        """List all registered sql logs"""
        params = request.args
        return get_sql_logs(
            database_id=database_id, params=params, current_user=current_user
        )

    @api.doc("Create a new sql log")
    @api.expect(_sql_log_post, validate=True)
    @api.response(201, "sql_log_created", _default_message_response)
    @api.response(400, "Input payload validation failed", _validation_error_response)
    @token_required()
    def post(self, database_id: int, current_user: User) -> tuple[dict[str, str], int]:
        """Create a new sql log"""
        data = request.json
        save_new_sql_log(database_id=database_id, data=data, current_user=current_user)
        return {"message": "sql_log_created"}, 201


@api.route("/<int:sql_log_id>")
class SqlLogById(Resource):
    @api.doc("Get sql log by id")
    @api.marshal_with(_sql_log_response, code=200, description="sql_log_info")
    @api.response(404, "sql_log_not_found", _default_message_response)
    @token_required(admin_privileges_required=True, return_user=False)
    def get(self, sql_log_id: int):
        """Get sql log by id"""
        return get_sql_log_by_id(sql_log_id=sql_log_id)

    @api.doc("Update a sql log")
    @api.expect(_sql_log_put, validate=True)
    @api.response(200, "sql_log_updated", _default_message_response)
    @api.response(400, "Input payload validation failed", _validation_error_response)
    @api.response(404, "sql_log_not_found", _default_message_response)
    @token_required(admin_privileges_required=True, return_user=False)
    def put(self, sql_log_id):
        """Update a sql log"""
        data = request.json
        update_sql_log(sql_log_id=sql_log_id, data=data)
        return {"message": "sql_log_updated"}, 200

    @api.doc("Delete a sql log")
    @api.response(200, "sql_log_deleted", _default_message_response)
    @api.response(404, "sql_log_not_found", _default_message_response)
    @token_required(admin_privileges_required=True, return_user=False)
    def delete(self, sql_log_id: int) -> tuple[dict[str, str], int]:
        """Delete a sql log"""
        delete_sql_log(sql_log_id=sql_log_id)
        return {"message": "sql_log_deleted"}, 200
