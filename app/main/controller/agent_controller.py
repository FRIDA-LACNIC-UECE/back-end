from flask import request
from flask_restx import Resource

from app.main.service import (
    process_deletions,
    process_inserts,
    process_updates,
    show_rows_hash,
    token_required,
)
from app.main.util import AgentDTO, DefaultResponsesDTO
from app.main.model import User

agent_ns = AgentDTO.api
api = agent_ns

_show_row_hash_list = AgentDTO.show_row_hash_list
_process_inserts = AgentDTO.process_inserts
_process_deletions = AgentDTO.process_deletions
_process_updates = AgentDTO.process_updates

_default_message_response = DefaultResponsesDTO.message_response
_validation_error_response = DefaultResponsesDTO.validation_error


@api.route("/show_row_hash/database/<int:database_id>/table/<int:table_id>")
class ShowRowsHash(Resource):
    @api.doc(
        "List all row hash of cloud database",
        params={
            "page": {
                "required": True,
                "description": "Page number",
                "default": 1,
                "type": int,
            },
            "per_page": {
                "required": True,
                "description": "Items per page",
                "type": int,
            },
        },
        description=f"List all row hash of cloud database",
    )
    @api.response(400, "Input payload validation failed", _validation_error_response)
    @api.response(
        401,
        "token_not_found\ntoken_invalid\nunauthorized_user",
        _default_message_response,
    )
    @api.response(
        404, "database_not_found\ntable_not_found", _validation_error_response
    )
    @api.marshal_with(_show_row_hash_list, code=200, description="show_row_hash")
    @token_required()
    def get(
        self, database_id: int, table_id: int, current_user: User
    ) -> tuple[dict[str, str], int]:
        """List all row hash of cloud database"""
        params = request.args
        return show_rows_hash(
            database_id=database_id,
            table_id=table_id,
            params=params,
            current_user=current_user,
        )


@api.route("/process_inserts/database/<int:database_id>/table/<int:table_id>")
class IncludeHasRows(Resource):
    @api.doc("Include hash of new rows")
    @api.expect(_process_inserts, validate=True)
    @api.response(200, "inserts_processed", _default_message_response)
    @api.response(400, "Input payload validation failed", _validation_error_response)
    @api.response(
        401,
        "token_not_found\ntoken_invalid\nunauthorized_user",
        _default_message_response,
    )
    @api.response(500, "inserts_not_processed", _default_message_response)
    @token_required()
    def post(
        self, database_id: int, table_id: int, current_user: User
    ) -> tuple[dict[str, str], int]:
        """Process inserts on database"""
        data = request.json
        process_inserts(
            database_id=database_id,
            table_id=table_id,
            data=data,
            current_user=current_user,
        )
        return {"message": "hash_rows_included"}, 200


@api.route("/process_updates/database/<int:database_id>/table/<int:table_id>")
class ProcessUpdate(Resource):
    @api.doc("Process updates on database")
    @api.expect(_process_updates, validate=True)
    @api.response(200, "updates_processed", _default_message_response)
    @api.response(400, "Input payload validation failed", _validation_error_response)
    @api.response(
        401,
        "token_not_found\ntoken_invalid\nunauthorized_user",
        _default_message_response,
    )
    @api.response(500, "updates_not_processed", _default_message_response)
    @token_required()
    def post(
        self, database_id: int, table_id: int, current_user: User
    ) -> tuple[dict[str, str], int]:
        """Process updates on database"""
        data = request.json
        process_updates(
            database_id=database_id,
            table_id=table_id,
            data=data,
            current_user=current_user,
        )
        return {"message": "updates_processed"}, 200


@api.route("/process_deletions/database/<int:database_id>/table/<int:table_id>")
class ProcessDeletions(Resource):
    @api.doc("Process deletions on database")
    @api.expect(_process_deletions, validate=True)
    @api.response(200, "deletions_processed", _default_message_response)
    @api.response(400, "Input payload validation failed", _validation_error_response)
    @api.response(
        401,
        "token_not_found\ntoken_invalid\nunauthorized_user",
        _default_message_response,
    )
    @api.response(500, "deletes_not_processed", _default_message_response)
    @token_required()
    def post(
        self, database_id: int, table_id: int, current_user: User
    ) -> tuple[dict[str, str], int]:
        """Process deletions on database"""
        data = request.json
        process_deletions(
            database_id=database_id,
            table_id=table_id,
            current_user=current_user,
            data=data,
        )
        return {"message": "deletions_processed"}, 200
