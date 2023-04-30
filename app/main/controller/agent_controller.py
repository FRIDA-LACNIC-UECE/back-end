from flask import request
from flask_restx import Resource

from app.main.service import (
    decrypt_row,
    include_hash_rows,
    jwt_user_required,
    process_deletions,
    process_updates,
    show_rows_hash,
)
from app.main.util import AgentDTO, DefaultResponsesDTO

agent_ns = AgentDTO.api
api = agent_ns

_decrypt_row = AgentDTO.decrypt_row
_show_row_hash_list = AgentDTO.show_row_hash_list
_include_hash_rows = AgentDTO.include_hash_rows
_process_deletions = AgentDTO.process_deletions
_process_updates = AgentDTO.process_updates

_default_message_response = DefaultResponsesDTO.message_response
_validation_error_response = DefaultResponsesDTO.validation_error


@api.route("/show_row_hash/<int:database_id>")
class ShowRowsHash(Resource):
    @api.doc(
        "List all row hash of cloud database",
        params={
            "table_name": {
                "required": True,
                "description": "Database table name",
                "type": str,
            },
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
    @api.response(401, "token_not_found\ntoken_invalid", _default_message_response)
    @api.marshal_with(_show_row_hash_list, code=200, description="databases_list")
    @jwt_user_required
    def get(self, database_id, current_user) -> tuple[dict[str, str], int]:
        """List all row hash of cloud database"""
        params = request.args
        return show_rows_hash(
            params=params, database_id=database_id, current_user=current_user
        )


@api.route("/decrypt_row/<int:database_id>")
class DecryptRow(Resource):
    @api.doc("Decrypt database row")
    @api.expect(_decrypt_row, validate=True)
    @api.response(200, "row_decrypted", _default_message_response)
    @api.response(400, "Input payload validation failed", _validation_error_response)
    @api.response(401, "token_not_found\ntoken_invalid", _default_message_response)
    @jwt_user_required
    def post(self, database_id, current_user) -> tuple[dict[str, str], int]:
        """Decrypt database row"""
        data = request.json
        decrypted_row = decrypt_row(
            database_id=database_id, data=data, current_user=current_user
        )
        return {"decrypted_row": decrypted_row}, 200


@api.route("/include_hash_rows/<int:database_id>")
class IncludeHasRows(Resource):
    @api.doc("Include hash of new rows")
    @api.expect(_include_hash_rows, validate=True)
    @api.response(200, "hash_rows_included", _default_message_response)
    @api.response(400, "Input payload validation failed", _validation_error_response)
    @api.response(401, "token_not_found\ntoken_invalid", _default_message_response)
    @jwt_user_required
    def post(self, database_id, current_user) -> tuple[dict[str, str], int]:
        """Include hash of new rows"""
        data = request.json
        include_hash_rows(
            database_id=database_id,
            data=data,
            current_user=current_user,
        )
        return {"message": "hash_rows_included"}, 200


@api.route("/process_updates/<int:database_id>")
class ProcessUpdate(Resource):
    @api.doc("Process updates on database")
    @api.expect(_process_updates, validate=True)
    @api.response(200, "updates_processed", _default_message_response)
    @api.response(400, "Input payload validation failed", _validation_error_response)
    @api.response(401, "token_not_found\ntoken_invalid", _default_message_response)
    @jwt_user_required
    def post(self, database_id, current_user) -> tuple[dict[str, str], int]:
        """Process updates on database"""
        data = request.json
        process_updates(database_id=database_id, data=data, current_user=current_user)
        return {"message": "updates_processed"}, 200


@api.route("/process_deletions/<int:database_id>")
class ProcessDeletions(Resource):
    @api.doc("Process deletions on database")
    @api.expect(_process_deletions, validate=True)
    @api.response(200, "deletions_processed", _default_message_response)
    @api.response(400, "Input payload validation failed", _validation_error_response)
    @api.response(401, "token_not_found\ntoken_invalid", _default_message_response)
    @jwt_user_required
    def post(self, database_id, current_user) -> tuple[dict[str, str], int]:
        """Process deletions on database"""
        data = request.json
        process_deletions(database_id=database_id, current_user=current_user, data=data)
        return {"message": "deletions_processed"}, 200
