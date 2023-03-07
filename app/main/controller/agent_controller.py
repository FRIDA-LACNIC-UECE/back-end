from flask import request
from flask_restx import Resource

from app.main.service import jwt_user_required, show_rows_hash
from app.main.util import DefaultResponsesDTO, SseDTO

agent_ns = SseDTO.api
api = agent_ns

_show_row_hash_list = SseDTO.show_row_hash_list

_default_message_response = DefaultResponsesDTO.message_response


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
    @api.response(400, "Input payload validation failed", _default_message_response)
    @api.response(401, "token_not_found\ntoken_invalid", _default_message_response)
    @api.marshal_with(_show_row_hash_list, code=200, description="databases_list")
    @jwt_user_required
    def get(self, database_id, current_user) -> tuple[dict[str, str], int]:
        """List all row hash of cloud database"""
        params = request.args
        return show_rows_hash(
            params=params, database_id=database_id, current_user=current_user
        )
