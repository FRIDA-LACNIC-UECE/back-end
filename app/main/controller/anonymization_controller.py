from flask import request
from flask_restx import Resource

from app.main.model import User
from app.main.service import (
    anonymization_database_rows,
    anonymization_table,
    get_anonymization_progress,
    get_remove_anonymization_progress,
    remove_table_anonymizaiton,
    token_required,
)
from app.main.util import AnonymizationDTO, DefaultResponsesDTO

anonymization_ns = AnonymizationDTO.api
api = anonymization_ns

_anonymization_database_rows = AnonymizationDTO.anonymization_database_rows
_anonymization_progress = AnonymizationDTO.anonymization_progress

_default_message_response = DefaultResponsesDTO.message_response
_validation_error_response = DefaultResponsesDTO.validation_error


@api.route("/database/<int:database_id>/table/<int:table_id>/database_rows")
class DatabaseRowsAnonymization(Resource):
    @api.doc("Anonymize database rows")
    @api.expect(_anonymization_database_rows, validate=True)
    @api.response(200, "database_rows_anonymized", _default_message_response)
    @api.response(400, "Input payload validation failed", _validation_error_response)
    @api.response(401, "token_not_found\ntoken_invalid", _validation_error_response)
    @api.response(404, "database_not_found\ntable_not_found", _default_message_response)
    @api.response(409, "database_not_conected", _default_message_response)
    @api.response(500, "database_rows_not_anonymized", _default_message_response)
    @token_required()
    def post(
        self, database_id: int, table_id: int, current_user: User
    ) -> tuple[dict[str, str], int]:
        """Anonymize database rows"""
        data = request.json
        anonymization_database_rows(
            database_id=database_id,
            table_id=table_id,
            data=data,
            current_user=current_user,
        )
        return {"message": "database_rows_anonymized"}, 200


@api.route("/database/<int:database_id>/table/<int:table_id>")
class DatabaseTableAnonymization(Resource):
    @api.doc("Anonymize database table")
    @api.response(200, "table_anonymized", _default_message_response)
    @api.response(400, "Input payload validation failed", _validation_error_response)
    @api.response(401, "token_not_found\ntoken_invalid", _default_message_response)
    @api.response(404, "database_not_found\ntable_not_found", _default_message_response)
    @api.response(409, "database_not_conected", _default_message_response)
    @api.response(500, "table_not_anonymized", _default_message_response)
    @token_required()
    def post(
        self, database_id: int, table_id: int, current_user: User
    ) -> tuple[dict[str, str], int]:
        """Anonymize database table"""
        anonymization_table(
            database_id=database_id,
            table_id=table_id,
            current_user=current_user,
        )
        return {"message": "table_anonymized"}, 200


@api.route("/remove/database/<int:database_id>/table/<int:table_id>")
class RemoveTableAnonymization(Resource):
    @api.doc("Remove table anonymization")
    @api.response(200, "anonymization_removed", _default_message_response)
    @api.response(400, "Input payload validation failed", _validation_error_response)
    @api.response(401, "token_not_found\ntoken_invalid", _default_message_response)
    @api.response(404, "database_not_found\ntable_not_found", _default_message_response)
    @api.response(409, "database_not_conected", _default_message_response)
    @api.response(500, "anonymization_not_removed", _default_message_response)
    @token_required()
    def post(
        self, database_id: int, table_id: int, current_user: User
    ) -> tuple[dict[str, str], int]:
        """Remove table anonymization"""
        remove_table_anonymizaiton(
            database_id=database_id,
            table_id=table_id,
            current_user=current_user,
        )
        return {"message": "anonymization_removed"}, 200


@api.route("/database/<int:database_id>/table/<int:table_id>/progress")
class AnonymizationProgress(Resource):
    @api.doc("Get anonymization progress")
    @api.marshal_with(
        _anonymization_progress, code=200, description="anonymization_progress_info"
    )
    @api.response(
        401,
        "token_not_found\ntoken_invalid\nunauthorized_user",
        _default_message_response,
    )
    @api.response(404, "database_not_found\ntable_not_found", _default_message_response)
    @api.response(409, "database_not_conected", _default_message_response)
    @token_required()
    def get(
        self, database_id: int, table_id: int, current_user: User
    ) -> tuple[dict[str, str], int]:
        """Get anonymization progress"""
        return get_anonymization_progress(
            database_id=database_id,
            table_id=table_id,
            current_user=current_user,
        )


@api.route("/remove/database/<int:database_id>/table/<int:table_id>/progress")
class RemoveTableAnonymizationProgress(Resource):
    @api.doc("Get remove table anonymization progress")
    @api.marshal_with(
        _anonymization_progress,
        code=200,
        description="remove_anonymization_progress_info",
    )
    @api.response(
        401,
        "token_not_found\ntoken_invalid\nunauthorized_user",
        _default_message_response,
    )
    @api.response(404, "database_not_found\ntable_not_found", _default_message_response)
    @api.response(409, "database_not_conected", _default_message_response)
    @token_required()
    def get(
        self, database_id: int, table_id: int, current_user: User
    ) -> tuple[dict[str, str], int]:
        """Get remove table anonymization progress"""
        return get_remove_anonymization_progress(
            database_id=database_id,
            table_id=table_id,
            current_user=current_user,
        )
