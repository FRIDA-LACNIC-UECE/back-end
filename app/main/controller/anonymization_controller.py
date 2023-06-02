from flask import request
from flask_restx import Resource

from app.main.config import Config
from app.main.service import (
    anonymization_database_rows,
    anonymization_table,
    token_required,
)
from app.main.util import AnonymizationDTO, DefaultResponsesDTO

anonymization_ns = AnonymizationDTO.api
api = anonymization_ns

_table_anonymization = AnonymizationDTO.table_anonymization
_anonymization_database_rows = AnonymizationDTO.database_rows_anonymization

_default_message_response = DefaultResponsesDTO.message_response
_validation_error_response = DefaultResponsesDTO.validation_error


@api.route("/table/<int:database_id>")
class DatabaseTableAnonymization(Resource):
    @api.doc("Anonymize database table")
    @api.expect(_table_anonymization, validate=True)
    @api.response(200, "table_anonymized", _default_message_response)
    @api.response(400, "Input payload validation failed", _validation_error_response)
    @api.response(401, "token_not_found\ntoken_invalid", _default_message_response)
    @api.response(500, "table_not_anonymized", _default_message_response)
    @token_required()
    def post(self, database_id, current_user) -> tuple[dict[str, str], int]:
        """Anonymize database table"""
        data = request.json
        anonymization_table(
            database_id=database_id, data=data, current_user=current_user
        )
        return {"message": "table_anonymized"}, 200


@api.route("/database_rows/<int:database_id>")
class DatabaseRowsAnonymization(Resource):
    @api.doc("Anonymize database rows")
    @api.expect(_anonymization_database_rows, validate=True)
    @api.response(200, "database_rows_anonymized", _default_message_response)
    @api.response(400, "Input payload validation failed", _validation_error_response)
    @api.response(401, "token_not_found\ntoken_invalid", _validation_error_response)
    @api.response(500, "database_rows_not_anonymized", _default_message_response)
    @token_required()
    def post(self, database_id, current_user) -> tuple[dict[str, str], int]:
        """Anonymize database rows"""
        data = request.json
        anonymization_database_rows(
            database_id=database_id, data=data, current_user=current_user
        )
        return {"message": "database_rows_anonymized"}, 200
