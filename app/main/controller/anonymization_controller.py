from flask import request
from flask_restx import Resource

from app.main.config import Config
from app.main.service import (
    anonymization_database,
    anonymization_database_rows,
    jwt_user_required,
)
from app.main.util import AnonymizationDTO, DefaultResponsesDTO

anonymization_ns = AnonymizationDTO.api
api = anonymization_ns

_anonymization_database = AnonymizationDTO.database_anonymization
_anonymization_database_rows = AnonymizationDTO.database_rows_anonymization

_default_message_response = DefaultResponsesDTO.message_response
_validation_error_response = DefaultResponsesDTO.validation_error

_CONTENT_PER_PAGE = Config.CONTENT_PER_PAGE
_DEFAULT_CONTENT_PER_PAGE = Config.DEFAULT_CONTENT_PER_PAGE


@api.route("/database/<int:database_id>")
class DatabaseAnonymization(Resource):
    @api.doc("Anonymize database")
    @api.expect(_anonymization_database, validate=True)
    @api.response(201, "anonymized_database", _default_message_response)
    @api.response(400, "Input payload validation failed", _validation_error_response)
    @api.response(401, "token_not_found\ntoken_invalid", _default_message_response)
    @jwt_user_required
    def post(self, database_id, current_user) -> tuple[dict[str, str], int]:
        """Create a new anonymization record"""
        data = request.json
        anonymization_database(
            database_id=database_id, data=data, current_user=current_user
        )
        return {"message": "anonymized_database"}, 201


@api.route("/database_rows/<int:database_id>")
class DatabaseRowsAnonymization(Resource):
    @api.doc("Anonymize database rows")
    @api.expect(_anonymization_database_rows, validate=True)
    @api.response(200, "database_rows_anonymized", _default_message_response)
    @api.response(400, "Input payload validation failed", _default_message_response)
    @api.response(401, "token_not_found\ntoken_invalid", _default_message_response)
    @jwt_user_required
    def post(self, database_id, current_user) -> tuple[dict[str, str], int]:
        """Anonymize database rows"""
        data = request.json
        anonymization_database_rows(
            database_id=database_id, data=data, current_user=current_user
        )
        return {"message": "database_rows_anonymized"}, 200
