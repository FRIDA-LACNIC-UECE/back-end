from flask import request
from flask_restx import Resource

from app.main.config import Config
from app.main.service import anonymization_database, jwt_user_required
from app.main.util import AnonymizationDTO, DefaultResponsesDTO

anonymization_ns = AnonymizationDTO.api
api = anonymization_ns

_anonymization_database = AnonymizationDTO.anonymization_database

_default_message_response = DefaultResponsesDTO.message_response
_validation_error_response = DefaultResponsesDTO.validation_error

_CONTENT_PER_PAGE = Config.CONTENT_PER_PAGE
_DEFAULT_CONTENT_PER_PAGE = Config.DEFAULT_CONTENT_PER_PAGE


@api.route("/anonymize_database")
class Database(Resource):
    @api.doc("Anonymize database")
    @api.expect(_anonymization_database, validate=True)
    @api.response(201, "anonymized_database", _default_message_response)
    @api.response(400, "Input payload validation failed", _validation_error_response)
    @api.response(401, "token_not_found\ntoken_invalid", _default_message_response)
    @jwt_user_required
    def post(self, current_user) -> tuple[dict[str, str], int]:
        """Create a new anonymization record"""
        data = request.json
        anonymization_database(current_user=current_user, data=data)
        return {"message": "anonymized_database"}, 201
