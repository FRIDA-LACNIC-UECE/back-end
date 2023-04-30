from flask import request
from flask_restx import Resource

from app.main.config import Config
from app.main.service import (
    get_anonymization_records,
    jwt_admin_required,
    jwt_user_required,
    save_new_anonymization_record,
)
from app.main.util import AnonymizationRecordDTO, DefaultResponsesDTO

anonymization_record_ns = AnonymizationRecordDTO.api
api = anonymization_record_ns

_anonymization_record_post = AnonymizationRecordDTO.anonymization_record_post
_anonymization_record_put = AnonymizationRecordDTO.anonymization_record_put
_anonymization_record_response = AnonymizationRecordDTO.anonymization_record_response
_anonymization_record_list = AnonymizationRecordDTO.anonymization_record_list

_default_message_response = DefaultResponsesDTO.message_response
_validation_error_response = DefaultResponsesDTO.validation_error

_CONTENT_PER_PAGE = Config.CONTENT_PER_PAGE
_DEFAULT_CONTENT_PER_PAGE = Config.DEFAULT_CONTENT_PER_PAGE


@api.route("")
class Database(Resource):
    @api.doc(
        "List all registered anonymization records of each user",
        params={
            "page": {"description": "Page number", "default": 1, "type": int},
            "per_page": {
                "description": "Items per page",
                "default": _DEFAULT_CONTENT_PER_PAGE,
                "enum": _CONTENT_PER_PAGE,
                "type": int,
            },
            "database_id": {"description": "Database id", "type": int},
        },
        description=f"List all registered anonymization records of each user with pagination. {_DEFAULT_CONTENT_PER_PAGE} anonymization records per page.",
    )
    @api.marshal_with(
        _anonymization_record_list, code=200, description="anonymization_record_list"
    )
    @api.response(401, "token_not_found\ntoken_invalid", _default_message_response)
    @jwt_admin_required
    def get(self):
        """List all registered anonymization records of each user"""
        params = request.args
        return get_anonymization_records(params=params)

    @api.doc("Create a new anonymization record")
    @api.expect(_anonymization_record_post, validate=True)
    @api.response(201, "anonymization_record_created", _default_message_response)
    @api.response(400, "Input payload validation failed", _default_message_response)
    @api.response(401, "token_not_found\ntoken_invalid", _default_message_response)
    @jwt_user_required
    def post(self, current_user) -> tuple[dict[str, str], int]:
        """Create a new anonymization record"""
        data = request.json
        save_new_anonymization_record(current_user=current_user, data=data)
        return {"message": "anonymization_record_created"}, 201
