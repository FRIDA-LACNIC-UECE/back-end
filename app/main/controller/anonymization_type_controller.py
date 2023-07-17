from flask import request
from flask_restx import Resource

from app.main.config import Config
from app.main.service import (
    delete_anonymization_type,
    get_anonymization_type_by_id,
    get_anonymization_types,
    save_new_anonymization_type,
    token_required,
    update_anonymization_type,
)
from app.main.util import AnonymizationTypeDTO, DefaultResponsesDTO

anonymization_type_ns = AnonymizationTypeDTO.api
api = anonymization_type_ns
_anonymization_type_post = AnonymizationTypeDTO.anonymization_type_post
_anonymization_type_put = AnonymizationTypeDTO.anonymization_type_put
_anonymization_type_response = AnonymizationTypeDTO.anonymization_type_response
_anonymization_type_list = AnonymizationTypeDTO.anonymization_type_list

_default_message_response = DefaultResponsesDTO.message_response
_validation_error_response = DefaultResponsesDTO.validation_error

_CONTENT_PER_PAGE = Config.CONTENT_PER_PAGE
_DEFAULT_CONTENT_PER_PAGE = Config.DEFAULT_CONTENT_PER_PAGE


@api.route("")
class AnonymizationType(Resource):
    @api.doc(
        "List all registered anonymization types",
        params={
            "page": {"description": "Page number", "default": 1, "type": int},
            "per_page": {
                "description": "Items per page",
                "default": _DEFAULT_CONTENT_PER_PAGE,
                "enum": _CONTENT_PER_PAGE,
                "type": int,
            },
            "name": {"description": "Anonymization type name", "type": str},
        },
        description=f"List all registered anonymization types with pagination. {_DEFAULT_CONTENT_PER_PAGE} anonymization types per page.",
    )
    @api.marshal_with(
        _anonymization_type_list, code=200, description="anonymization_types_list"
    )
    @api.response(401, "token_not_found\ntoken_invalid", _default_message_response)
    @token_required(return_user=False)
    def get(self) -> tuple[dict[str, any], int]:
        """List all registered anonymization types"""
        params = request.args
        return get_anonymization_types(params=params)

    @api.doc("Create a new anonymization type")
    @api.expect(_anonymization_type_post, validate=True)
    @api.response(201, "anonymization_type_created", _default_message_response)
    @api.response(400, "Input payload validation failed", _validation_error_response)
    @api.response(401, "token_not_found\ntoken_invalid", _default_message_response)
    @api.response(403, "required_administrator_privileges", _default_message_response)
    @api.response(409, "anonymization_type_exists", _default_message_response)
    @token_required(admin_privileges_required=True, return_user=False)
    def post(self) -> tuple[dict[str, str], int]:
        """Create a new anonymization type"""
        data = request.json
        save_new_anonymization_type(data=data)
        return {"message": "anonymization_type_created"}, 201


@api.route("/<int:anonymization_type_id>")
class ValidDatabaseById(Resource):
    @api.doc("Get valid database by id")
    @api.marshal_with(
        _anonymization_type_response, code=200, description="anonymization_type_info"
    )
    @api.response(401, "token_not_found\ntoken_invalid", _default_message_response)
    @api.response(404, "anonymization_type_not_found", _default_message_response)
    @token_required(return_user=False)
    def get(self, anonymization_type_id: int) -> tuple[dict[str, any]]:
        """Get anonymization type by id"""
        return get_anonymization_type_by_id(anonymization_type_id=anonymization_type_id)

    @api.doc("Update a anonymization type")
    @api.expect(_anonymization_type_put, validate=True)
    @api.response(200, "anonymization_type_updated", _default_message_response)
    @api.response(400, "Input payload validation failed", _validation_error_response)
    @api.response(401, "token_not_found\ntoken_invalid", _default_message_response)
    @api.response(403, "required_administrator_privileges", _default_message_response)
    @api.response(404, "anonymization_type_not_found", _default_message_response)
    @api.response(409, "anonymization_type_already_exists", _default_message_response)
    @token_required(admin_privileges_required=True, return_user=False)
    def put(self, anonymization_type_id) -> tuple[dict[str, str], int]:
        """Update a anonymization type"""
        data = request.json
        update_anonymization_type(
            anonymization_type_id=anonymization_type_id, data=data
        )
        return {"message": "anonymization_type_updated"}, 200

    @api.doc("Delete a anonymization type")
    @api.response(200, "anonymization_type_deleted", _default_message_response)
    @api.response(401, "token_not_found\ntoken_invalid", _default_message_response)
    @api.response(403, "required_administrator_privileges", _default_message_response)
    @api.response(404, "anonymization_type_not_found", _default_message_response)
    @token_required(admin_privileges_required=True, return_user=False)
    def delete(self, anonymization_type_id: int) -> tuple[dict[str, str], int]:
        """Delete a anonymization type"""
        delete_anonymization_type(anonymization_type_id=anonymization_type_id)
        return {"message": "anonymization_type_deleted"}, 200
