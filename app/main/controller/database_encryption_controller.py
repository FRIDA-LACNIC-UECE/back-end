from flask import request
from flask_restx import Resource

from app.main.service import encrypt_database, jwt_user_required
from app.main.util import DatabaseEncryptionDTO, DefaultResponsesDTO

database_encryption_ns = DatabaseEncryptionDTO.api
api = database_encryption_ns
_database_encryption = DatabaseEncryptionDTO.database_encryption

_default_message_response = DefaultResponsesDTO.message_response


@api.route("")
class DatabaseEncryption(Resource):
    @api.doc("Encrypt database")
    @api.expect(_database_encryption, validate=True)
    @api.response(200, "database_encrypted", _default_message_response)
    @api.response(400, "Input payload validation failed", _default_message_response)
    @api.response(401, "token_not_found\ntoken_invalid", _default_message_response)
    @jwt_user_required
    def post(self, current_user) -> tuple[dict[str, str], int]:
        """Encrypt database"""
        data = request.json
        data["user_id"] = current_user.id
        encrypt_database(data=data)
        return {"message": "database_encrypted"}, 200
