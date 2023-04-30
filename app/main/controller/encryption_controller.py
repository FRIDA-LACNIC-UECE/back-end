from flask import request
from flask_restx import Resource

from app.main.service import encrypt_database, encrypt_database_row, jwt_user_required
from app.main.util import DefaultResponsesDTO, EncryptionDTO

encryption_ns = EncryptionDTO.api
api = encryption_ns
_database_encryption = EncryptionDTO.database_encryption
_database_rows_encryption = EncryptionDTO.database_rows_encryption

_default_message_response = DefaultResponsesDTO.message_response


@api.route("/database/<int:database_id>")
class DatabaseEncryption(Resource):
    @api.doc("Encrypt database")
    @api.expect(_database_encryption, validate=True)
    @api.response(200, "database_encrypted", _default_message_response)
    @api.response(400, "Input payload validation failed", _default_message_response)
    @api.response(401, "token_not_found\ntoken_invalid", _default_message_response)
    @jwt_user_required
    def post(self, database_id, current_user) -> tuple[dict[str, str], int]:
        """Encrypt database"""
        data = request.json
        encrypt_database(database_id=database_id, data=data, current_user=current_user)
        return {"message": "database_encrypted"}, 200


@api.route("/database_rows/<int:database_id>")
class DatabaseRowsEncryption(Resource):
    @api.doc("Encrypt database rows")
    @api.expect(_database_rows_encryption, validate=True)
    @api.response(200, "database_rows_encrypted", _default_message_response)
    @api.response(400, "Input payload validation failed", _default_message_response)
    @api.response(401, "token_not_found\ntoken_invalid", _default_message_response)
    @jwt_user_required
    def post(self, database_id, current_user) -> tuple[dict[str, str], int]:
        """Encrypt database rows"""
        data = request.json
        encrypt_database_row(
            database_id=database_id, data=data, current_user=current_user
        )
        return {"message": "database_rows_encrypted"}, 200
