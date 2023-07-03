from flask import request
from flask_restx import Resource

from app.main.service import (
    decrypt_row,
    encrypt_database_row,
    encrypt_database_table,
    token_required,
)
from app.main.util import DefaultResponsesDTO, EncryptionDTO

encryption_ns = EncryptionDTO.api
api = encryption_ns
_encrypt_database_table = EncryptionDTO.encrypt_database_table
_encryption_database_rows = EncryptionDTO.encryption_database_rows
_encryption_decrypt_row = EncryptionDTO.encryption_decrypt_row

_default_message_response = DefaultResponsesDTO.message_response
_validation_error_response = DefaultResponsesDTO.validation_error


@api.route("/table")
class DatabaseTableEncryption(Resource):
    @api.doc("Encrypt database table")
    @api.expect(_encrypt_database_table, validate=True)
    @api.response(200, "table_encrypted", _default_message_response)
    @api.response(400, "Input payload validation failed", _validation_error_response)
    @api.response(
        401,
        "token_not_found\ntoken_invalid\nunauthorized_user",
        _default_message_response,
    )
    @api.response(404, "database_not_found\ntable_not_found", _default_message_response)
    @api.response(500, "table_not_encrypted", _default_message_response)
    @token_required()
    def post(self, current_user) -> tuple[dict[str, str], int]:
        """Encrypt database table"""
        data = request.json
        encrypt_database_table(data=data, current_user=current_user)
        return {"message": "table_encrypted"}, 200


@api.route("/database_rows")
class DatabaseRowsEncryption(Resource):
    @api.doc("Encrypt database rows")
    @api.expect(_encryption_database_rows, validate=True)
    @api.response(200, "database_rows_encrypted", _default_message_response)
    @api.response(400, "Input payload validation failed", _validation_error_response)
    @api.response(
        401,
        "token_not_found\ntoken_invalid\nunauthorized_user",
        _default_message_response,
    )
    @api.response(500, "database_rows_not_encrypted", _default_message_response)
    @token_required()
    def post(self, current_user) -> tuple[dict[str, str], int]:
        """Encrypt database rows"""
        data = request.json
        encrypt_database_row(data=data, current_user=current_user)
        return {"message": "database_rows_encrypted"}, 200


@api.route("/decrypt_row/<int:database_id>")
class DecryptRow(Resource):
    @api.doc("Decrypt database row")
    @api.expect(_encryption_decrypt_row, validate=True)
    @api.response(200, "row_decrypted", _default_message_response)
    @api.response(400, "Input payload validation failed", _validation_error_response)
    @api.response(401, "token_not_found\ntoken_invalid", _default_message_response)
    @token_required()
    def post(self, database_id, current_user) -> tuple[dict[str, str], int]:
        """Decrypt database row"""
        data = request.json
        decrypted_row = decrypt_row(
            database_id=database_id, data=data, current_user=current_user
        )
        return {"decrypted_row": decrypted_row}, 200
