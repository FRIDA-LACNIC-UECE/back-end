from flask import request
from flask_restx import Resource

from app.main.model import User
from app.main.service import (
    decrypt_row,
    encrypt_database_row,
    encrypt_database_table,
    get_encryption_progress,
    token_required,
)
from app.main.util import DefaultResponsesDTO, EncryptionDTO

encryption_ns = EncryptionDTO.api
api = encryption_ns

_encryption_database_rows = EncryptionDTO.encryption_database_rows
_encryption_progress = EncryptionDTO.encryption_progress
_encryption_decrypt_row = EncryptionDTO.encryption_decrypt_row

_default_message_response = DefaultResponsesDTO.message_response
_validation_error_response = DefaultResponsesDTO.validation_error


@api.route("/database/<int:database_id>/table/<int:table_id>/database_rows")
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
    @api.response(404, "database_not_found\ntable_not_found", _default_message_response)
    @api.response(409, "database_not_conected", _default_message_response)
    @api.response(500, "database_rows_not_encrypted", _default_message_response)
    @token_required()
    def post(
        self, database_id: int, table_id: int, current_user: User
    ) -> tuple[dict[str, str], int]:
        """Encrypt database rows"""
        data = request.json
        encrypt_database_row(
            database_id=database_id,
            table_id=table_id,
            data=data,
            current_user=current_user,
        )
        return {"message": "database_rows_encrypted"}, 200


@api.route("/database/<int:database_id>/table/<int:table_id>")
class DatabaseTableEncryption(Resource):
    @api.doc("Encrypt database table")
    @api.response(200, "table_encrypted", _default_message_response)
    @api.response(400, "Input payload validation failed", _validation_error_response)
    @api.response(
        401,
        "token_not_found\ntoken_invalid\nunauthorized_user",
        _default_message_response,
    )
    @api.response(404, "database_not_found\ntable_not_found", _default_message_response)
    @api.response(409, "database_not_conected", _default_message_response)
    @api.response(500, "table_not_encrypted", _default_message_response)
    @token_required()
    def post(
        self, database_id: int, table_id: int, current_user: User
    ) -> tuple[dict[str, str], int]:
        """Encrypt database table"""
        encrypt_database_table(
            database_id=database_id,
            table_id=table_id,
            current_user=current_user,
        )
        return {"message": "table_encrypted"}, 200


@api.route("/database/<int:database_id>/table/<int:table_id>/progress")
class EncryptionProgress(Resource):
    @api.doc("Get encryption progress")
    @api.marshal_with(
        _encryption_progress, code=200, description="encryption_progress_info"
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
        """Get encryption progress"""
        return get_encryption_progress(
            database_id=database_id,
            table_id=table_id,
            current_user=current_user,
        )


@api.route("/database/<int:database_id>/table/<int:table_id>/decrypt_row")
class DecryptDatabaseRow(Resource):
    @api.doc("Decrypt database row")
    @api.expect(_encryption_decrypt_row, validate=True)
    @api.response(200, "row_decrypted", _default_message_response)
    @api.response(400, "Input payload validation failed", _validation_error_response)
    @api.response(
        401,
        "token_not_found\ntoken_invalid\nunauthorized_user",
        _default_message_response,
    )
    @api.response(
        404,
        "database_not_found\ntable_not_found\nrow_not_found",
        _default_message_response,
    )
    @api.response(409, "database_not_conected", _default_message_response)
    @api.response(500, "row_not_decrypted", _default_message_response)
    @token_required()
    def post(
        self, database_id: int, table_id: int, current_user: User
    ) -> tuple[dict[str, any], int]:
        """Decrypt database row"""
        data = request.json
        decrypted_row = decrypt_row(
            database_id=database_id,
            table_id=table_id,
            data=data,
            current_user=current_user,
        )
        return {"decrypted_row": decrypted_row}, 200
