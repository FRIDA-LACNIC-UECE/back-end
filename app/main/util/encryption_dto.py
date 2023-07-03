from flask_restx import Namespace, fields

from app.main.util.custom_field.dictionary_field import Dictionary

SEARCH_TYPE = ["primary_key", "row_hash"]


class EncryptionDTO:
    api = Namespace("encryption", description="Encryption related operations")

    encryption_database_id = {
        "database_id": fields.Integer(
            required=True, description="database id to encryption"
        ),
    }

    encryption_table_id = {
        "table_id": fields.Integer(required=True, description="table id to encryption"),
    }

    encryption_rows_to_encrypt = {
        "rows_to_encrypt": fields.List(
            Dictionary(attribute="rows_to_encrypt", description="row to encrypt"),
            description="rows to encrypt",
        ),
    }

    encryption_update_database = {
        "update_database": fields.Boolean(description="update database flag"),
    }

    encryption_search_type = {
        "search_type": fields.Boolean(description="search type flag"),
    }

    encryption_search_value = {
        "search_value": fields.String(resquired=True, description="search value"),
    }

    encryption_database_rows = api.model(
        "encryption_database_rows",
        encryption_database_id
        | encryption_table_id
        | encryption_rows_to_encrypt
        | encryption_update_database,
    )

    encrypt_database_table = api.model(
        "encryption_database", encryption_database_id | encryption_table_id
    )

    encryption_decrypt_row = api.model(
        "decrypt_row",
        encryption_database_id
        | encryption_table_id
        | encryption_search_type
        | encryption_search_value,
    )
