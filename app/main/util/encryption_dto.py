from flask_restx import Namespace, fields

from app.main.util.custom_field.dictionary_field import Dictionary

SEARCH_TYPE = ["primary_key", "row_hash"]


class EncryptionDTO:
    api = Namespace("encryption", description="Encryption related operations")

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
        "search_type": fields.String(enum=SEARCH_TYPE, description="search type flag"),
    }

    encryption_search_value = {
        "search_value": fields.String(resquired=True, description="search value"),
    }

    encryption_progress_value = {
        "progress": fields.Integer(
            resquired=True, description="encryption progress value", min=0, max=100
        )
    }

    encryption_database_rows = api.model(
        "encryption_database_rows",
        encryption_rows_to_encrypt | encryption_update_database,
    )

    encryption_decrypt_row = api.model(
        "decrypt_row",
        encryption_search_type | encryption_search_value,
    )

    encryption_progress = api.model("encryption_progress", encryption_progress_value)
