from flask_restx import Namespace, fields

from app.main.util.custom_field.dictionary_field import Dictionary

SEARCH_TYPE = ["primary_key", "row_hash"]


class EncryptionDTO:
    api = Namespace("encryption", description="Database encryption related operations")

    encryption_table_name = {
        "table_name": fields.String(
            required=True, description="table name to encryption"
        ),
    }

    encryption_rows_to_encrypt = {
        "rows_to_encrypt": fields.List(
            Dictionary(attribute="rows_to_encrypt", description="row to encrypt"),
            description="rows to encrypt",
        ),
    }

    encryption_decrypt_row = api.model(
        "decrypt_row",
        {
            "table_name": fields.String(
                required=True, description="database table name"
            ),
            "search_type": fields.String(
                resquired=True, description="search type", enum=SEARCH_TYPE
            ),
            "search_value": fields.String(resquired=True, description="search value"),
        },
    )

    encryption_update_database = {
        "update_database": fields.Boolean(description="update database flag"),
    }

    encryption_database_rows = api.model(
        "encryption_database_rows",
        encryption_table_name | encryption_rows_to_encrypt | encryption_update_database,
    )

    encrypt_database_table = api.model("encryption_database", encryption_table_name)
