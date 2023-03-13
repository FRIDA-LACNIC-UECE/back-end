from flask_restx import Namespace, fields

from app.main.service import Dictionary


class EncryptionDTO:
    api = Namespace("encryption", description="Database encryption related operations")

    database_rows_encryption = api.model(
        "database_rows_encryption",
        {
            "database_id": fields.Integer(
                required=True,
                description="database id to encryption",
            ),
            "table_name": fields.String(
                required=True, description="table name to encryption"
            ),
            "rows_to_encrypt": fields.List(
                Dictionary(attribute="rows_to_encrypt", description="row to encrypt"),
                description="rows to encrypt",
            ),
            "update_database": fields.Boolean(description="update database flag"),
        },
    )

    database_encryption = api.model(
        "database_encryption",
        {
            "database_id": fields.Integer(
                required=True,
                description="database id to encryption",
            ),
            "table_name": fields.String(
                required=True, description="table name to encryption"
            ),
        },
    )
