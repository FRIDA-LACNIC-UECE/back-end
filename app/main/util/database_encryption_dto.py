from flask_restx import Namespace, fields


class DatabaseEncryptionDTO:
    api = Namespace(
        "database_encryption", description="Database encryption related operations"
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
