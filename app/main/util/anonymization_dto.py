from flask_restx import Namespace, fields

from app.main.service import Dictionary


class AnonymizationDTO:
    api = Namespace("anonymization", description="Anonymization operations")

    database_rows_encryption = api.model(
        "database_rows_encryption",
        {
            "database_id": fields.Integer(
                required=True,
                description="database id to anonymization",
            ),
            "table_name": fields.String(
                required=True, description="table name to anonymization"
            ),
            "rows_to_anonymization": fields.List(
                Dictionary(
                    attribute="rows_to_anonymization",
                    description="row to anonymization",
                ),
                description="rows to anonymization",
            ),
            "insert_database": fields.Boolean(description="insert database flag"),
        },
    )

    anonymization_database = api.model(
        "anonymization_database",
        {
            "database_id": fields.Integer(
                required=True, description="database relationship"
            ),
            "table_name": fields.String(
                required=True, description="anonymization record table name"
            ),
        },
    )
