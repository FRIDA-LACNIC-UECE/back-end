from flask_restx import Namespace, fields


class AnonymizationDTO:
    api = Namespace("anonymization", description="Anonymization operations")

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
