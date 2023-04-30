from flask_restx import Namespace, fields

from app.main.service import Dictionary


class AnonymizationDTO:
    api = Namespace("anonymization", description="Anonymization operations")

    database_rows_anonymization = api.model(
        "database_rows_anonymization",
        {
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

    database_anonymization = api.model(
        "database_anonymization",
        {
            "table_name": fields.String(
                required=True, description="anonymization record table name"
            ),
        },
    )
