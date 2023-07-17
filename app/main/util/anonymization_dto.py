from flask_restx import Namespace, fields

from app.main.util.custom_field import Dictionary


class AnonymizationDTO:
    api = Namespace("anonymization", description="Anonymization operations")

    anonymization_rows_to_anonymization = {
        "rows_to_anonymization": fields.List(
            Dictionary(
                attribute="rows_to_anonymization",
                description="row to anonymization",
            ),
            description="rows to anonymization",
        ),
    }

    anonymization_insert_database = {
        "insert_database": fields.Boolean(description="insert database flag"),
    }

    anonymization_database_rows = api.model(
        "anonymization_database_rows",
        anonymization_rows_to_anonymization | anonymization_insert_database,
    )

    anonymization_progress_value = {
        "progress": fields.Integer(
            resquired=True, description="anonymization progress value", min=0, max=100
        ),
    }

    anonymization_progress = api.model(
        "anonymization_progress", anonymization_progress_value
    )
