from flask_restx import Namespace, fields
from app.main.util.custom_field import Dictionary


class AnonymizationDTO:
    api = Namespace("anonymization", description="Anonymization operations")

    anonymization_database_id = {
        "database_id": fields.Integer(
            required=True, description="database id to anonymization", example=1
        )
    }

    anonymization_table_id = {
        "table_id": fields.Integer(
            required=True, description="table id to anonymization", example=1
        )
    }

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

    database_rows_anonymization = api.model(
        "database_rows_anonymization",
        anonymization_database_id
        | anonymization_table_id
        | anonymization_rows_to_anonymization
        | anonymization_insert_database,
    )

    table_anonymization = api.model(
        "table_anonymization", anonymization_database_id | anonymization_table_id
    )

    database_remove_anonymization = api.model(
        "database_remove_anonymization",
        anonymization_database_id,
        anonymization_table_id,
    )
