from flask_restx import Namespace, fields

from app.main.util.custom_field.dictionary_field import Dictionary


class AnonymizationDTO:
    api = Namespace("anonymization", description="Anonymization operations")

    anonymization_table_name = {
        "table_name": fields.String(
            required=True, description="table name to anonymization"
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
        anonymization_table_name
        | anonymization_rows_to_anonymization
        | anonymization_insert_database,
    )

    table_anonymization = api.model("table_anonymization", anonymization_table_name)

    database_remove_anonymization = api.model(
        "database_remove_anonymization", anonymization_table_name
    )
