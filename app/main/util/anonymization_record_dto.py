from flask_restx import Namespace, fields

from app.main.util.anonymization_type_dto import AnonymizationTypeDTO
from app.main.util.database_dto import DatabaseDTO
from app.main.util.table_dto import TableDTO


class AnonymizationRecordDTO:
    api = Namespace(
        "anonymization_record", description="Anonymization record related operations"
    )

    anonymization_record_id = {
        "id": fields.Integer(description="anonymization record id"),
    }

    anonymization_record_database_id = {
        "database_id": fields.Integer(
            required=True, description="database relationship", example=1
        ),
    }

    anonymization_record_database = {
        "database": fields.Nested(
            DatabaseDTO.database_response,
            description="database info",
        ),
    }

    anonymization_record_table_id = {
        "table_id": fields.Integer(
            required=True, description="table relationship", example=1
        ),
    }

    anonymization_record_table = {
        "table": fields.Nested(
            api.model(
                "anonymization_record_table_response",
                TableDTO.table_id | TableDTO.table_name,
            ),
            description="table info",
        ),
    }

    anonymization_record_anonymization_type_id = {
        "anonymization_type_id": fields.Integer(
            required=True, description="anonymization type relationship", example=1
        ),
    }

    anonymization_record_anonymization_type = {
        "anonymization_type": fields.Nested(
            api.model(
                "anonymization_record_anonymization_type",
                AnonymizationTypeDTO.anonymization_type_id
                | AnonymizationTypeDTO.anonymization_type_name,
            ),
            description="anonymization type info",
        ),
    }

    anonymization_record_columns = {
        "columns": fields.List(
            fields.String(description="anonymization record column"),
            required=True,
            description="anonymization record column list",
            min_item=1,
        )
    }

    anonymization_record_post = api.model(
        "anonymization_record_post",
        anonymization_record_database_id
        | anonymization_record_table_id
        | anonymization_record_anonymization_type_id
        | anonymization_record_columns,
    )

    anonymization_record_update = api.model(
        "anonymization_record_put", anonymization_record_columns
    )

    anonymization_record_response = api.model(
        "anonymization_record_response",
        anonymization_record_id
        | anonymization_record_table
        | anonymization_record_anonymization_type
        | anonymization_record_columns,
    )

    anonymization_record_list = api.model(
        "anonymization_record_list",
        {
            "current_page": fields.Integer(),
            "total_items": fields.Integer(),
            "total_pages": fields.Integer(),
            "items": fields.List(fields.Nested(anonymization_record_response)),
        },
    )
