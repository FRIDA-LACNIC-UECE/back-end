from flask_restx import Namespace, fields

from app.main.util.anonymization_type_dto import AnonymizationTypeDTO
from app.main.util.table_dto import TableDTO


class ColumnDTO:
    api = Namespace("column", description="column related operations")

    column_id = {"id": fields.Integer(description="column id")}

    column_table_id = {"id": fields.Integer(description="table id", example=1)}

    column_table = {
        "table": fields.Nested(
            api.model("column_table_response", TableDTO.table_id | TableDTO.table_name)
        )
    }

    column_anonymization_type_id = {
        "anonymization_type_id": fields.Integer(
            description="anonymization type id", example=1
        )
    }

    column_anonymization_type = {
        "anonymization_type": fields.Nested(
            api.model(
                "column_anonymization_type_response",
                AnonymizationTypeDTO.anonymization_type_id
                | AnonymizationTypeDTO.anonymization_type_name,
            )
        )
    }

    column_name = {
        "name": fields.String(required=True, description="column name", min_length=1)
    }

    column_type = {
        "type": fields.String(required=True, description="column type", min_length=1)
    }

    column_post = api.model("column_post", column_anonymization_type_id | column_name)

    column_update = api.model("column_put", column_anonymization_type_id | column_name)

    column_response = api.model(
        "column_response",
        column_id
        | column_table
        | column_anonymization_type
        | column_name
        | column_type,
    )

    column_list = api.model(
        "column_list",
        {
            "current_page": fields.Integer(),
            "total_items": fields.Integer(),
            "total_pages": fields.Integer(),
            "items": fields.List(fields.Nested(column_response)),
        },
    )
