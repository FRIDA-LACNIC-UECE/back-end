from flask_restx import Namespace, fields


class ColumnDTO:
    api = Namespace("column", description="Column related operations")

    column_id = {"id": fields.Integer(description="column id")}

    column_table_id = {"id": fields.Integer(description="table id")}

    column_anonymization_type_id = {
        "anonymization_type_id": fields.Integer(description="anonimyzation type id ")
    }

    column_name = {
        "name": fields.String(required=True, description="column name", min_length=1)
    }

    column_type = {
        "type": fields.String(required=True, description="column type", min=0, max=100)
    }
    column_post = api.model(
        "column_post",
        column_anonymization_type_id | column_name | column_type,
    )

    column_put = api.model(
        "column_put",
        column_anonymization_type_id | column_name | column_type,
    )

    column_response = api.model(
        "column_response",
        column_id
        | column_table_id
        | column_anonymization_type_id
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
