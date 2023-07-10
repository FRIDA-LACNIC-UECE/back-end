from flask_restx import Namespace, fields


class TableDTO:
    api = Namespace("table", description="Table related operations")

    table_id = {"id": fields.Integer(description="table id")}

    table_name = {
        "name": fields.String(required=True, description="table name", min_length=1)
    }

    table_encryption_progress = {
        "encryption_progress": fields.Integer(
            required=True, description="encryption progress", min=0, max=100
        )
    }

    table_anonimyzation_progress = {
        "anonimyzation_progress": fields.Integer(
            required=True, description="anonimyzation progress", min=0, max=100
        )
    }

    table_encrypted = {
        "encrypted": fields.Boolean(
            required=True, description="True if table is encrypted"
        )
    }

    table_anonymized = {
        "anonymized": fields.Boolean(
            required=True, description="True if table is anonymized"
        )
    }

    table_post = api.model(
        "table_post",
        table_name,
    )

    table_put = api.model(
        "table_put",
        table_name,
    )

    table_response = api.model(
        "table_response",
        table_id
        | table_name
        | table_encrypted
        | table_encryption_progress
        | table_anonymized
        | table_anonimyzation_progress,
    )

    table_sensitive_columns = api.model(
        "table_sensitive_columns",
        {
            "sensitive_column_names": fields.List(
                fields.String(description="sensitive column name"),
                description="sensitive column name list",
            ),
        },
    )

    table_list = api.model(
        "table_list",
        {
            "current_page": fields.Integer(),
            "total_items": fields.Integer(),
            "total_pages": fields.Integer(),
            "items": fields.List(fields.Nested(table_response)),
        },
    )
