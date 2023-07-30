from flask_restx import Namespace, fields


class ValidDatabaseDTO:
    api = Namespace("valid_database", description="Valid database related operations")

    valid_database_id = {
        "id": fields.Integer(required=True, description="valid database id")
    }

    valid_database_name = {
        "name": fields.String(
            required=True,
            description="valid database name",
            min_length=1,
            max_length=255,
        ),
    }

    valid_database_dialect = {
        "dialect": fields.String(
            required=True,
            description="valid database dialect",
            min_length=1,
            max_length=255,
        ),
    }

    valid_database_post = api.model(
        "valid_database_post", valid_database_name, valid_database_dialect
    )

    valid_database_put = api.clone("valid_database_put", valid_database_post)

    valid_database_response = api.model(
        "valid_database_response",
        valid_database_id | valid_database_name | valid_database_dialect,
    )

    valid_database_list = api.model(
        "valid_database_list",
        {
            "current_page": fields.Integer(),
            "total_items": fields.Integer(),
            "total_pages": fields.Integer(),
            "items": fields.List(fields.Nested(valid_database_response)),
        },
    )
