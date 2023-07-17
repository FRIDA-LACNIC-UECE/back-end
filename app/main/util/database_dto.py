from flask_restx import Namespace, fields

from app.main.util.valid_database_dto import ValidDatabaseDTO
from app.main.util.user_dto import UserDTO


class DatabaseDTO:
    api = Namespace("database", description="Database related operations")

    database_id = {
        "id": fields.Integer(required=True, description="database id", example=1)
    }

    database_valid_database_id = {
        "valid_database_id": fields.Integer(
            required=True, description="valid database relationship", example=1
        )
    }

    database_name = {
        "name": fields.String(required=True, description="database name"),
    }

    database_host = {
        "host": fields.String(required=True, description="database host"),
    }

    database_username = {
        "username": fields.String(required=True, description="database username"),
    }

    database_port = {
        "port": fields.Integer(required=True, description="database port"),
    }

    database_password = {
        "password": fields.String(description="database password"),
    }

    database_post = api.model(
        "database_post",
        database_valid_database_id
        | database_name
        | database_host
        | database_username
        | database_port
        | database_password,
    )

    database_put = api.clone("database_put", database_post)

    database_response = api.model(
        "database_response",
        database_id
        | database_name
        | database_host
        | database_username
        | database_port
        | database_password
        | {
            "user": fields.Nested(
                api.model(
                    "database_response_user",
                    UserDTO.user_id | UserDTO.user_username,
                ),
                description="user info",
            ),
            "valid_database": fields.Nested(
                api.model(
                    "database_response_valid_database",
                    ValidDatabaseDTO.valid_database_id
                    | ValidDatabaseDTO.valid_database_name
                    | ValidDatabaseDTO.valid_database_dialect,
                ),
                description="valid database info",
            ),
        },
    )

    database_tables = api.model(
        "database_tables",
        {
            "table_names": fields.List(
                fields.String(description="table name"),
                description="table name list",
            ),
        },
    )

    database_table_column = api.model(
        "database_table_column",
        {
            "name": fields.String(description="column name"),
            "type": fields.String(description="column type"),
        },
    )

    database_table_columns = api.model(
        "database_table_columns",
        {"table_columns": fields.Nested(database_table_column)},
    )

    database_list = api.model(
        "database_list",
        {
            "current_page": fields.Integer(),
            "total_items": fields.Integer(),
            "total_pages": fields.Integer(),
            "items": fields.List(fields.Nested(database_response)),
        },
    )
