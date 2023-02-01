from flask_restx import Namespace, fields


class DatabaseDTO:
    api = Namespace("database", description="Database related operations")

    database_post = api.model(
        "database_post",
        {
            "valid_database_id": fields.Integer(
                required=True, description="valid database relationship", example=1
            ),
            "name": fields.String(required=True, description="database name"),
            "host": fields.String(required=True, description="database host"),
            "username": fields.String(required=True, description="database username"),
            "port": fields.Integer(required=True, description="database port"),
            "password": fields.String(required=True, description="database password"),
        },
    )

    database_put = api.model(
        "database_put",
        {
            "name": fields.String(required=True, description="database name"),
            "host": fields.String(required=True, description="database host"),
            "username": fields.String(required=True, description="database username"),
            "port": fields.Integer(required=True, description="database port"),
            "password": fields.String(required=True, description="database password"),
        },
    )

    database_response = api.model(
        "database_response",
        {
            "id": fields.Integer(description="database id"),
            "user_id": fields.Integer(description="user relationship"),
            "valid_database_id": fields.Integer(
                description="valid database relationship"
            ),
            "name": fields.String(description="database name"),
            "host": fields.String(description="database host"),
            "username": fields.String(description="database user name"),
            "port": fields.Integer(description="database port"),
            "password": fields.String(description="database password"),
        },
    )

    database_tables = api.model(
        "database_tables",
        {
            "table_names": fields.List(
                fields.String(description="table name"),
                required=True,
                description="table name list",
            ),
        },
    )

    database_columns = api.model(
        "database_columns",
        {
            "column_names": fields.List(
                fields.String(description="column name"),
                required=True,
                description="column name list",
            ),
        },
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
