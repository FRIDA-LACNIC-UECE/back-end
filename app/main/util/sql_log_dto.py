from flask_restx import Namespace, fields

from app.main.util import DatabaseDTO


class SqlLogDTO:
    api = Namespace("sql_log", description="SQL Log related operations")

    sql_log_id = {"id": fields.Integer(description="sql log id")}

    sql_log_sql_command = {
        "sql_command": fields.String(
            required=True, description="sql log command", min_length=1
        )
    }

    sql_log_post = api.model("sql_log_post", sql_log_sql_command)

    sql_log_put = api.clone("sql_log_put", sql_log_post)

    sql_log_response = api.model(
        "sql_log_response",
        sql_log_id
        | sql_log_sql_command
        | {
            "database": fields.Nested(
                DatabaseDTO.database_response,
                description="database info",
            ),
        },
    )

    sql_log_list = api.model(
        "sql_log_list",
        {
            "current_page": fields.Integer(),
            "total_items": fields.Integer(),
            "total_pages": fields.Integer(),
            "items": fields.List(fields.Nested(sql_log_response)),
        },
    )
