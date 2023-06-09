from flask_restx import Namespace, fields


class SqlLogDTO:
    api = Namespace("sql_log", description="SQL Log related operations")

    sql_log_post = api.model(
        "sql_log_post",
        {
            "database_id": fields.Integer(description="relatioship database"),
            "sql_command": fields.String(required=True, description="sql log command"),
        },
    )

    sql_log_put = api.clone("sql_log_put", sql_log_post)

    sql_log_response = api.model(
        "sql_log_response",
        {
            "id": fields.Integer(description="sql log id"),
            "database_id": fields.Integer(description="relationship database"),
            "sql_command": fields.String(description="sql log command"),
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
