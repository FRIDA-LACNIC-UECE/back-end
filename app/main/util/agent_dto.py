from flask_restx import Namespace, fields

from app.main.util.custom_field.dictionary_field import Dictionary


class AgentDTO:
    api = Namespace("agent", description="Agent related operations")

    _row_hash_list = api.model(
        "row_hash_list",
        {
            "primary_key": fields.List(
                fields.Integer(description="primary key value"),
                description="primary key list",
            ),
            "row_hash": fields.List(
                fields.String(description="row hash"),
                description="row hash list",
            ),
        },
    )

    show_row_hash_list = api.model(
        "show_row_hash_list",
        {
            "row_hash_list": fields.Nested(_row_hash_list, description="row hash list"),
            "primary_key_value_min_limit": fields.Integer(
                description="primary key value min limit"
            ),
            "primary_key_value_max_limit": fields.Integer(
                description="primary key value max limit"
            ),
        },
    )

    process_inserts = api.model(
        "process_inserts",
        {
            "hash_rows": fields.List(
                Dictionary(attribute="hash row", description="hash row"),
                description="hash rows",
            ),
        },
    )

    process_updates = api.model(
        "process_updates",
        {
            "primary_key_list": fields.List(
                fields.Integer(description="primary key value"),
                description="primary key list",
            ),
        },
    )

    process_deletions = api.model(
        "process_deletions",
        {
            "primary_key_list": fields.List(
                fields.Integer(description="primary key value"),
                description="primary key list",
            ),
        },
    )
