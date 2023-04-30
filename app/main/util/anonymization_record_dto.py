from flask_restx import Namespace, fields


class AnonymizationRecordDTO:
    api = Namespace(
        "anonymization_record", description="Anonymization record related operations"
    )

    anonymization_record_post = api.model(
        "anonymization_record_post",
        {
            "database_id": fields.Integer(
                required=True, description="database relationship"
            ),
            "anonymization_type_id": fields.Integer(
                required=True, description="anonymization type relationship"
            ),
            "table_name": fields.String(
                required=True, description="anonymization record table name"
            ),
            "columns": fields.List(
                fields.String(description="anonymization record column"),
                required=True,
                description="anonymization record column list",
            ),
        },
    )

    anonymization_record_update = api.model(
        "anonymization_record_put",
        {
            "anonymization_type_id": fields.Integer(
                required=True, description="anonymization type relationship"
            ),
            "table_name": fields.String(
                required=True, description="anonymization record table name"
            ),
            "columns": fields.List(
                fields.String(description="anonymization record column"),
                required=True,
                description="anonymization record column list",
            ),
        },
    )

    anonymization_record_response = api.model(
        "anonymization_record_response",
        {
            "id": fields.Integer(description="anonymization record id"),
            "database_id": fields.Integer(description="database relationship"),
            "anonymization_type_id": fields.Integer(
                description="anonymization type relationship"
            ),
            "table": fields.String(description="anonymization record table name"),
            "columns": fields.List(
                fields.String(description="anonymization record column"),
                description="anonymization record column list",
            ),
        },
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
