from flask_restx import Namespace, fields


class AnonymizationTypeDTO:
    api = Namespace(
        "anonymization_type", description="Anonymization type related operations"
    )

    anonymization_type_post = api.model(
        "anonymization_type_post",
        {
            "name": fields.String(required=True, description="anonymization type name"),
        },
    )

    anonymization_type_put = api.clone(
        "anonymization_type_put", anonymization_type_post
    )

    anonymization_type_response = api.model(
        "anonymization_type_response",
        {
            "id": fields.Integer(description="anonymization type id"),
            "name": fields.String(description="anonymization type name"),
        },
    )

    anonymization_type_list = api.model(
        "anonymization_type_list",
        {
            "current_page": fields.Integer(),
            "total_items": fields.Integer(),
            "total_pages": fields.Integer(),
            "items": fields.List(fields.Nested(anonymization_type_response)),
        },
    )
