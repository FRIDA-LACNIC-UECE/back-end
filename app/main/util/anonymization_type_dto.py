from flask_restx import Namespace, fields


class AnonymizationTypeDTO:
    api = Namespace(
        "anonymization_type", description="Anonymization type related operations"
    )

    anonymization_type_id = {"id": fields.Integer(description="anonymization type id")}

    anonymization_type_name = {
        "name": fields.String(
            required=True,
            description="anonymization type name",
            min_length=1,
            max_length=255,
        )
    }

    anonymization_type_post = api.model(
        "anonymization_type_post", anonymization_type_name
    )

    anonymization_type_put = api.clone(
        "anonymization_type_put", anonymization_type_post
    )

    anonymization_type_response = api.model(
        "anonymization_type_response", anonymization_type_id | anonymization_type_name
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
