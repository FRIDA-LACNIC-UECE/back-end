from flask_restx import Namespace, fields


class DefaultResponsesDTO:
    api = Namespace("default_responses", description="default responses")

    message_response = api.model("message_response", {"message": fields.String()})

    validation_error = api.model(
        "validation_error_response",
        {"errors": fields.Raw(), "message": fields.String()},
    )
