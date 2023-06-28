from flask import request
from flask_restx import Resource

from app.main.service import (
    activate_user,
    check_activation_token,
    resend_activation_email,
)
from app.main.util import ActivationDTO, DefaultResponsesDTO, UserDTO

activation_ns = UserDTO.api
api = activation_ns

_default_message_response = DefaultResponsesDTO.message_response


@api.route("/activate_user/<string:token>")
class ActivationUser(Resource):
    @api.doc("Activate user")
    @api.response(200, "user_activated", _default_message_response)
    @api.response(400, "Input payload validation failed", _default_message_response)
    @api.response(401, "token_expired", _default_message_response)
    @api.response(409, "token_invalid", _default_message_response)
    @api.response(
        409,
        "user_already_activated\nuser_is_blocked",
        _default_message_response,
    )
    def post(self, token: str) -> tuple[dict[str, str], int]:
        """Activate user"""
        activate_user(token)
        return {"message": "user_activated"}, 200


@api.route("/check_activation_token/<string:token>")
class CheckActivationToken(Resource):
    @api.doc("Check activation token")
    @api.response(200, "token_valid", _default_message_response)
    @api.response(401, "token_expired", _default_message_response)
    @api.response(409, "token_invalid", _default_message_response)
    def get(self, token: str) -> tuple[dict[str, str], int]:
        """Check activation token"""
        check_activation_token(token)
        return {"message": "token_valid"}, 200


@api.route("/resend_activation_email/<int:user_id>")
class ResendActivationEmail(Resource):
    @api.doc("Resend activation email")
    @api.response(200, "activation_email_resent", _default_message_response)
    @api.response(404, "user_not_found", _default_message_response)
    @api.response(
        409, "user_already_activated\nuser_is_blocked", _default_message_response
    )
    def put(self, user_id: int) -> tuple[dict[str, str], int]:
        """Resend activation email"""
        resend_activation_email(user_id)
        return {"message": "activation_email_resent"}, 200
