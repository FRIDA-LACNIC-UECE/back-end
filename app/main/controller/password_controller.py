from flask import request
from flask_restx import Resource

from app.main.model import User
from app.main.service import (
    change_password,
    check_password_reset_token,
    forgot_password,
    jwt_user_required,
    redefine_password,
)
from app.main.util import DefaultResponsesDTO, PasswordDTO

password_ns = PasswordDTO.api
api = password_ns
password_change = PasswordDTO.password_change
password_forgot = PasswordDTO.password_forgot
password_redefine = PasswordDTO.password_redefine

_default_message_response = DefaultResponsesDTO.message_response
_validation_error_response = DefaultResponsesDTO.validation_error


@api.route("/change")
class ChangePassword(Resource):
    @api.doc("Change password when logged in")
    @api.expect(password_change, validate=True)
    @api.response(200, "password_updated", _default_message_response)
    @api.response(400, "Input payload validation failed", _validation_error_response)
    @api.response(404, "user_not_found", _default_message_response)
    @api.response(
        409,
        "password_not_match\npassword_incorrect_information",
        _default_message_response,
    )
    @jwt_user_required
    def patch(self, current_user: User) -> tuple[dict[str, str], int]:
        """Change password when logged in"""
        data = request.json
        change_password(data=data, current_user=current_user)
        return {"message": "password_updated"}, 200


@api.route("/forgot")
class ForgotPassword(Resource):
    @api.doc("Forgot password")
    @api.expect(password_forgot, validate=True)
    @api.response(200, "recovery_email_sent", _default_message_response)
    @api.response(400, "Input payload validation failed", _validation_error_response)
    @api.response(
        404, "user_not_found\nprofessional_not_found", _default_message_response
    )
    @api.response(409, "user_not_actived", _default_message_response)
    def post(self) -> tuple[dict[str, str], int]:
        """Forgot password"""
        data = request.json
        forgot_password(data=data)
        return {"message": "recovery_email_sent"}, 200


@api.route("/check_password_reset_token/<string:token>")
class CheckPasswordResetToken(Resource):
    @api.doc("Check password reset token")
    @api.response(200, "token_valid", _default_message_response)
    @api.response(401, "token_expired", _default_message_response)
    @api.response(409, "token_invalid", _default_message_response)
    def get(self, token: str) -> tuple[dict[str, str], int]:
        """Check password reset token"""
        check_password_reset_token(token)
        return {"message": "token_valid"}, 200


@api.route("/redefine/<string:token>")
class RedefinePassword(Resource):
    @api.doc("Redefine password")
    @api.expect(password_redefine, validate=True)
    @api.response(200, "password_updated", _default_message_response)
    @api.response(400, "Input payload validation failed", _validation_error_response)
    @api.response(401, "token_expired", _default_message_response)
    @api.response(404, "user_not_found", _default_message_response)
    @api.response(409, "token_invalid", _default_message_response)
    @api.response(
        409, "user_not_active\npasswords_not_match", _default_message_response
    )
    def patch(self, token: str) -> tuple[dict[str, str], int]:
        """Redefine password"""
        data = request.json
        redefine_password(data=data, token=token)
        return {"message": "password_updated"}, 200
