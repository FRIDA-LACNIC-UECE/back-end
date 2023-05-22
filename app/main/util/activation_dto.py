from flask_restx import Namespace, fields


class ActivationDTO:
    api = Namespace("activation", description="activation related operations")
