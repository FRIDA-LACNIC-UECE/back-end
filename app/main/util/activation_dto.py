from flask_restx import Namespace, fields


class ActivationDTO:
    api = Namespace("activation", description="Activation related operations")
