from werkzeug.exceptions import HTTPException


class DefaultException(HTTPException):
    def __init__(self, message="Generic error", info=None, code=None):
        self.code = code
        self.description = message
        self.info = info

        super().__init__()
