class ValidationException(Exception):
    def __init__(self, code=400, errors={}, message="Generic error"):
        self.code = code
        self.errors = errors
        self.message = message
