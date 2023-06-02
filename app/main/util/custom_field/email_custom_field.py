from flask_restx import fields


class Email(fields.String):
    __schema_type__ = "string"
    __schema_format__ = "email"
    __schema_example__ = "string@example.com"

    def __init__(self, *args, **kwargs):
        super(Email, self).__init__(*args, **kwargs)
        self.pattern = "([A-Za-z0-9]+[.-_])*[A-Za-z0-9]+@[A-Za-z0-9-]+(\.[A-Z|a-z]{2,})"
        self.description = "user email\n\n\
        This regular expression validates email addresses with the following characteristics:\n\
        - It must start with a sequence of one or more alphanumeric characters.\n\
        - After the initial sequence, there can be zero or more occurrences of dots, dashes, or underscores.\n\
        - It must contain the '@' symbol.\n\
        - After the '@' symbol, there must be a sequence of one or more alphanumeric characters or hyphens.\n\
        - The domain extension of the email must consist of a dot followed by two or more uppercase or lowercase letters."
