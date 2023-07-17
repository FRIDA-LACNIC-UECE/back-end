from flask_restx import fields


class Username(fields.String):
    __schema_type__ = "string"
    __schema_format__ = "username"
    __schema_example__ = "Abc_123"

    def __init__(self, *args, **kwargs):
        super(Username, self).__init__(*args, **kwargs)
        self.pattern = (
            r"^[a-zA-Z0-9](_(?!(\.|_))|\.(?!(_|\.))|[a-zA-Z0-9]){6,30}[a-zA-Z0-9]$"
        )
        self.description = "User username.\n\n\
        In summary, this regex validates a sequence of alphanumeric characters that meet the following criteria: \n\
        - It must start with an alphanumeric character.\n\
        - It must end with an alphanumeric character.\n\
        - It may contain a variable number of alphanumeric characters (including underscores and periods) between the first and last character.\n\
        - It cannot have two consecutive underscores (_).\n\
        - It cannot have two consecutive periods (.).\n\
        - It must have a minimum length of 6 characters and a maximum length of 30 characters."
