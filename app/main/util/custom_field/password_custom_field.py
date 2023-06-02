from flask_restx import fields


class Password(fields.String):
    __schema_type__ = "string"
    __schema_format__ = "password"
    __schema_example__ = "Larces123@"

    def __init__(self, *args, **kwargs):
        super(Password, self).__init__(*args, **kwargs)
        self.pattern = "^(?=.*?[A-Z])(?=.*?[a-z])(?=.*?[0-9])(?=.*?[#?!@$%^&*-]).{8,}$"
        self.description = "user password\n\n\
        In summary, this regular expression validates passwords that meet the following requirements:\n\
        - Must contain at least one uppercase letter.\n\
        - Must contain at least one lowercase letter.\n\
        - Must contain at least one numeric digit.\n\
        - Must contain at least one special character (#?!@$%^&*-).\n\
        - Must have a minimum length of 8 characters."
