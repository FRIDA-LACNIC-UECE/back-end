from flask_restx import fields


class Dictionary(fields.Raw):
    __schema_type__ = "raw"
    __schema_format__ = "dictionary"
    __schema_example__ = "'{'key:value'}'"

    def output(self, key, obj, *args, **kwargs):
        try:
            dct = getattr(obj, self.attribute)
        except AttributeError:
            return {}
        return dct or {}
