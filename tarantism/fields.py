
import re

import tarantool

from tarantism.errors import ValidationError


__all__ = [
    'BaseField', 'IntField', 'LongField', 'StringField', 'BytesField',
    'DEFAULT_ENCODING'
]


class BaseField(object):
    name = None

    creation_counter = 0

    def __init__(self,
                 required=True,
                 default=None,
                 primary_key=False,
                 db_index=None,
                 verbose_name=None,
                 help_text=None):
        self.required = required
        self.default = default
        self.primary_key = primary_key
        self.db_index = db_index
        self.verbose_name = verbose_name
        self.help_text = help_text

        self.creation_counter = BaseField.creation_counter
        BaseField.creation_counter += 1

    @property
    def db_type(self):
        return tarantool.RAW

    def __get__(self, instance, owner):
        if instance is None:
            return self

        return instance._data.get(self.name)

    def __set__(self, instance, value):
        if value is None and self.default is not None:
            value = self.default
            if callable(value):
                value = value()

        instance._data[self.name] = value

    def to_python(self, value):
        return value

    def to_db(self, value):
        return self.to_python(value)

    def validate(self, value):
        pass


class IntField(BaseField):
    def __init__(self, min_value=None, max_value=None, **kwargs):
        self.min_value = min_value
        self.max_value = max_value

        self._type_factory = int

        super(IntField, self).__init__(**kwargs)

    @property
    def db_type(self):
        return tarantool.NUM

    def to_python(self, value):
        return self._type_factory(value)

    def validate(self, value):
        try:
            value = self._type_factory(value)
        except ValueError:
            raise ValidationError(
                '{name} field error: '
                '{value} could not be converted to {type}.'.format(
                    name=self.name, value=value, type=self._type_factory
                )
            )

        if self.min_value is not None and value < self.min_value:
            raise ValidationError(
                '{name} field error: '
                'value {value} is less than {min_value}'.format(
                    name=self.name, value=value, min_value=self.min_value
                )
            )

        if self.max_value is not None and value > self.max_value:
            raise ValidationError(
                '{name} field error: '
                'value {value} is greater than {max_value}'.format(
                    name=self.name, value=value, max_value=self.max_value
                )
            )


class LongField(IntField):
    def __init__(self, **kwargs):
        super(LongField, self).__init__(**kwargs)

        self._type_factory = long

    @property
    def db_type(self):
        return tarantool.NUM64


DEFAULT_ENCODING = 'utf8'


class BytesField(BaseField):
    def __init__(self,
                 regex=None,
                 max_length=None,
                 min_length=None,
                 **kwargs):
        self.regex = re.compile(regex) if regex else None
        self.max_length = max_length
        self.min_length = min_length

        super(BytesField, self).__init__(**kwargs)

    def validate(self, value):
        if not isinstance(value, basestring):
            raise ValidationError(
                '{name} field error: '
                '{value} has incorrect type {type} '
                'and could not be converted to string.'.format(
                    name=self.name, value=value, type=type(value)
                )
            )

        if self.min_length is not None and len(value) < self.min_length:
            raise ValidationError(
                '{name} field error: '
                'value {value} length is less than {min_length}'.format(
                    name=self.name, value=value, min_length=self.min_length
                )
            )

        if self.max_length is not None and len(value) > self.max_length:
            raise ValidationError(
                '{name} field error: '
                'value {value} length is greater than {max_length}'.format(
                    name=self.name, value=value, max_length=self.max_length
                )
            )

        if self.regex is not None and self.regex.match(value) is None:
            raise ValidationError(
                '{name} field error: '
                'value {value} did not match validation regex.'.format(
                    name=self.name, value=value
                )
            )


class StringField(BytesField):
    def __init__(self, encoding=None, **kwargs):
        self.encoding = encoding or DEFAULT_ENCODING

        super(StringField, self).__init__(**kwargs)

    @property
    def db_type(self):
        return tarantool.STR

    def to_db(self, value):
        return value.encode(self.encoding)

    def to_python(self, value):
        return value.decode(self.encoding)
