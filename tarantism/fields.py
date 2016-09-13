import re
from datetime import datetime
from decimal import Decimal
from uuid import uuid4

import ujson

from tarantism.exceptions import ValidationError

__all__ = [
    'BaseField',
    'INT32_MIN', 'INT32_MAX', 'Num32Field',
    'INT64_MIN', 'INT64_MAX', 'Num64Field',
    'StringField', 'BytesField',
    'DateTimeField', 'DEFAULT_DATETIME_FORMAT',
    'DecimalField',
]

INT32_MIN = -2147483648

INT32_MAX = +2147483647

INT64_MIN = -9223372036854775808

INT64_MAX = +9223372036854775807


class BaseField(object):
    name = None

    # Used in tarantism.metaclasses.ModelMetaclass to sort fields.
    creation_counter = 1

    # Used for field_types in tarantool client filter method.
    tarantool_filter_type = str
    tarantool_index_type = 'scalar'

    def __init__(self,
                 required=False,
                 default=None,
                 primary_key=False,
                 db_index=None,
                 verbose_name=None,
                 help_text=None):
        self.required = required
        self.default = default
        self.primary_key = primary_key
        self.db_index = 0 if primary_key else db_index
        self.verbose_name = verbose_name
        self.help_text = help_text

        self.creation_counter = BaseField.creation_counter
        BaseField.creation_counter += 1

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
        if self.required and not value:
            raise ValidationError(
                '{name} field error: '
                'value is required.'.format(
                    name=self.name
                )
            )


class Num32Field(BaseField):
    MIN = INT32_MIN
    MAX = INT32_MAX

    # Used for field_types in tarantool client filter method.
    tarantool_filter_type = int
    tarantool_index_type = 'integer'

    type_factory = int

    def __init__(self, min_value=None, max_value=None, **kwargs):
        min_value = min_value or self.MIN
        max_value = max_value or self.MAX

        if min_value < self.MIN:
            raise ValueError('min_value can not be less than {}.'.format(self.MIN))

        if max_value > self.MAX:
            raise ValueError('max_value can not be greater than {}.'.format(self.MAX))

        self.min_value = min_value
        self.max_value = max_value

        super(Num32Field, self).__init__(**kwargs)

    def to_python(self, value):
        if value:
            return self.type_factory(value)
        return value

    def validate(self, value):
        super(Num32Field, self).validate(value)

        try:
            value = self.type_factory(value)
        except ValueError:
            raise ValidationError(
                '{name} field error: '
                'Invalid {value} for field {field_class}.'.format(
                    name=self.name, value=value, field_class=self.__class__.__name__
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


class Num64Field(Num32Field):
    MIN = INT64_MIN
    MAX = INT64_MAX

    tarantool_filter_type = long
    tarantool_index_type = 'integer'

    type_factory = long


IntField = Num32Field
LongIntField = Num64Field


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
        super(BytesField, self).validate(value)

        if not isinstance(value, basestring):
            raise ValidationError(
                '{name} field error: '
                'Invalid {value} for field {field_class}.'.format(
                    name=self.name, value=value, field_class=self.__class__.__name__
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
    tarantool_filter_type = unicode
    tarantool_index_type = 'string'

    def to_db(self, value):
        if value:
            return value.encode('utf8')
        return value

    def to_python(self, value):
        if value:
            return value.decode('utf8')
        return value


class UUIDField(StringField):
    @staticmethod
    def str_uuid():
        return str(uuid4())

    def __init__(self, **kwargs):
        kwargs.update(
            default=UUIDField.str_uuid,
            max_length=36,
            min_length=36,
        )

        super(UUIDField, self).__init__(**kwargs)


DEFAULT_DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S.%f'


class DateTimeField(BaseField):
    tarantool_index_type = 'string'

    def __init__(self,
                 datetime_format=DEFAULT_DATETIME_FORMAT,
                 **kwargs):
        self.datetime_format = datetime_format

        super(DateTimeField, self).__init__(**kwargs)

    def to_db(self, value):
        if value:
            return value.strftime(self.datetime_format)
        return ''

    def to_python(self, value):
        if value:
            return datetime.strptime(value, self.datetime_format)
        return None

    def validate(self, value):
        super(DateTimeField, self).validate(value)

        if not isinstance(value, datetime):
            raise ValidationError(
                '{name} field error: '
                '{value} has incorrect type {type}.'.format(
                    name=self.name, value=value, type=type(value)
                )
            )


class DecimalField(BaseField):
    def __init__(self, **kwargs):
        super(DecimalField, self).__init__(**kwargs)

    def to_db(self, value):
        return str(value)

    def to_python(self, value):
        return Decimal(value)


class BooleanField(BaseField):
    tarantool_filter_type = bool
    tarantool_index_type = 'scalar'

    def __init__(self, **kwargs):
        super(BooleanField, self).__init__(**kwargs)

    def to_db(self, value):
        return value

    def to_python(self, value):
        return bool(value)


class JsonField(BaseField):
    def to_db(self, value):
        return ujson.dumps(value)

    def to_python(self, value):
        return ujson.loads(value)

    def validate(self, value):
        super(JsonField, self).validate(value)

        if not isinstance(value, (dict, list, tuple)):
            raise ValidationError(
                '{name} field error: '
                'value is not dict/list. Use simple field'.format(
                    name=self.name
                )
            )


class DictField(BaseField):
    def __init__(self, **kwargs):
        kwargs.setdefault('default', lambda: {})
        super(DictField, self).__init__(**kwargs)

    def validate(self, value):
        super(DictField, self).validate(value)

        if not isinstance(value, dict):
            raise ValidationError(
                '{name} field error: '
                'value is not dict.'.format(
                    name=self.name
                )
            )


class ListField(BaseField):
    def __init__(self, field, **kwargs):
        self.field = field
        BaseField.creation_counter -= 1

        kwargs.setdefault('default', lambda: [])
        super(ListField, self).__init__(**kwargs)

    def validate(self, value):
        super(ListField, self).validate(value)

        if not isinstance(value, (list, tuple)):
            raise ValidationError(
                '{name} field error: '
                'value is not list.'.format(
                    name=self.name
                )
            )

        if self.field:
            if hasattr(value, 'iteritems') or hasattr(value, 'items'):
                sequence = value.iteritems()
            else:
                sequence = enumerate(value)
            for k, v in sequence:
                try:
                    self.field.validate(v)
                except Exception as e:
                    raise ValidationError(
                        '{name} field error: '
                        'list item value is not validated'.format(
                            name=self.name
                        )
                    )


class ListAsDictField(ListField):
    def to_db(self, value):
        return {i: True for i in value}

    def to_python(self, value):
        return value.keys()
