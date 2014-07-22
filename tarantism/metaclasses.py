
from tarantism.fields import BaseField
from tarantism.errors import DoesNotExist
from tarantism.errors import MultipleObjectsReturned


__all__ = ['ModelMetaclass']


class ModelMetaclass(type):
    def __new__(cls, name, bases, attrs):
        super_new = super(ModelMetaclass, cls).__new__

        fields = {}
        for attr_name, attr_value in attrs.iteritems():
            if not isinstance(attr_value, BaseField):
                continue

            attr_value.name = attr_name
            fields[attr_name] = attr_value

        attrs['_fields'] = fields
        attrs['_meta'] = attrs.pop('meta') if 'meta' in attrs else {}

        for exc in (DoesNotExist, MultipleObjectsReturned):
            attrs[exc.__name__] = exc

        return super_new(cls, name, bases, attrs)
