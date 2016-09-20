from tarantism.core import Space
from tarantism.metaclasses import ModelMetaclass
from tarantism.connection import get_space, get_connection
from tarantism.connection import DEFAULT_ALIAS
from tarantism.exceptions import ValidationError, SpaceExists, IgnorableError

__all__ = ['Model']

OPERATIONS_MAP = {
    'add': '+',
    'assign': '=',
    'and': '&',
    'xor': '^',
    'or': '|',
}


class Model(object):
    __metaclass__ = ModelMetaclass

    def __init__(self, **kwargs):
        self._data = {}
        self._exists_in_db = kwargs.pop('exists_in_db', False)

        self.reset()

        for key, value in kwargs.iteritems():
            if key in self._fields:
                setattr(self, key, value)

    def __iter__(self):
        return iter(self._fields_ordered)

    def __getitem__(self, name):
        try:
            return getattr(self, name)
        except AttributeError:
            raise KeyError(name)

    def __setitem__(self, name, value):
        if name not in self._fields:
            raise KeyError(name)

        return setattr(self, name, value)

    @classmethod
    def objects(cls, **kwargs):
        if kwargs:
            return cls._objects(**kwargs)
        return cls._objects

    @classmethod
    def create_space(cls):
        space_name = cls._meta['space']
        space_args = cls._meta.get('space_args', tuple())
        try:
            get_connection().call('box.schema.space.create', space_name, *space_args)
        except IgnorableError:
            pass

    @classmethod
    def get_space(cls):
        '''
        :rtype: Space
        '''
        return get_space(
            space=cls._meta['space'],
            alias=cls._meta.get('db_alias', DEFAULT_ALIAS)
        )

    @classmethod
    def space(cls):
        return cls.get_space()

    @classmethod
    def index(cls, index_name):
        return cls.space().index(index_name)

    @classmethod
    def indexes(cls):
        index_map = cls.space().connection.call('indexes', cls._meta['space'])[0][0]
        index_map = {k: v for k, v in index_map.items() if isinstance(k, int)}
        for v in index_map.itervalues():
            v['fields'] = []
            for p in v.get('parts', []):
                p['field_name'] = cls.field_name(p['fieldno'])
                v['fields'].append(p['field_name'])

        return index_map

    @classmethod
    def create_index(cls, index_name=None, index_type=None, fields=None, **kwargs):
        s = cls.get_space()

        assert index_name or fields

        index_type = index_type or 'tree'
        index_name = index_name or '_'.join(fields)

        parts = []
        for field_name in (fields or []):
            f = cls._fields.get(field_name)
            parts.extend([f.creation_counter, f.tarantool_index_type])

        index_params = dict(
            type=index_type,
            parts=parts,
            **kwargs
        )

        return s.create_index(index_name, index_params)

    @classmethod
    def field_name(cls, field_no):
        return cls._fields_ordered[field_no-1]

    @classmethod
    def from_dict(cls, raw_data):
        data = {}
        for field_name, field in cls._fields.iteritems():
            if field_name in raw_data:
                data[field_name] = field.to_python(raw_data[field_name])

        return cls(**data)

    @property
    def exists_in_db(self):
        return self._exists_in_db

    def reset(self):
        self._data = {}
        for field_name, field in self._fields.iteritems():
            value = getattr(self, field_name, None)
            setattr(self, field_name, value)

    def to_db(self):
        data = {}
        for field_name, field in self._fields.items():
            value = self._data.get(field_name, None)
            data[field_name] = field.to_db(value)

        return data

    def validate(self):
        for field_name, field in self._fields.items():
            value = self._data.get(field_name)
            if value is not None:
                field.validate(value)

            elif field.required:
                raise ValidationError(
                    'Field {name} is required.'.format(name=field.name)
                )

    def save(self, validate=True):
        if validate:
            self.validate()

        data = self.to_db()

        if self.exists_in_db:
            return self.update(**data)
        else:
            return self.insert(**data)

    def insert(self, **data):
        values = self._dict_to_values(data)

        self.get_space().insert(values)
        self._exists_in_db = True

        return self

    def update(self, **kwargs):
        primary_key_value = self._get_primary_key_value()

        changes = self._make_changes_struct(kwargs)

        self.get_space().update(primary_key_value, changes)

        self._exists_in_db = True

        # XXX
        for field_name, field_value in kwargs.iteritems():
            setattr(self, field_name, field_value)

        return self

    def delete(self):
        primary_key_value = self._get_primary_key_value()

        response = self.get_space().delete(primary_key_value)

        self._exists_in_db = False

        return response.rowcount > 0

    @classmethod
    def _values_to_dict(cls, values):
        return dict(zip(
            cls._fields_ordered, values
        ))

    @classmethod
    def _dict_to_values(cls, data):
        return tuple([
                         data[field_name] for field_name in cls._fields_ordered
                         if field_name in data
                         ])

    @classmethod
    def _get_tarantool_filter_types(cls):
        field_types = []
        for field_name in cls._fields_ordered:
            field = cls._fields[field_name]
            field_types.append(field.tarantool_filter_type)

        return tuple(field_types)

    def _get_primary_key_value(self):
        pk = getattr(self, 'pk', None)
        if pk:
            return pk

        for field_name, field in self._fields.iteritems():
            if field.primary_key:
                return getattr(self, field_name)

        raise ValueError(
            'Model should have primary key field.'
        )

    def _parse_fields(self, data):
        field_operation_map = {}

        for key, value in data.iteritems():
            chunks = key.split('__', 1)

            if len(chunks) == 1:
                field_name = chunks[0]
                modificator = 'assign'
            else:
                field_name = chunks[0]
                modificator = chunks[1]

            try:
                operation = OPERATIONS_MAP[modificator]
            except KeyError:
                raise ValueError(
                    'Unknown field modificator {mod}.'.format(mod=modificator)
                )

            field_operation_map[field_name] = (operation, value)

        return field_operation_map

    def _make_changes_struct(self, data):
        field_operation_map = self._parse_fields(data)
        changes = []
        for field_number, field_name in enumerate(self._fields_ordered):
            if field_name in field_operation_map:
                operation, value = field_operation_map[field_name]

                # FIXME: update do not understand unicode strings.
                if isinstance(value, unicode):
                    value = str(value)

                changes.append((operation, field_number, value))

        return changes
