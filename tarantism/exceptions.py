from tarantool import DatabaseError

__all__ = [
    'DoesNotExist',
    'MultipleObjectsReturned',
    'ValidationError',
    'FieldError'
]


class DoesNotExist(Exception):
    pass


class MultipleObjectsReturned(Exception):
    pass


class ValidationError(Exception):
    pass


class FieldError(Exception):
    pass


class IgnorableErrorMixin(object):
    pass


class IgnorableError(DatabaseError, IgnorableErrorMixin):
    pass


class IndexExists(IgnorableError):
    pass


class SpaceExists(IgnorableError):
    pass


def parse_tarantool_exception(e):
    if e.args and e.args[0] == 85:
        return IndexExists(*e.args)

    if e.args and e.args[0] == 10:
        return SpaceExists(*e.args)

    if e.args and e.args[0] == 32:
        return IgnorableError(*e.args)

    return e
