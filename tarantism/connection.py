
import sys

from tarantool import DatabaseError

from tarantism.core import Connection

__all__ = [
    'DEFAULT_ALIAS', 'DEFAULT_HOST', 'DEFAULT_PORT', 'DEFAULT_SPACE',
    'ConnectionError',
    'connect', 'disconnect',
    'register_connection', 'get_connection', 'get_space',
]


_connection_settings = {}
"""Aliases to settings mapping."""

_connections = {}
"""Aliases to Tarantool connection objects mapping."""

_spaces = {}
"""Aliases to Tarantool space objects mapping."""

DEFAULT_ALIAS = 'default'

DEFAULT_HOST = 'localhost'

DEFAULT_PORT = 33013

DEFAULT_SPACE = 0


class ConnectionError(Exception):
    """Wraps tarantool driver connection errors."""
    pass


def register_connection(alias, host=None, port=None, **kwargs):
    """Register connection settings for alias.

    :param alias:
    :param host:
    :param port:
    :param space:

    """
    global _connection_settings

    conn_settings = {
        'host': host or DEFAULT_HOST,
        'port': int(port) or DEFAULT_PORT,
    }
    conn_settings.update(kwargs)

    _connection_settings[alias] = conn_settings

    return _connection_settings[alias]


def disconnect(alias=DEFAULT_ALIAS):
    """Close connection by alias.

    :param alias:

    """
    global _connections
    global _spaces

    if alias in _connections:
        get_connection(alias=alias).close()
        del _connections[alias]

    if alias in _spaces:
        del _spaces[alias]


def get_connection(alias=DEFAULT_ALIAS, reconnect=False):
    """Return connection by alias.

    :param alias:
    :param reconnect:

    """
    global _connections

    if reconnect:
        disconnect(alias)

    if alias not in _connections:
        alias_settings = _connection_settings.get(alias)

        if not alias_settings:
            raise ValueError(
                'Connection with alias {alias} have not defined.'.format(
                    alias=alias
                )
            )

        try:
            conn_settings = alias_settings.copy()
            host = conn_settings.pop('host')
            port = conn_settings.pop('port')
            conn_settings.pop('space', None)

            _connections[alias] = Connection(host, port, **conn_settings)
        except DatabaseError as exc:
            message = 'Connect error for alias "{alias}": "{message}".'.format(
                alias=alias, message=exc
            )
            raise ConnectionError, message, sys.exc_info()[2]

    return _connections[alias]


def get_space(space, alias=DEFAULT_ALIAS, reconnect=False):
    global _spaces

    if reconnect:
        disconnect(alias)

    if space not in _spaces:
        _spaces[space] = get_connection(alias).space(space)

    return _spaces[space]


def connect(alias=DEFAULT_ALIAS, **kwargs):
    global _connections

    if alias not in _connections:
        register_connection(alias, **kwargs)

    return get_connection(alias)
