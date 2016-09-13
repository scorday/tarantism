from tarantool import space, connection

from tarantism.exceptions import parse_tarantool_exception


class Call(object):
    def __init__(self, connection, func_name):
        self.connection = connection
        self.name = func_name

    def __call__(self, *args, **kwargs):
        args = (self.name,) + args
        return self.connection.call(*args, **kwargs)


class Index(object):
    def __init__(self, space, index_name):
        self.connection = space.connection
        self.space = space
        self.name = index_name

    def __getattr__(self, item):
        func_name = 'box.space.%s.index.%s:%s' % (self.space.name, self.name, item)
        return Call(self.connection, func_name)

    def call(self, *args, **kwargs):
        return self.connection.call(*args, **kwargs)

    def select(self, *args, **kwargs):
        return self.space.select(*args, index=self.name, **kwargs)


class Space(space.Space):
    def __init__(self, connection, space_name):
        self.name = space_name
        super(Space, self).__init__(connection, space_name)

    def __getattr__(self, item):
        return Call(self.connection, 'box.space.%s:%s' % (self.name, item))

    def index(self, index_name):
        return Index(self, index_name)

    def call(self, *args, **kwargs):
        return self.connection.call(*args, **kwargs)


class Connection(connection.Connection):
    def space(self, space_name):
        return Space(self, space_name)

    def call(self, func_name, *args):
        try:
            return super(Connection, self).call(func_name, *args)
        except Connection.DatabaseError as e:
            raise parse_tarantool_exception(e)


