
from .common import DbapiDriver, C, register

from ..error import (NoSuchTableError, ConnectionError, AuthenticationError,
                     NoSuchDatabaseError)

import warnings

import mysql.connector as mysql


@register('mysql')
class MysqlDriver(DbapiDriver):
    """Driver for mysql databases

    mysql requires only one parameter: database, which is the name of the
    database to use.

    >>> import dibi

    """

    @property
    def engines(self):
        return {'MyISAM', 'InnoDB', 'MERGE', 'MEMORY', 'BDB', 'EXAMPLE',
                'FEDERATED', 'ARCHIVE', 'CSV', 'BLACKHOLE'}

    identifier_quote = C('`')

    def __init__(self, database, user='root', password=None, host='localhost',
                 engine='MyISAM', port=3306, debug=False):
        self.database = database
        self.user = user
        self.password = password
        with self.catch_exception():
            super(MysqlDriver, self).__init__(
                mysql, host=host, port=port, user=user,
                password=password or '', database=database)
        self.engine = engine
        self.debug = debug

    @property
    def engine(self):
        return C(self.__dict__['engine'])

    @engine.setter
    def engine(self, new):
        assert new in self.engines, 'Unknown storage engine %r' % new
        if new in {'InnoDB', 'BDB'}:
            self.features.add('transactions')
        else:
            self.features.discard('transactions')
        self.__dict__['engine'] = new

    def map_type(self, database_type, database_size):
        try:
            return dict(
                INT=C("INT"),
                REAL=C("REAL"),
                TEXT=C("VARCHAR({})").format(C(int(database_size))),
                BLOB=C("BLOB"),
                DATETIME=C("DATETIME"),
            )[database_type]
        except KeyError:
            raise ValueError(
                "datatype {!r}({}) did not produce a valid MySQL type".format(
                    database_type, database_size))

    def handle_exception(self, error):
        if isinstance(error, mysql.errors.InterfaceError):
            if error.errno == 2003:
                raise ConnectionError(error.msg)
        elif isinstance(error, mysql.errors.ProgrammingError):
            if error.errno in (1044, 1045):
                raise AuthenticationError(self.user)
            elif error.errno == 1049:
                raise NoSuchDatabaseError(self.database)
            #elif code == 1054:
                #raise KeyError(e.args[1])
        #elif isinstance(e, MySQLdb.IntegrityError):
            #code = e.args[0]
            #if code == 1062:
                #raise ValueError(e.message)
        #elif isinstance(e, MySQLdb.ProgrammingError):
            #text = e.args[1].partition("'")[2].rpartition("'")[0]
            #offset = self.lastsql.index(text)
            #raise SQLSyntaxError(self.lastsql, offset, text)

    def unmap_type(self, t):
        name, _, size = t.partition('(')
        if name in ('int', 'tinyint'):
            return int if int((size or '0 ')[:-1]) > 1 else bool
        return {'text': unicode, 'varchar': unicode,
                'timestamp': datetime.datetime, 'double': float, 'real': float,
                'blob': bytes}.get(name)

    def list_tables(self):
        return self.execute("SHOW TABLES")

    def list_columns(self, table):
        for name, v_type, null, key, default, extra in self.execute(
                "DESCRIBE %s;" % table):
            ut = self.unmap_type(v_type)
            if not ut:
                raise Exception('Unknown column type %s' % v_type)
            yield (str(name), ut, null != 'YES', default)

    def create_table(self, table, columns, force_create):
        return self.execute(
            C("CREATE"),
            C("TEMPORARY") if self.debug else None,
            C("TABLE"),
            C("IF NOT EXISTS") if force_create else None,
            self.identifier(table.name),
            C("({})").join_format(C(", "), (
                self.column_definition(column) for column in columns)),
            C("ENGINE={}").format(self.engine)
        )

    def insert_rowid(self, cursor):
        return self.connection.insert_id()

    op_SUM = staticmethod(lambda a: 'sum(%s)' % a)
    op_CONCATENATE = staticmethod(lambda a, b: 'CONCAT(%s,%s)' % (a, b))
