#!/usr/bin/env python

from .common import DbapiDriver, C, register

from .. import NoSuchTableError

import sqlite3


@register('sqlite')
class SQLiteDriver(DbapiDriver):
    def __init__(self, path=':memory:'):
        super(SQLiteDriver, self).__init__(
            sqlite3, path, sqlite3.PARSE_DECLTYPES)
        self.path = path

    identifier_quote = C('"')

    def __repr__(self):
        return "SQLiteDriver(path={!r})".format(self.path)

    @classmethod
    def parse_uri_path(cls, path):
        if not path or path == ':memory:':
            return dict(path=':memory:')
        return dict(path=path)

    def handle_exception(self, error):
        if isinstance(error, sqlite3.OperationalError):
            message = error.args[0]
            if message.startswith('no such table: '):
                table = message[15:]
                raise NoSuchTableError(table)
            elif message.endswith(': syntax error'):
                raise SyntaxError((message, self.last_statement))
        if isinstance(error, (sqlite3.OperationalError,
                              sqlite3.ProgrammingError)):
            raise Exception((error, self.last_statement))

    def map_type(self, database_type, database_size):
        try:
            return dict(
                INT=C("INT"),
                REAL=C("REAL"),
                TEXT=C("TEXT"),
                BLOB=C("BLOB"),
                DATETIME=C("TIMESTAMP"),
            )[database_type]
        except KeyError:
            raise ValueError(
                "datatype {!r}({}) did not produce a valid SQLite type".format(
                    database_type, database_size))

    def unmap_type(self, database_type):
        return dict(
            TEXT=Text,
            INTEGER=Integer,
            REAL=Float,
            BLOB=Blob,
            TIMESTAMP=DateTime,
        )
