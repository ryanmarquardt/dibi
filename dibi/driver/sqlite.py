#!/usr/bin/env python

from ..common import Column
from .common import DbapiDriver, C, register, NoSuchTableError, operator
from ..error import (NoSuchTableError, NoSuchDatabaseError, TableAlreadyExists)
from ..datatype import Text, Integer, Float, Blob, DateTime

import sqlite3


@register('sqlite')
class SQLiteDriver(DbapiDriver):
    def __init__(self, path=':memory:', create=True, debug=False):
        self.path = path
        if path is None or path == ':memory:':
            path = ':memory:'
            uri = False
        else:
            path = path.replace('?', '%3f').replace('#', '%23')
            while '//' in path:
                path = path.replace('//', '/')
            path = 'file:{}?mode=rw{}'.format(
                path,
                'c' if create else '',
            )
            uri = True
        super(SQLiteDriver, self).__init__(
            sqlite3, path, sqlite3.PARSE_DECLTYPES, uri=uri)

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
            elif message == 'unable to open database file':
                raise NoSuchDatabaseError(self.path)
            elif (message.startswith('table "') and
                  message.endswith('" already exists')):
                raise TableAlreadyExists(message[7:-16])
        if isinstance(error, sqlite3.Error):
            raise Exception((error, self.last_statement))

    def map_type(self, database_type, database_size):
        return dict(
            INT=C("INT"),
            REAL=C("REAL"),
            TEXT=C("TEXT"),
            BLOB=C("BLOB"),
            DATETIME=C("TIMESTAMP"),
        )[database_type]

    def unmap_type(self, database_type):
        return dict(
            TEXT=Text,
            INTEGER=Integer,
            INT=Integer,
            REAL=Float,
            BLOB=Blob,
            TIMESTAMP=DateTime,
        )[database_type]

    def column_definition(self, column):
        if column.autoincrement:
            return C(" ").join_words(
                self.identifier(column.name),
                C("INTEGER PRIMARY KEY ASC"),
            )
        return C(" ").join_words(
            self.identifier(column.name),
            self.map_type(column.datatype.database_type,
                          column.datatype.database_size),
            C("PRIMARY KEY") if column.primarykey else None,
            C("AUTO_INCREMENT") if column.autoincrement else None,
        )

    def list_tables(self):
        return (name for (name,) in self.execute_ro(
            C("SELECT name FROM sqlite_master WHERE type='table'")))

    def list_columns(self, table):
        rows = self.execute_ro(C("PRAGMA table_info({})").format(
            self.identifier(table))).fetchall()
        # An empty result set indicates the table doesn't exist
        if not rows:
            raise NoSuchTableError(table)
        # name, data type, can be NULL, default, pk (0 or index)
        for cid, name, type, notnull, default, pk in rows:
            datatype = self.unmap_type(type)
            yield Column(
                None, None, name, datatype, primarykey=(pk > 0),
                autoincrement=False,  # TODO: Detect rowid fields
            )

    class operators(DbapiDriver.operators):
        SUM = operator("total({})")
