#!/usr/bin/env python

"""

>>> mydb = DB()

>>> orders = mydb.add_table('orders')

>>> orders.add_column('amount', Integer)
"orders"."amount"

>>> orders.add_column('quantity', Text)
"orders"."quantity"

>>> orders.add_column('date', Date)
"orders"."date"

>>> orders in mydb.tables
True

>>> orders.save()

>>> orders.insert(amount=100, quantity=2, date='2000-01-01')
>>> orders.insert(amount=450, quantity=11, date='2000-04-10')

>>> allrows = orders.select()

>>> allrows
<Selection("orders"."amount", "orders"."quantity", "orders"."date")>

>>> for amount, quantity, date in allrows:
...   print("${:.02f} ; {} ; {}".format(int(amount)/100., quantity, date))
$1.00 ; 2 ; 2000-01-01
$4.50 ; 11 ; 2000-04-10

>>> (orders.columns['amount'] == 100)
Filter('EQUAL', "orders"."amount", 100)

>>> for amount, quantity, date in (orders.columns['amount'] == 100).select():
...   print("${:.02f} ; {} ; {}".format(int(amount)/100., quantity, date))
$1.00 ; 2 ; 2000-01-01

>>> (orders.amount == 100).select(orders.quantity)
<Selection("orders"."quantity")>

>>> print(orders.db.driver.last_statement)
SELECT "orders"."quantity" FROM "orders" WHERE ("orders"."amount"=100);

>>> len(orders.select())
2

>>> orders.delete()

>>> len(orders.select())
0

>>> orders.drop()

>>> orders in mydb.tables
False

>>> orders.insert(amount=100, quantity=2, date='2000-01-02')
Traceback (most recent call last):
 ...
NoSuchTable: orders

"""


from dibi.collection import Collection, OrderedCollection

from abc import ABCMeta, abstractmethod

import datetime
import sqlite3


class NoSuchTable(Exception):
    pass


class NoColumns(Exception):
    pass


class DataType(object):
    @staticmethod
    def serialize(value):
        return value

    @staticmethod
    def deserialize(value):
        return value


class Text(DataType):
    database_type = 'TEXT'
    serialize = str
    deserialize = str


class Integer(DataType):
    database_type = 'INT'
    serialize = int


class Date(Text):
    @staticmethod
    def serialize(value):
        return value.isoformat()

    @staticmethod
    def deserialize(value):
        return datetime.datetime.strptime(value, "%Y-%m-%d").date()


class DateTime(Text):
    @staticmethod
    def serialize(value):
        return value.isoformat()

    @staticmethod
    def deserialize(value):
        return datetime.datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%f")


class CleanSQL(str):
    def __new__(cls, original):
        return str.__new__(cls, cls.sanitize(original))

    def __init__(self, original):
        self.original = original

    @staticmethod
    def sanitize(value):
        return value

    def join_format(self, joiner, values):
        if not isinstance(joiner, CleanSQL):
            raise TypeError("Joiner is not clean")
        values = list(values)
        if any(not isinstance(value, CleanSQL) for value in values):
            raise TypeError("One or more values are not clean")
        return CleanSQL(
            self.original.format(joiner.join(values)))

    def join(self, values):
        values = list(values)
        if any(not isinstance(value, CleanSQL) for value in values):
            raise TypeError("one or more values are not clean")
        return self.__class__(str.join(self, values))

    def join_words(self, *words):
        return self.join(word for word in words if word)

    def format(self, *args, **kwargs):
        if any(not isinstance(value, CleanSQL) for value in args):
            raise TypeError("one or more values are not clean")
        if any(not isinstance(value, CleanSQL) for value in kwargs.values()):
            raise TypeError("one or more values are not clean")
        return self.__class__(str.format(self, *args, **kwargs))

    def __repr__(self):
        return 'C({!r})'.format(str(self))


C = CleanSQL


class Identifier(CleanSQL):
    @staticmethod
    def sanitize(value):
        return "{0}{1}{0}".format('"', str(value).replace('"', '""'))


def op(string):
    def operation(*args):
        return string.format(*args)
    return operation


class Expression(CleanSQL):
    @classmethod
    def sanitize(cls, value):
        if isinstance(value, Filter):
            operator = value[0]
            arguments = value[1:]
            return cls.operator[operator](
                *(arg for arg in arguments))

    operator = dict(
        EQUAL=op("({}={})"),
        AND=op("({} AND {})"),
    )


class Selection(object):
    def __init__(self, db, columns, tables, where, distinct):
        self.db = db
        self.columns = columns
        self.cursor = self.db.driver.select(columns, tables, where, distinct)

    def __iter__(self):
        for row in self.cursor:
            yield row

    def __len__(self):
        return len(self.cursor.fetchall())

    def __repr__(self):
        return "<Selection({})>".format(", ".join(
            repr(column) for column in self.columns))


class Selectable(object):
    def __init__(self, db, tables):
        self.db = db
        if not isinstance(tables, set):
            raise TypeError(("Expected tables as set, "
                             "got {!r}").format(type(tables)))
        self.tables = tables

    def select(self, *columns, **kwargs):
        distinct = kwargs.pop('distinct', False)
        if kwargs:
            raise TypeError("select() got an unexpected keyword argument "
                            "{!r}".format(kwargs.popitem()[0]))

        if not columns:
            columns = []
            for table in self.tables:
                columns.extend(table.columns)
        return Selection(
            self.db, columns, self.tables,
            self if isinstance(self, Filter) else None,
            distinct,
        )

    def update(self, **values):
        raise NotImplementedError

    def delete(self):
        self.db.driver.delete(
            self.tables,
            self if isinstance(self, Filter) else None,
        )

    def count(self):
        raise NotImplementedError


class Filter(tuple, Selectable):
    def __new__(cls, db, operator, *arguments):
        return super(Filter, cls).__new__(cls, (operator,) + arguments)

    def __init__(self, db, operator, *arguments):
        tables = set(arg.table for arg in arguments if isinstance(arg, Column))
        Selectable.__init__(self, db, tables)

    def __repr__(self):
        return 'Filter({})'.format(", ".join(repr(a) for a in self))

    def __and__(self, other):
        return Filter(self.db, 'AND', self, other)

    def __or__(self, other):
        return Filter(self.db, 'OR', self, other)

    def __not__(self):
        return Filter(self.db, 'NOT', self)

    def __eq__(self, other):
        return Filter(self.db, 'EQUAL', self, other)


class Column(Filter):
    def __init__(self, table, name, datatype, primarykey):
        self.db = table.db
        self.table = table
        self.name = name
        self.datatype = datatype
        self.primarykey = primarykey

    def __key__(self):
        return self.name

    def __repr__(self):
        return str(self)

    def __str__(self):
        return "{}.{}".format(Identifier(self.table), Identifier(self.name))


class Table(Selectable):
    def __init__(self, db, name, primarykey=None):
        self.db = db
        self.name = name
        self.columns = OrderedCollection()
        Selectable.__init__(self, db, {self})
        if primarykey:
            self.primarykey = self.add_column(
                primarykey, Integer, primarykey=True)

    def __hash__(self):
        return hash((self.db, self.name))

    def __key__(self):
        return self.name

    def __str__(self):
        return self.name

    def __repr__(self):
        return "Table({!r})".format(self.name)

    def add_column(self, name, datatype=DataType, primarykey=False):
        column = Column(self, name, datatype, primarykey)
        self.columns.add(column)
        return column

    def save(self, force_create=False):
        if not self.columns:
            raise NoColumns("Cannot create table {!r} with no columns".format(
                self.name))
        self.db.driver.create_table(self, self.columns, force_create)

    def drop(self, ignore_absence=True):
        self.db.driver.drop_table(self, ignore_absence)
        self.db.tables.discard(self)

    def insert(self, **values):
        self.db.driver.insert(self, values)

    def __getattr__(self, key):
        return self.columns[key]


class Driver(metaclass=ABCMeta):
    def __init__(self, *args, **kwargs):
        self.connection = self.connect(*args, **kwargs)

    @abstractmethod
    def handle_exception(self, error_class, error):
        """

        """
        return

    @abstractmethod
    def connect(self, address, *parameters, **kw_parameters):
        """

        """
        return

    @abstractmethod
    def insert(self, table, values):
        return

    def execute(self, *words, **kwargs):
        values = list(kwargs.pop('values', ()))
        if kwargs:
            raise TypeError("DB.execute() got an unexpected keyword argument "
                            "'{}'".format(kwargs.popitem()[0]))
        self.last_statement = statement = \
            C('{};').join_format(C(' '), (word for word in words if word))
        error = None
        try:
            return self.connection.execute(statement, values)
        except Exception as error:
            self.handle_exception(error)
            raise


class SQLiteDriver(Driver):
    def __init__(self, path=':memory:'):
        super(SQLiteDriver, self).__init__(path)
        self.path = path

    def connect(self, path):
        return sqlite3.connect(path)

    def placeholders(self, values):
        return (C("?") for key in values)

    def handle_exception(self, error):
        if isinstance(error, sqlite3.OperationalError):
            message = error.args[0]
            if message.startswith('no such table: '):
                table = message[15:]
                raise NoSuchTable(table)
            elif message.endswith(': syntax error'):
                raise SyntaxError((message, self.last_statement))
        raise error_class(*error.args)

    # Schema methods

    def column_definition(self, column):
        if column.datatype.database_type not in {
                "", "TEXT", "REAL", "INT", "BLOB"}:
            raise ValueError(
                "datatype {!r} did not produce a valid SQLite type".format(
                    self.datatype))
        return C(" ").join_words(
            Identifier(column.name),
            CleanSQL(column.datatype.database_type),
            C("PRIMARY KEY") if column.primarykey else None,
        )

    def create_table(self, table, columns, force_create):
        return self.execute(
            C("CREATE TABLE"),
            C("IF NOT EXISTS") if force_create else None,
            Identifier(table.name),
            C("({})").join_format(C(", "), (
                self.column_definition(column) for column in columns)),
        )

    def drop_table(self, table, ignore_absence):
        return self.execute(
            C("DROP TABLE"),
            C("IF EXISTS") if ignore_absence else None,
            Identifier(table.name)
        )

    # Row methods

    def insert(self, table, values):
        return self.execute(
            C("INSERT INTO"),
            Identifier(table.name),
            C("({})").join_format(
                C(", "), (Identifier(key) for key in values.keys())),
            C("VALUES"),
            C("({})").join_format(C(", "), self.placeholders(values)),
            values=values.values()
        )

    def select(self, columns, tables, where, distinct):
        return self.execute(
            C("SELECT"),
            C("DISTINCT") if distinct else None,
            C(", ").join(C("{}.{}").format(
                Identifier(column.table),
                Identifier(column.name)
            ) for column in columns),
            C("FROM"),
            C(", ").join(Identifier(table.name) for table in tables),
            C("WHERE") if where else None,
            Expression(where) if where else None,
        )

    def delete(self, tables, where):
        self.execute(
            C("DELETE FROM"),
            C(", ").join(Identifier(table.name) for table in tables),
            C("WHERE") if where else None,
            Expression(where) if where else None,
        )


class DB(object):
    def __init__(self, path=':memory:'):
        self.driver = SQLiteDriver(path)
        self.path = path
        self.tables = Collection()

    def __hash__(self):
        return hash(self.path)

    def add_table(self, name, primarykey=None):
        self.tables.add(Table(self, name, primarykey=primarykey))
        return self.tables[name]

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=['test'], nargs='?', default=None)

    arguments = parser.parse_args()

    if arguments.command == 'test':
        import doctest
        doctest.testmod()

        import dibi.collection
        doctest.testmod(dibi.collection)

        import logging
        import traceback
        from contextlib import contextmanager

        class Failure(Exception):
            pass

        class Result(object):
            def __init__(self, tb, title, message, *args):
                self.tb = tb
                self.title = title
                self.message = message
                self.args = args

            def __str__(self):
                return "Failure in {title!r}:\n  {message}".format(
                    title=self.title,
                    message=self.message.format(*self.args),
                )

        class Test(object):
            def __init__(self, *args, **kwargs):
                self.failures = []
                self.errors = []

            def __call__(self, comment):
                self.comment = comment
                return self

            def __enter__(self):
                return self

            def report_failure(self, tb, comment, condition, *args):
                self.failures.append(Result(tb, comment, condition, *args))
                logging.error("{message} in {comment!r}".format(
                    tb=tb,
                    comment=comment,
                    message=condition.format(*args),
                ))

            def report_error(self, tb, comment, exc, obj):
                self.errors.append(Result(tb, comment, "U"))
                logging.critical(("Unhandled exception in {comment!r}:\n"
                                  "    {error}").format(
                    tb=tb,
                    comment=comment,
                    error=''.join(traceback.format_exception_only(exc, obj)),
                ))

            def __exit__(self, exc, obj, tb):
                comment = self.comment
                del self.comment
                if exc is Failure:
                    self.report_failure(tb, comment, *obj.args)
                    return True
                if exc is not None:
                    self.report_error(tb, comment, exc, obj)
                    return True

            def instance(self, value, baseclass):
                if not isinstance(value, baseclass):
                    raise Failure('{0} is instance of {1}', value, baseclass)

            def equal(self, first, second):
                if first != second:
                    raise Failure('{0} != {1}', first, second)

            def identical(self, first, second):
                if first is not second:
                    raise Failure('{0} is not {1}', first, second)

            def contains(self, container, item):
                if item not in container:
                    raise Failure('{1!r} not in {0!r}', container, item)

            @contextmanager
            def raises(self, exception):
                try:
                    yield
                except exception as error:
                    return
                raise Failure('{0} not raised', exception)

        test = Test()

        test.db = DB()

        with test("Create a table"):
            orders = test.db.add_table('orders')
            test.instance(orders, Table)

        with test("Add column to a table"):
            amount = orders.add_column('amount', Integer)
            test.identical(amount.table, orders)

        with test("Integer datatype serialization"):
            test.equal(amount.datatype.serialize('123'), 123)
            test.equal(amount.datatype.deserialize(123), 123)

        with test("Add text column"):
            quantity = orders.add_column('quantity', Text)

        with test("Add date column"):
            date = orders.add_column('date', Date)

        with test("Date datatype serialization"):
            turn_of_century = datetime.date(2000, 1, 1)
            test.equal(date.datatype.serialize(turn_of_century),
                       "2000-01-01")
            test.equal(date.datatype.deserialize("2000-01-01"),
                       turn_of_century)

        with test("Defined tables are stored in DB.tables"):
            test.contains(test.db.tables, orders)

        with test("Save a table to create it"):
            orders.save()

        with test("Create a table with no columns"):
            with test.raises(NoColumns):
                test.db.add_table('empty').save()

        with test("Table names are quoted to prevent SQL injection"):
            inject = test.db.add_table('"; DROP TABLE orders;')
            inject.add_column('id', Integer)
            inject.save()

        with test("Define table with autoincrement column"):
            autoinc = test.db.add_table("autoinc", primarykey='id')
            autoinc.save()
            test.equal(len(autoinc.columns), 1)

        if test.failures or test.errors:
            exit(1)
