#!/usr/bin/env python

"""

>>> mydb = DB.connect('sqlite')

>>> orders = mydb.add_table('orders')

>>> orders.add_column('amount', Integer)
'orders'.'amount'

>>> orders.add_column('quantity', Text)
'orders'.'quantity'

>>> orders.add_column('date', Date)
'orders'.'date'

>>> orders in mydb.tables
True

>>> orders.save()

>>> orders.insert(amount=100, quantity=2, date='2000-01-01')
1

>>> orders.insert(amount=450, quantity=11, date='2000-04-10')
2

>>> allrows = orders.select()

>>> allrows
<Selection('orders'.'amount', 'orders'.'quantity', 'orders'.'date')>

>>> for amount, quantity, date in allrows:
...   print("${:.02f} ; {} ; {}".format(int(amount)/100., quantity, date))
$1.00 ; 2 ; 2000-01-01
$4.50 ; 11 ; 2000-04-10

>>> (orders.columns['amount'] == 100)
Filter('EQUAL', 'orders'.'amount', 100)

>>> for amount, quantity, date in (orders.columns['amount'] == 100).select():
...   print("${:.02f} ; {} ; {}".format(int(amount)/100., quantity, date))
$1.00 ; 2 ; 2000-01-01

>>> (orders.amount == 100).select(orders.quantity)
<Selection('orders'.'quantity')>

>>> print(orders.db.driver.last_statement)
SELECT "orders"."quantity" FROM "orders" WHERE ("orders"."amount"=100);

>>> len(orders.select_all())
2

>>> orders.update(quantity=5)

>>> for amount, quantity, date in orders.select():
...   print("${:.02f} ; {} ; {}".format(int(amount)/100., quantity, date))
$1.00 ; 5 ; 2000-01-01
$4.50 ; 5 ; 2000-04-10

>>> orders.delete()

>>> len(orders.select_all())
0

>>> orders.drop()

>>> orders in mydb.tables
False

>>> orders.insert(amount=100, quantity=2, date='2000-01-02')
Traceback (most recent call last):
 ...
dibi.error.NoSuchTableError: Table 'orders' does not exist

"""


from .collection import Collection, OrderedCollection
from .datatype import (DataType, Integer, Float, Text, Blob, DateTime, Date,
                       AutoIncrement)
from .error import NoSuchTableError, NoColumnsError, TableAlreadyExists

import datetime
import sqlite3


class DbObject(object):
    def __init__(self, db):
        self.db = db


class Selection(DbObject):
    def __init__(self, db, columns, tables, criteria, distinct):
        DbObject.__init__(self, db)
        self.columns = columns
        self.cursor = self.db.driver.select(
            tables, criteria, columns, distinct)

    def __iter__(self):
        for row in self.cursor:
            yield row

    def __repr__(self):
        return "<Selection({})>".format(", ".join(
            repr(column) for column in self.columns if not column.implicit))

    def one(self):
        for row in self:
            return row
        return None


class Selectable(DbObject):
    def __init__(self, db, tables):
        DbObject.__init__(self, db)
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
                for column in table.columns:
                    if not column.implicit:
                        columns.append(column)
        return Selection(
            self.db, columns, self.tables,
            self if isinstance(self, Filter) else None,
            distinct,
        )

    def select_all(self, *columns, **kwargs):
        return list(self.select(*columns, **kwargs))

    def update(self, **values):
        if len(self.tables) != 1:
            raise ValueError("Can only update one table at a time")
        self.db.driver.update(
            list(self.tables)[0],
            self if isinstance(self, Filter) else None,
            values,
        )

    def delete(self):
        self.db.driver.delete(
            self.tables,
            self if isinstance(self, Filter) else None,
        )

    def count(self):
        raise NotImplementedError


def operator(identifier, order=2, reverse=False):
    def operation(*arguments):
        if len(arguments) != order:
            raise TypeError("Expected {} arguments, got {}".format(
                order, len(arguments)))
        return Filter(arguments[0].db, identifier, *(
            reversed(arguments) if reverse else arguments))
    return operation


def operator_pair(identifier):
    return (operator(identifier), operator(identifier, reverse=True))


class Filter(Selectable):
    def __init__(self, db, operator, *arguments):
        self.operator = operator
        self.arguments = arguments
        tables = set(arg.table for arg in arguments if isinstance(arg, Column))
        Selectable.__init__(self, db, tables)

    def __repr__(self):
        return 'Filter({}, {})'.format(
            repr(self.operator),
            ", ".join(repr(a) for a in self.arguments),
        )

    # Logical operators

    __and__ = operator('AND')
    __or__ = operator('OR')
    __invert__ = operator('NOT', order=1)

    # Comparisons

    __eq__ = operator('EQUAL')
    __ne__ = operator('NOTEQUAL')
    __gt__ = operator('GREATERTHAN')
    __ge__ = operator('GREATEREQUAL')
    __lt__ = operator('LESSTHAN')
    __le__ = operator('LESSEQUAL')

    # Mathematical operators

    def __add__(self, other):
        return Filter(self.db, 'ADD', self, other)

    def __radd__(self, other):
        return Filter(self.db, 'ADD', other, self)

    __neg__ = operator('NEGATIVE', order=1)
    __sub__, __rsub__ = operator_pair('SUBTRACT')
    __mul__, __rmul__ = operator_pair('MULTIPLY')
    __truediv__, __rtruediv__ = operator_pair('DIVIDE')
    __floordiv__, __rfloordiv__ = operator_pair('DIVIDE')
    __mod__, __rmod__ = operator_pair('MODULO')
    __lshift__, __rlshift__ = operator_pair('LEFTSHIFT')
    __rshift__, __rrshift__ = operator_pair('RIGHTSHIFT')

    # Aggregate functions

    sum = operator('SUM', order=1)
    average = operator('AVERAGE', order=1)
    max = operator('MAXIMUM', order=1)
    min = operator('MINIMUM', order=1)
    count = operator('COUNT', order=1)


class Column(Filter):
    def __init__(self, db, table, name, datatype, primarykey, autoincrement,
                 implicit=False):
        self.table = table
        self.name = name
        self.datatype = datatype
        self.primarykey = primarykey
        self.autoincrement = autoincrement
        self.implicit = implicit
        Filter.__init__(self, db, 'ID', self)

    def __repr__(self):
        return str(self)

    def __str__(self):
        if self.table is None:
            return repr(self.name)
        return "{!r}.{!r}".format(self.table.name, self.name)


class Table(Selectable):
    def __init__(self, db, name, primarykey=None):
        self.name = name
        Selectable.__init__(self, db, {self})
        self.columns = OrderedCollection(lambda col: col.name)
        self.primarykey = None
        if primarykey is not None:
            self.primarykey = self.add_column(
                primarykey, Integer, primarykey=True, autoincrement=True)

    def __hash__(self):
        return hash(self.name)

    def __str__(self):
        return self.name

    def __repr__(self):
        return "Table({!r})".format(self.name)

    def add_column(self, name, datatype=DataType, primarykey=False,
                   autoincrement=False):
        column = self.columns.add(
            Column(self.db, self, name, datatype, primarykey, autoincrement),
            replace=False)
        if primarykey:
            self.primarykey = column
        return column

    def save(self, force_create=False):
        if not self.columns:
            raise NoColumnsError(
                "Cannot create table {!r} with no columns".format(self.name))
        # Since no primarykey column was specified, create an implicit
        #  autoincrement column
        if self.__dict__.get('primarykey') is None:
            self.primarykey = self.add_column(
                '__id__', Integer, primarykey=True, autoincrement=True)
            self.primarykey.implicit = True
        self.db.driver.create_table(self, self.columns, force_create)

    def drop(self, ignore_absence=True):
        self.db.driver.drop_table(self, ignore_absence)
        self.db.tables.discard(self)

    def insert(self, **values):
        return self.db.driver.insert(self, values)

    def __getattr__(self, key):
        return self.columns[key]

    def __getitem__(self, key):
        return (self.__dict__['primarykey'] == key).select().one()


import dibi.driver


class DB(object):
    def __init__(self, driver):
        self.driver = driver
        self.tables = Collection(lambda table: table.name)

    @classmethod
    def connect(cls, driver_name, *args, **kwargs):
        driver = dibi.driver.get(driver_name)
        return cls(driver(*args, **kwargs))

    @classmethod
    def connect_uri(cls, uri):
        """
        Form a database connection by parsing a URI describing the database.

        Valid URIs are in the form 'scheme://path_info'.

        >>> db = DB.connect_uri('sqlite://')

        >>> db.driver
        SQLiteDriver(path=':memory:')

        >>> DB.connect_uri('http://database.com')
        Traceback (most recent call last):
         ...
        KeyError: 'http'

        >>> DB.connect_uri('not a uri')
        Traceback (most recent call last):
         ...
        ValueError: Cannot connect to invalid uri 'not a uri'
        """
        scheme, successful, remainder = uri.partition('://')
        if not successful:
            raise ValueError("Cannot connect to invalid uri {!r}".format(uri))
        driver = dibi.driver.get(scheme)
        if not hasattr(driver, 'parse_uri_path'):
            raise TypeError("Driver {!r} does not support URIs".format(scheme))
        parameters = driver.parse_uri_path(remainder)
        return cls(driver(**parameters))

    def __hash__(self):
        return hash(self.driver)

    def add_table(self, name, primarykey=None):
        if name in self.tables:
            raise TableAlreadyExists(name)
        return self.tables.add(Table(self, name, primarykey=primarykey))

    def find_table(self, name):
        """
        Attemt to discover a table which exists, but dibi doesn't know about

        >>> DB.connect('sqlite').find_table('missing')
        Traceback (most recent call last):
         ...
        NoSuchTableError: Table 'missing' does not exist
        """
        columns = self.driver.list_columns(name)
        table = Table(self, name)
        for column in columns:
            column.table = table
            column.db = self
            table.columns.add(column)
        return table

    def __repr__(self):
        return "<DB({!r})>".format(self.driver)

connect = DB.connect
