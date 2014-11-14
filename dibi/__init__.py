#!/usr/bin/env python

"""

>>> mydb = DB()

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
>>> orders.insert(amount=450, quantity=11, date='2000-04-10')

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

>>> len(orders.select())
2

>>> orders.update(quantity=5)

>>> for amount, quantity, date in orders.select():
...   print("${:.02f} ; {} ; {}".format(int(amount)/100., quantity, date))
$1.00 ; 5 ; 2000-01-01
$4.50 ; 5 ; 2000-04-10

>>> orders.delete()

>>> len(orders.select())
0

>>> orders.drop()

>>> orders in mydb.tables
False

>>> orders.insert(amount=100, quantity=2, date='2000-01-02')
Traceback (most recent call last):
 ...
dibi.NoSuchTableError: Table 'orders' does not exist

"""


from .collection import Collection, OrderedCollection
from .datatype import DataType, Integer, Float, Text, Blob, DateTime, Date

import datetime
import sqlite3


class NoSuchTableError(NameError):
    def __init__(self, table_name):
        super(NoSuchTableError, self).__init__(
            "Table {!r} does not exist".format(table_name))
        self.name = table_name


class NoColumnsError(ValueError):
    pass


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

    def __len__(self):
        return len(self.cursor.fetchall())

    def __repr__(self):
        return "<Selection({})>".format(", ".join(
            repr(column) for column in self.columns))


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
                columns.extend(table.columns)
        return Selection(
            self.db, columns, self.tables,
            self if isinstance(self, Filter) else None,
            distinct,
        )

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

    def __and__(self, other):
        return Filter(self.db, 'AND', self, other)

    def __or__(self, other):
        return Filter(self.db, 'OR', self, other)

    def __not__(self):
        return Filter(self.db, 'NOT', self)

    def __eq__(self, other):
        return Filter(self.db, 'EQUAL', self, other)


class Column(Filter):
    def __init__(self, db, table, name, datatype, primarykey):
        self.table = table
        self.name = name
        self.datatype = datatype
        self.primarykey = primarykey
        Filter.__init__(self, db, 'ID', self)

    def __key__(self):
        return self.name

    def __repr__(self):
        return str(self)

    def __str__(self):
        return "{!r}.{!r}".format(self.table.name, self.name)


class Table(Selectable):
    def __init__(self, db, name, primarykey=None):
        self.name = name
        Selectable.__init__(self, db, {self})
        self.columns = OrderedCollection(lambda col: col.name)
        if primarykey:
            self.primarykey = self.add_column(
                primarykey, Integer, primarykey=True)

    def __hash__(self):
        return hash(self.name)

    def __str__(self):
        return self.name

    def __repr__(self):
        return "Table({!r})".format(self.name)

    def add_column(self, name, datatype=DataType, primarykey=False):
        return self.columns.add(
            Column(self.db, self, name, datatype, primarykey),
            replace=False)

    def save(self, force_create=False):
        if not self.columns:
            raise NoColumnsError(
                "Cannot create table {!r} with no columns".format(self.name))
        self.db.driver.create_table(self, self.columns, force_create)

    def drop(self, ignore_absence=True):
        self.db.driver.drop_table(self, ignore_absence)
        self.db.tables.discard(self)

    def insert(self, **values):
        self.db.driver.insert(self, values)

    def __getattr__(self, key):
        return self.columns[key]


import dibi.driver


class DB(object):
    def __init__(self, path=':memory:'):
        self.driver = driver.get('sqlite')(path)
        self.path = path
        self.tables = Collection(lambda table: table.name)

    def __hash__(self):
        return hash(self.path)

    def add_table(self, name, primarykey=None):
        return self.tables.add(Table(self, name, primarykey=primarykey))
