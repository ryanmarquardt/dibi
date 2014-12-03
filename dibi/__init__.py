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


from .datatype import DataType, Integer, Float, Text, Blob, DateTime, Date
from .collection import Collection
from .error import NoSuchTableError, TableAlreadyExists
from .common import Selection, Selectable, Filter, Column, Table
from . import driver


class DB(object):
    def __init__(self, driver):
        self.driver = driver
        self.tables = Collection(lambda table: table.name)

    @classmethod
    def connect(cls, driver_name, *args, **kwargs):
        driver_class = driver.get(driver_name)
        return cls(driver_class(*args, **kwargs))

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
        driver_class = driver.get(scheme)
        if not hasattr(driver_class, 'parse_uri_path'):
            raise TypeError("Driver {!r} does not support URIs".format(scheme))
        parameters = driver_class.parse_uri_path(remainder)
        return cls(driver_class(**parameters))

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
