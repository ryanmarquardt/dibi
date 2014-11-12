#!/usr/bin/env python

import dibi
from dibi import DB, Table, NoColumnsError
from dibi.datatype import Integer, Text, Date

import datetime

import doctest
doctest.testmod(dibi)

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
        self.failed = False

    def __call__(self, comment):
        self.comment = comment
        return self

    def __enter__(self):
        return self

    def report_failure(self, tb, comment, condition, *args):
        self.failed = True
        logging.error("{message} in {comment!r}".format(
            tb=tb,
            comment=comment,
            message=condition.format(*args),
        ))

    def report_error(self, tb, comment, exc, obj):
        self.failed = True
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
    with test.raises(NoColumnsError):
        test.db.add_table('empty').save()

with test("Table names are quoted to prevent SQL injection"):
    inject = test.db.add_table('"; DROP TABLE orders;')
    inject.add_column('id', Integer)
    inject.save()

with test("Define table with autoincrement column"):
    autoinc = test.db.add_table("autoinc", primarykey='id')
    autoinc.save()
    test.equal(len(autoinc.columns), 1)

if test.failed:
    exit(1)
