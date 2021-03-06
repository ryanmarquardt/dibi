#!/usr/bin/env python

from ..common import Column, Filter
from ..error import NoSuchTableError

from abc import ABCMeta, abstractmethod
from contextlib import contextmanager
import logging


class CleanSQL(str):
    def __new__(cls, original):
        return str.__new__(cls, cls.sanitize(original))

    def __init__(self, original):
        self.original = original

    @staticmethod
    def sanitize(value):
        return value

    def join_format(self, joiner, *iterators):
        if not isinstance(joiner, CleanSQL):
            raise TypeError("Joiner is not clean")
        values = list(value for iterator in iterators for value in iterator)
        if any(not isinstance(value, CleanSQL) for value in values):
            raise TypeError("One or more values are not clean")
        return self.__class__(
            str.format(self, joiner.join(values)))

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

    def replace(self, find, new):
        return self.__class__(str.replace(self, find, new))

    def __mul__(self, other):
        return self.__class__(str.__mul__(self, other))

    def __repr__(self):
        return 'C({!r})'.format(str(self))


C = CleanSQL


class Driver(metaclass=ABCMeta):
    def __init__(self):
        self.features = set()

    @abstractmethod
    def handle_exception(self, error):
        """
        Process, convert, or ignore errors raised by interface.
        """
        return

    @contextmanager
    def catch_exception(self):
        """
        Wraps calls to underlying data store, to intercept errors.
        """
        try:
            yield
        except Exception as error:
            self.handle_exception(error)
            raise

    @abstractmethod
    def connect(self, address, *parameters, **kw_parameters):
        """
        Connect to the database and return the interface object.
        """
        return

    # Table schema methods

    @abstractmethod
    def create_table(self, name, columns, force_create):
        """
        Add a table to the database schema.
        """
        return

    @abstractmethod
    def list_tables(self):
        """
        Returns a list of all table identifiers.
        """
        return

    def rename_table(self, name, new_name):
        """
        Change the operational name of a table.

        This method is an optional extension. It should be implemented if
        possible, but its absence does not prevent normal operation.
        """
        raise NotImplementedError

    @abstractmethod
    def drop_table(self, name, ignore_absence):
        """
        Remove a table and all of its data from the database schema.
        """
        return

    # Column schema methods

    def add_column(self, table_name, column):
        """
        Add a column to a previously defined table.

        This method is an optional extension. It should be implemented if
        possible, but its absence does not prevent normal operation.
        """
        raise NotImplementedError

    @abstractmethod
    def list_columns(self, table):
        """
        Returns a list of Columns in table.
        """
        return

    def rename_column(self, table_name, column_name, new_name):
        """
        Change the operational name of a column.

        This method is an optional extension. It should be implemented if
        possible, but its absence does not prevent normal operation.
        """
        raise NotImplementedError

    def drop_column(self, table_name, column_name):
        """
        Remove a column from an existing table.

        This method is an optional extension. It should be implemented if
        possible, but its absence does not prevent normal operation.
        """
        raise NotImplementedError

    # Row/object methods

    @abstractmethod
    def insert(self, table, values):
        """
        Add a new row to table with values and return its primary key.
        """
        return

    @abstractmethod
    def select(self, tables, criteria, columns, distinct):
        """
        Return rows of columns from tables which match criteria.
        """
        return

    @abstractmethod
    def update(self, table, criteria, values):
        """
        Set new values for rows which match critera.
        """
        return

    @abstractmethod
    def delete(self, tables, criteria):
        """
        Delete rows which match criteria.
        """
        return


def operator(string):
    def operation(*args):
        return C(string.format(*args))
    return operation


class DbapiDriver(Driver):
    """
    Driver subclass for writing DBAPI compatible drivers.
    """
    def __init__(self, dbapi_module, *args, **kwargs):
        # Fail early if these required attributes aren't present
        self.identifier_quote
        try:
            self.identifier_quote_escape
        except AttributeError:
            self.identifier_quote_escape = self.identifier_quote * 2

        super(DbapiDriver, self).__init__()
        self.dbapi_module = dbapi_module
        with self.catch_exception():
            self.connection = self.connect(*args, **kwargs)
        self.transaction_depth = 0

    def connect(self, *args, **kwargs):
        return self.dbapi_module.connect(*args, **kwargs)

    def placeholders(self, values):
        if self.dbapi_module.paramstyle == 'qmark':
            return (values.keys(),
                    (C("?") for key in values),
                    list(values.values()))
        elif self.dbapi_module.paramstyle == 'format':
            return (values.keys(),
                    (C("%s") for key in values),
                    list(values.values()))
        elif self.dbapi_module.paramstyle == 'numeric':
            return (values.keys(),
                    (C(":{}").format(C(i)) for i, key in enumerate(values)),
                    list(values.values()))
        elif self.dbapi_module.paramstyle == 'named':
            return (values.keys(),
                    (C(":{}").format(C(key)) for key in values),
                    values)
        elif self.dbapi_module.paramstyle == 'pyformat':
            return (values.keys(),
                    (C("%({})s").format(C(key)) for key in values),
                    values)

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    @contextmanager
    def transaction(self):
        self.transaction_depth += 1
        error = None
        try:
            with self.catch_exception():
                yield self
        finally:
            self.transaction_depth -= 1
            if not self.transaction_depth:
                if error is None:
                    self.commit()
                else:
                    self.rollback()

    @classmethod
    def construct_statement(cls, *words):
        """
        Build a SQL statement from the provided list of strings.

        Any non-true parameter is omitted. All other parameters must be
        instances of CleanSQL.

        >>> DbapiDriver.construct_statement(
        ...   C("DELETE FROM"), C("table")
        ... )
        'DELETE FROM table;'

        The paradigm "word if condition else None" can conditionally drop
        terms.

        >>> def create_table(temporary):
        ...   table = 'a'
        ...   return DbapiDriver.construct_statement(
        ...     C("CREATE"), C("TEMPORARY") if temporary else None,
        ...     C("TABLE"), C(table), C("(x INT)"),
        ...   )

        >>> create_table(False)
        'CREATE TABLE a (x INT);'

        >>> create_table(True)
        'CREATE TEMPORARY TABLE a (x INT);'
        """
        return str(C('{};').join_format(
            C(' '), (word for word in words if word)))

    def execute(self, *words, **kwargs):
        """
        Execute a SQL statement.

        If errors are not encountered, changes will be committed. If they are
        the action will be rolled back.
        """
        with self.transaction():
            return self.execute_ro(*words, **kwargs)

    def execute_ro(self, *words, **kwargs):
        """
        Execute a SQL statement without initiating a transaction.
        """
        values = kwargs.pop('values', ())
        if kwargs:
            raise TypeError("execute_ro() got an unexpected keyword argument "
                            "'{}'".format(kwargs.popitem()[0]))
        self.last_statement = self.construct_statement(*words)
        self.last_values = values
        cursor = self.connection.cursor()
        try:
            cursor.execute(self.last_statement, self.last_values)
        except Exception:
            raise
        return cursor

    @abstractmethod
    def map_type(self, database_type, database_size):
        return

    @abstractmethod
    def unmap_type(self, database_type):
        return

    # Syntax cleansers

    def literal(self, value):
        if value is None:
            return C('NULL')
        elif isinstance(value, str):
            return C("'{}'").format(C(value.replace("'", "''")))
        elif isinstance(value, int):
            return C(value)
        raise TypeError("Can't convert {!r} to literal".format(value))

    def expression(self, value):
        if isinstance(value, Column):
            return C("{}.{}").format(self.identifier(value.table.name),
                                     self.identifier(value.name))
        elif isinstance(value, Filter):
            operator = getattr(self.operators, value.operator)
            return operator(*(self.expression(arg) for arg in value.arguments))
        else:
            return self.literal(value)

    def identifier(self, value):
        name = C(value).replace(self.identifier_quote,
                                self.identifier_quote_escape)
        return C("{0}{1}{0}").format(self.identifier_quote, name)

    def column_definition(self, column):
        return C(" ").join_words(
            self.identifier(column.name),
            self.map_type(column.datatype.database_type,
                          column.datatype.database_size),
            C("PRIMARY KEY") if column.primarykey else None,
            C("AUTO_INCREMENT") if column.autoincrement else None,
        )

    # Schema methods

    def create_table(self, table, columns, force_create):
        return self.execute(
            C("CREATE TABLE"),
            C("IF NOT EXISTS") if force_create else None,
            self.identifier(table.name),
            C("({})").join_format(C(", "), (
                self.column_definition(column) for column in columns)),
        )

    def drop_table(self, table, ignore_absence):
        return self.execute(
            C("DROP TABLE"),
            C("IF EXISTS") if ignore_absence else None,
            self.identifier(table.name)
        )

    # Row methods

    def insert(self, table, values):
        names, placeholders, values = self.placeholders(values)
        cursor = self.execute(
            C("INSERT INTO"),
            self.identifier(table.name),
            C("({})").join_format(
                C(", "), (self.identifier(key) for key in names)),
            C("VALUES"),
            C("({})").join_format(C(", "), placeholders),
            values=values
        )
        return cursor.lastrowid

    def select(self, tables, criteria, columns, distinct):
        return self.execute_ro(
            C("SELECT"),
            C("DISTINCT") if distinct else None,
            C(", ").join(C("{}.{}").format(
                self.identifier(column.table.name),
                self.identifier(column.name)
            ) for column in columns),
            C("FROM"),
            C(", ").join(self.identifier(table.name) for table in tables),
            C("WHERE") if criteria else None,
            self.expression(criteria) if criteria else None,
        )

    def update(self, table, criteria, values):
        names, placeholders, values = self.placeholders(values)
        pairs = zip(names, placeholders)
        self.execute(
            C("UPDATE"),
            self.identifier(table.name),
            C("SET"),
            C(", ").join(
                C("{}={}").format(
                    self.identifier(name),
                    placeholder,
                ) for name, placeholder in pairs
            ),
            C("WHERE") if criteria else None,
            self.expression(criteria) if criteria else None,
            values=values,
        )

    def delete(self, tables, criteria):
        self.execute(
            C("DELETE FROM"),
            C(", ").join(self.identifier(table.name) for table in tables),
            C("WHERE") if criteria else None,
            self.expression(criteria) if criteria else None,
        )

    class operators:

        # Logical operators

        AND = operator("({} AND {})")
        OR = operator("({} OR {})")
        NOT = operator("(NOT {})")

        # Numerical comparisons

        def EQUAL(a, b):
            """

            >>> print(DbapiDriver.operators.EQUAL(1, 2))
            (1=2)

            >>> print(DbapiDriver.operators.EQUAL(1, 'NULL'))
            (1 IS NULL)
            """
            return (C("({} IS {})".format(a, b)) if 'NULL' in (a, b)
                    else C("({}={})".format(a, b)))

        def NOTEQUAL(a, b):
            """

            >>> print(DbapiDriver.operators.NOTEQUAL(1, 2))
            (1!=2)

            >>> print(DbapiDriver.operators.NOTEQUAL(1, 'NULL'))
            (1 IS NOT NULL)
            """
            return (C("({} IS NOT {})".format(a, b)) if 'NULL' in (a, b)
                    else C("({}!={})".format(a, b)))

        GREATERTHAN = operator("({} > {})")
        GREATEREQUAL = operator("({} >= {})")
        LESSTHAN = operator("({} < {})")
        LESSEQUAL = operator("({} <= {})")

        # Mathematical operators

        ADD = operator("({} + {})")
        SUBTRACT = operator("({} - {})")
        MULTIPLY = operator("({} * {})")
        DIVIDE = operator("({} / {})")
        NEGATIVE = operator("(-{})")
        MODULO = operator("({} % {})")
        LEFTSHIFT = operator("({} << {})")
        RIGHTSHIFT = operator("({} >> {})")

        # Aggregate functions

        SUM = operator("sum({})")
        AVERAGE = operator("avg({})")
        MAXIMUM = operator("max({})")
        MINIMUM = operator("min({})")
        COUNT = operator("count({})")


registry = dict()


def register(name_or_class, class_=None):
    if class_ is not None:
        registry[name_or_class] = class_
    else:
        def decorated(class_):
            registry[name_or_class] = class_
            return class_
        return decorated
