

class Error(Exception):
    pass


class NoSuchDatabaseError(Error):
    pass


class NoSuchTableError(NameError, Error):
    def __init__(self, table_name):
        super(NoSuchTableError, self).__init__(
            "Table {!r} does not exist".format(table_name))
        self.name = table_name


class TableAlreadyExists(Error):
    pass


class NoColumnsError(ValueError, Error):
    pass


class ConnectionError(Error):
    pass


class AuthenticationError(Error):
    pass
