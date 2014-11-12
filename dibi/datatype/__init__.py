
import datetime


class DataType(object):
    @staticmethod
    def serialize(value):
        return value

    @staticmethod
    def deserialize(value):
        return value


class Text(DataType):
    database_type = 'TEXT'
    database_size = 512
    serialize = str
    deserialize = str


class Integer(DataType):
    database_type = 'INT'
    database_size = 64
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


class Float(DataType):
    pass


class Blob(DataType):
    pass
