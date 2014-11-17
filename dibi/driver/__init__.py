
from . import common
from . import sqlite

try:
    from . import mysql
except ImportError:
    pass

registry = common.registry

get = common.registry.__getitem__
