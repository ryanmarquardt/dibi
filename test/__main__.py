#!/usr/bin/env python

import dibi
from dibi import DB, Table, NoColumnsError
from dibi.datatype import Integer, Text, Date

from collections import OrderedDict
import configparser
import datetime
import doctest
import logging

from importlib import reload, import_module

from .driver import test_driver

import os


def split_path(path):
    parts = []
    while path:
        path, tail = os.path.split(path)
        parts.insert(0, tail)
    return parts


def find_package_module_names(package):
    if isinstance(package, str):
        package = import_module(package)
    root = os.path.dirname(package.__file__)
    for base, dirs, files in os.walk(root):
        if '__pycache__' in dirs:
            dirs.remove('__pycache__')
        for filename in sorted(files):
            if filename.endswith('.py'):
                parts = split_path(os.path.relpath(
                    os.path.join(base, filename[:-3]), root))
                if parts[-1] == '__init__':
                    del parts[-1]
                parts.insert(0, package.__name__)
                yield '.'.join(parts)


modules = list(find_package_module_names('dibi'))


def doctest_modules(*names):
    error_message = "Unable to process {name}: {error}"
    success_message = ("Processed {attempted} statements successfully"
                       " in {name}")
    failure_message = ("{failed}/{attempted} failed while processing"
                       " {name}")
    failures = 0
    for name in names:
        variables = dict(name=name)
        try:
            module = import_module(name)
            result = doctest.testmod(module)
        except Exception as error:
            variables['error'] = error
            logging.error(error_message.format(**variables))
            continue
        variables['attempted'] = result.attempted
        variables['failed'] = result.failed
        failures += result.failed
        if result.failed:
            logging.warning(failure_message.format(**variables))
        else:
            logging.info(success_message.format(**variables))
    return failures


def test_drivers():
    configuration = configparser.ConfigParser()
    configuration['DEFAULT']['debug'] = '1'
    configuration.read('test_parameters.conf')
    configuration.read('test/test_parameters.conf')
    result = OrderedDict()
    for name, driver in dibi.driver.registry.items():
        try:
            parameters = configuration[name]
        except KeyError:
            logging.warning("No parameters found for {name!r}".format(
                name=name))
        else:
            logging.info("Testing driver {name!r} with parameters {}".format(
                parameters, name=name))
            result = test_driver(driver, parameters)

if __name__ == '__main__':
    exit_code = 0
    failures = doctest_modules(*modules)
    exit_code = bool(failures) or exit_code
    failures = test_drivers()
    exit_code = bool(failures) or exit_code
    exit(exit_code)

# if test.failed:
#     exit(1)
