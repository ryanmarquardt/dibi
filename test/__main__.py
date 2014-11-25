#!/usr/bin/env python

import dibi
from dibi import DB, Table, NoColumnsError
from dibi.datatype import Integer, Text, Date

#from collections import OrderedDict
import configparser
#import datetime
import doctest
import logging

from importlib import reload, import_module

from .driver import test_driver

import os

from .suite import TestSuite


def get_testing_configurations(configuration, driver):
    base_parameters = configuration[driver]
    for section in configuration.sections():
        name, colon, variant = section.partition(':')
        if name == driver:
            variant_parameters = dict(base_parameters)
            variant_parameters.update(configuration[section])
            yield (variant, variant_parameters)


def get_driver_variants(configuration, base):
    try:
        return sorted(get_testing_configurations(configuration, base))
    except KeyError:
        return ()


def test_drivers(suite):
    for name, driver in sorted(dibi.driver.registry.items()):
        for variant, parameters in get_driver_variants(configuration, name):
            expect = (getattr(dibi.error, parameters.pop('this raises'))
                      if 'this raises' in parameters else None)
            parameters['debug'] = True
            yield (('{}({})'.format(name, variant) if variant else name),
                   driver, parameters, expect)


if __name__ == '__main__':
    suite = TestSuite()
    logging.basicConfig(level=logging.WARNING)

    suite.run_package_docstrings(dibi)

    configuration = configparser.ConfigParser()
    configuration.read([
        'test/test_parameters.conf',
        'test_parameters.conf',
    ])

    for name, driver, parameters, expect in test_drivers(configuration):
        with suite.catch(expect, name=name):
            test_driver(suite, driver, parameters)

    suite.exit()
