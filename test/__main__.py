#!/usr/bin/env python

import configparser
import doctest
import logging


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
            this_raises = parameters.pop('this raises', None)
            if this_raises:
                expect = getattr(dibi.error, this_raises, None)
                if expect is None:
                    expect = getattr(__builtins__, this_raises)
                if not issubclass(expect, Exception):
                    raise ValueError("Unable to find error {}".format(
                        this_raises))
            else:
                expect = None
            parameters['debug'] = True
            yield (('{}({})'.format(name, variant) if variant else name),
                   driver, parameters, expect)


import dibi

from .suite import TestSuite
from .driver import test_driver


if __name__ == '__main__':
    suite = TestSuite()

    configuration = configparser.ConfigParser()
    configuration.read([
        'test/test_parameters.conf',
        'test_parameters.conf',
    ])

    for name, driver, parameters, expect in test_drivers(configuration):
        suite.test(test_driver, (suite, driver, parameters), exception=expect,
                   name=name)

    suite.run_package_docstrings(dibi)

    suite.exit()
