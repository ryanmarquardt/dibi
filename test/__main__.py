#!/usr/bin/env python

import configparser
import dibi
import doctest
import logging

from .suite import TestSuite, Success, Failure, Error, Unsuccessful
from .driver import test_driver


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


def indented(text, indent=' ' * 4):
    return '\n'.join('{}{}'.format(indent, line) for line in text.split('\n'))


class DoctestFormatSuite(TestSuite):
    def report_summary(self, results):
        print('*' * 70)
        print("""Results from {attempts} attempted test cases:
{failures} failures
{errors} errors
{successes} successes""".format(**results))

    def report_failure(self, result):
        print('*' * 70)
        print('File "{file}", line {line}, in {name}'.format(
            **vars(result)))
        print('Failed:'.format(status=result.status))
        if result.source_line and result.source_line.startswith('assert'):
            print(indented(result.source_line))
        elif result.source:
            print(indented(result.source))
        if result.expected is not None:
            print('Expected:')
            print(indented(result.expected))
        print('Got:')
        print(indented(result.actual))

    def report_error(self, result):
        print('*' * 70)
        print('File "{file}", line {line}, in {name}'.format(
            **vars(result)))
        print('Unexpected Error:'.format(status=result.status))
        if result.source_line and result.source_line.startswith('assert'):
            print(indented(result.source_line))
        elif result.source:
            print(indented(result.source))
        if result.expected is not None:
            print('Expected:')
            print(indented(result.expected))
        print('Got:')
        print(indented(result.actual))


if __name__ == '__main__':
    suite = DoctestFormatSuite()

    configuration = configparser.ConfigParser()
    configuration.read([
        'test/test_parameters.conf',
        'test_parameters.conf',
    ])

    for name, driver, parameters, expect in test_drivers(configuration):
        attempt = suite.get_child(name).test(
            test_driver, (suite, driver, parameters),
            exception=expect, name=name)

    suite.run_package_docstrings(
        dibi, optionflags=doctest.IGNORE_EXCEPTION_DETAIL)

    suite.summary()

    suite.exit()
