#!/usr/bin/env python

from distutils.core import setup

setup(
    name='dibi',
    version='0.2',
    author='Ryan Marquardt',
    author_email='ryan@integralws.com',
    url='http://github.com/orbnauticus/dibi',
    description='Object storage wrapper on databases',
    packages=[
        'dibi',
        'dibi.driver',
        'dibi.datatype',
        'test',
    ],
    license='Simplified BSD License',
)
