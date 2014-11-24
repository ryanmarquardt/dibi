

import dibi

import logging


def test_driver(driver, parameters):
    db = dibi.DB(driver(**parameters))
