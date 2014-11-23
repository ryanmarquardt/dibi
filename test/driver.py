

import dibi

import logging

def test_driver(driver, parameters):
    try:
        db = dibi.DB(driver(**parameters))
    except dibi.error.ConnectionError as error:
        logging.error('ConnectionError: {}'.format(error))
