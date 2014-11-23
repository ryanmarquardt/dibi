

import dibi


def test_driver(driver, parameters):
    db = dibi.DB(driver(**parameters))
