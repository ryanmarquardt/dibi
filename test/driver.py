

import dibi

import datetime
import logging


class catch_failure(object):
    def __init__(self, test):
        self.test = test

    def __enter__(self):
        return self

    def __exit__(self, exc, obj, tb):
        if exc is not None:
            self.test.logger.info(self.test.db.driver.last_statement)
            self.test.logger.info(self.test.db.driver.last_values)


class test_driver(object):
    def __init__(self, variant, driver, parameters):
        self.db = dibi.DB(driver(**parameters))
        self.logger = logging.getLogger(variant)
        for method in (self.create_table, self.list_tables, self.insert_rows):
            with catch_failure(self):
                method()

    def create_table(self):
        table_1 = self.db.add_table('table 1')
        table_1.add_column('name', dibi.datatype.Text)
        table_1.add_column('number', dibi.datatype.Integer)
        table_1.add_column('value', dibi.datatype.Float)
        table_1.add_column('binary_data', dibi.datatype.Blob)
        table_1.add_column('timestamp', dibi.datatype.DateTime)
        table_1.save()

    def list_tables(self):
        tables = list(self.db.driver.list_tables())
        assert tables == ['table 1'], tables

    def insert_rows(self):
        sample_1_id = self.db.tables['table 1'].insert(
            name='sample 1',
            number=46,
            value=-5.498,
            binary_data=b'\xa8\xe2u\xf5pZ\x1c\x82R5\x01\xe7UC\x06',
            timestamp=datetime.datetime(1900, 1, 1, 12, 15, 14),
        )
        assert sample_1_id == 1, sample_1_id
