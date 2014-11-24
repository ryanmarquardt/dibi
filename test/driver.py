

import dibi

import datetime
import logging


class test_driver(object):
    def __init__(self, driver, parameters):
        self.db = dibi.DB(driver(**parameters))
        self.create_table()
        try:
            self.insert_rows()
        except:
            logging.info(self.db.driver.last_statement)
            logging.info(self.db.driver.last_values)
            raise

    def create_table(self):
        table_1 = self.db.add_table('table 1')
        table_1.add_column('name', dibi.datatype.Text)
        table_1.add_column('number', dibi.datatype.Integer)
        table_1.add_column('value', dibi.datatype.Float)
        table_1.add_column('binary_data', dibi.datatype.Blob)
        table_1.add_column('timestamp', dibi.datatype.DateTime)
        table_1.save()

    def insert_rows(self):
        sample_1_id = self.db.tables['table 1'].insert(
            name='sample 1',
            number=46,
            value=-5.498,
            binary_data=b'\xa8\xe2u\xf5pZ\x1c\x82R5\x01\xe7UC\x06',
            timestamp=datetime.datetime(1900, 1, 1, 12, 15, 14),
        )
