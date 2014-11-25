

import dibi

import datetime


class test_driver(object):
    def __init__(self, suite, driver, parameters):
        self.suite = suite
        self.db = dibi.DB(driver(**parameters))
        for method in (self.create_table, self.list_tables, self.list_columns,
                       self.insert_rows):
            with suite.catch(name=method.__name__):
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
        assert tables == ['table 1']

    def list_columns(self):
        columns = list(self.db.driver.list_columns('table 1'))
        assert len(columns) == 5
        # TODO: Further assertions about the nature of the returned columns

    def insert_rows(self):
        sample_1_id = self.db.tables['table 1'].insert(
            name='sample 1',
            number=46,
            value=-5.498,
            binary_data=b'\xa8\xe2u\xf5pZ\x1c\x82R5\x01\xe7UC\x06',
            timestamp=datetime.datetime(1900, 1, 1, 12, 15, 14),
        )
        with self.suite.catch():
            assert sample_1_id == 1
        sample_1_id = self.db.tables['table 1'].insert(
            name='sample 2',
            number=83,
            value=16.937,
            binary_data=b'3\x90&`v\x80\xec\x87\x07\xd5/\t\xc5\xac\xa3',
            timestamp=datetime.datetime(1402, 2, 17, 4, 32, 55),
        )
        with self.suite.catch():
            assert sample_1_id == 2
