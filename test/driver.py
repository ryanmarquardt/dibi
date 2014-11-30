

import dibi

import datetime


class test_driver(object):
    def __init__(self, suite, driver, parameters):
        self.suite = suite
        self.db = dibi.DB(driver(**parameters))
        suite.test(self.create_table)
        suite.test(self.list_tables)
        suite.test(self.list_columns)
        suite.test(self.insert_rows)
        suite.test(self.select_row_by_id)
        suite.test(self.select_equal_to_string)
        suite.test(self.select_equal_to_none)
        suite.test(self.update_selection)
        suite.test(self.delete_all)

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
        columns = list(self.db.driver.list_columns(self.db.tables['table 1']))
        assert len(columns) == 5 + 1  # Implicit __id__ column is included
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
        sample_2_id = self.db.tables['table 1'].insert(
            name='sample 2',
            number=83,
            value=16.937,
            binary_data=b'3\x90&`v\x80\xec\x87\x07\xd5/\t\xc5\xac\xa3',
            timestamp=datetime.datetime(1402, 2, 17, 4, 32, 55),
        )
        with self.suite.catch():
            assert sample_2_id == 2
        sample_3_id = self.db.tables['table 1'].insert(
            name='sample 3',
            number=6,
            value=2,
            binary_data=b'\xf2\x15\xdb\xf3\nN\x91\xa0\xf0\xa3}\x7fWPE',
            timestamp=None,
        )
        with self.suite.catch():
            assert sample_3_id == 3

    def select_row_by_id(self):
        assert self.db.tables['table 1'][1] is not None

    def select_equal_to_string(self):
        table_1 = self.db.tables['table 1']
        rows = list((table_1.columns['name'] == 'sample 2').select())
        assert len(rows) == 1
        name, number, value, binary_data, timestamp = rows[0]
        assert number == 83

    def select_equal_to_none(self):
        table_1 = self.db.tables['table 1']
        null = None  # Pep8 complains, even though this is what we intend
        rows = list((table_1.columns['timestamp'] == null).select())
        assert len(rows) == 1
        name, number, value, binary_data, timestamp = rows[0]
        assert name == 'sample 3'

    def update_selection(self):
        value = self.db.tables['table 1'].value
        assert len((value < 0).select_all()) == 1
        (value < 0).update(value=100)
        assert len((value < 0).select_all()) == 0

    def delete_all(self):
        table_1 = self.db.tables['table 1']
        assert len(table_1.select_all()) > 0
        table_1.delete()
        assert len(table_1.select_all()) == 0
