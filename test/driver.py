

import dibi

import datetime
import logging

import inspect


class catch_failure(object):
    def __init__(self, test):
        self.test = test

    def __enter__(self):
        return self

    def __exit__(self, exc, obj, tb):
        if exc is not None:
            self.test.logger.info(self.test.db.driver.last_statement)
            self.test.logger.info(self.test.db.driver.last_values)
        if exc is AssertionError:
            while tb.tb_next:
                tb = tb.tb_next
            frame = tb.tb_frame
            code = frame.f_code
            source, first_line = inspect.getsourcelines(tb)
            source = source[frame.f_lineno - first_line]
            local_vars = {key: value for key, value in frame.f_locals.items()
                          if key in source}
            fail_text = ("Failure in {module}.{function}:{line}\n{source}  # "
                         "{locals}")
            self.test.logger.error(fail_text.format(
                module=inspect.getmoduleinfo(code.co_filename).name,
                function=code.co_name,
                line=frame.f_lineno,
                source=source.strip(),
                locals=", ".join("{}={!r}".format(*item) for item in
                                 local_vars.items()),
            ))
            return True


class test_driver(object):
    def __init__(self, variant, driver, parameters):
        self.db = dibi.DB(driver(**parameters))
        self.logger = logging.getLogger(variant)
        for method in (self.create_table, self.list_tables, self.list_columns,
                       self.insert_rows):
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
        assert tables == ['table 1']

    def list_columns(self):
        columns = list(self.db.driver.list_columns('table 1'))

    def insert_rows(self):
        sample_1_id = self.db.tables['table 1'].insert(
            name='sample 1',
            number=46,
            value=-5.498,
            binary_data=b'\xa8\xe2u\xf5pZ\x1c\x82R5\x01\xe7UC\x06',
            timestamp=datetime.datetime(1900, 1, 1, 12, 15, 14),
        )
        with catch_failure(self):
            assert sample_1_id == 1
        sample_1_id = self.db.tables['table 1'].insert(
            name='sample 2',
            number=83,
            value=16.937,
            binary_data=b'3\x90&`v\x80\xec\x87\x07\xd5/\t\xc5\xac\xa3',
            timestamp=datetime.datetime(1402, 2, 17, 4, 32, 55),
        )
        with catch_failure(self):
            assert sample_1_id == 2
