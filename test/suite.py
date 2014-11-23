
test = Test()

test.db = DB.connect('sqlite')

with test("Create a table"):
    orders = test.db.add_table('orders')
    test.instance(orders, Table)

with test("Add column to a table"):
    amount = orders.add_column('amount', Integer)
    test.identical(amount.table, orders)

with test("Integer datatype serialization"):
    test.equal(amount.datatype.serialize('123'), 123)
    test.equal(amount.datatype.deserialize(123), 123)

with test("Add text column"):
    quantity = orders.add_column('quantity', Text)

with test("Add date column"):
    date = orders.add_column('date', Date)

with test("Date datatype serialization"):
    turn_of_century = datetime.date(2000, 1, 1)
    test.equal(date.datatype.serialize(turn_of_century),
               "2000-01-01")
    test.equal(date.datatype.deserialize("2000-01-01"),
               turn_of_century)

with test("Defined tables are stored in DB.tables"):
    test.contains(test.db.tables, orders)

with test("Save a table to create it"):
    orders.save()

with test("Create a table with no columns"):
    with test.raises(NoColumnsError):
        test.db.add_table('empty').save()

with test("Table names are quoted to prevent SQL injection"):
    inject = test.db.add_table('"; DROP TABLE orders;')
    inject.add_column('id', Integer)
    inject.save()

with test("Define table with autoincrement column"):
    autoinc = test.db.add_table("autoinc", primarykey='id')
    autoinc.save()
    test.equal(len(autoinc.columns), 1)
