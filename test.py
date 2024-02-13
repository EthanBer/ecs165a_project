from lstore.db import Database
from lstore.query import Query
from random import choice, randint, sample, seed


from lstore.ColumnIndex import DataIndex, RawIndex




##################    TEST DELETE   ##########################################
# make sure that is flagged as deleted in the Base page
# make sure that is flagged as deleted in  all the Tail Pages
# make sure is delted from the page_directory of the table ?????????????
# 



def debugger_2(db: Database) -> None:
    # print("DATABASE: ")
    for table in db.tables:
        # print(table)
db = Database()
grades_table = db.create_table('Grades', 5, DataIndex(0))
# create a query class for the grades table
query = Query(grades_table)

# dictionary for records to test the database: test directory
records = {}
keys = []

number_of_records = 2
seed(3562901)

for i in range(0, number_of_records):
    key = 92106429 + randint(0, number_of_records)

    # skip duplicate keys
    while key in records:
        key = 92106429 + randint(0, number_of_records)

    records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
    keys.append(key)
    query.insert(*records[key])
    # print(key, records)
# print("insert finished")
# print(db.tables[0].get_record_by_rid(1))


query.delete(key)   # Delete last record inserted

debugger_2(db)
