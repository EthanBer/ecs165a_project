from lstore.ColumnIndex import DataIndex
from lstore.db import Database
from lstore.helper import helper
from lstore.query import Query
from random import choice, randint, sample, seed

from lstore.db import Database
from lstore.query import Query

from random import choice, randint, sample, seed

db = Database()
# Create a table  with 5 columns
#   Student Id and 4 grades
#   The first argument is name of the table
#   The second argument is the number of columns
#   The third argument is determining the which columns will be primay key
#       Here the first column would be student id and primary key
grades_table = db.create_table('Grades', 5, 0)

# create a query class for the grades table
query = Query(grades_table)

# dictionary for records to test the database: test directory
records = {}

number_of_records = 1000
number_of_aggregates = 100
seed(3562901)

for i in range(0, number_of_records):
    key = 92106429 + randint(0, number_of_records)

    # skip duplicate keys
    while key in records:
        key = 92106429 + randint(0, number_of_records)

    records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
    query.insert(*records[key])
    # #print('inserted', records[key])
#print("Insert finished")

def check_with_select() -> None:
    # Check inserted records using select query
    for key in sample(records.keys(), randint(1, len(records.keys()))):
        # select function will return array of records
        # here we are sure that there is only one record in t hat array
        # check for retreiving version -1. Should retreive version 0 since only one version exists.
        record = query.select_version(key, 0, [1, 1, 1, 1, 1], -1)[0]
        error = False
        for i, column in enumerate(record.columns):
            if column != records[key][i]:
                error = True
        if error:
            #print('select error on', key, ':', record, ', correct:', records[key])
        else:
            pass
            # #print('select on', key, ':', record)

updated_records = {}
def update() -> None:
    for key in sample(records.keys(), randint(1, len(records.keys()))):
        updated_columns: list[int | None] = [None, None, None, None, None]
        updated_records[key] = records[key].copy()
        cols_to_update = sample(range(5), randint(0, 5))

        for i in cols_to_update:
            # updated value
            value = randint(0, 20)
            updated_columns[i] = value
            # update our test directory
            updated_records[key][i] = value
        query.update(key, *updated_columns)

        # check version -1 for record
        record = query.select_version(key, 0, [1, 1, 1, 1, 1], -1)[0]
        error = False
        for j, column in enumerate(record.columns):
            if column != records[key][j]:
                error = True
        if error:
            #print('update error on', records[key], 'and', updated_columns, ':', record, ', correct:', records[key])
        else:
            pass
            # #print('update on', original, 'and', updated_columns, ':', record)

        # check version -2 for record
        record = query.select_version(key, 0, [1, 1, 1, 1, 1], -2)[0]
        error = False
        for j, column in enumerate(record.columns):
            if column != records[key][j]:
                error = True
        if error:
            #print('update error on', records[key], 'and', updated_columns, ':', record, ', correct:', records[key])
        else:
            pass
            # #print('update on', original, 'and', updated_columns, ':', record)

        # check version 0 for record
        record = query.select_version(key, 0, [1, 1, 1, 1, 1], 0)[0]
        error = False
        for j, column in enumerate(record.columns):
            if column != updated_records[key][j]:
                error = True
        if error:
            #print('update error on', records[key], 'and', updated_columns, ':', record, ', correct:', updated_records[key])

def test_sum() -> None:
    keys = sorted(list(records.keys()))
    # aggregate on every column
    for c in sample(range(grades_table.num_columns), randint(0, 5)):
        for i in range(0, randint(1, number_of_aggregates)):
        #  #print('--')
        #    #print(list(records.keys()))
            r = sorted(sample(range(0, len(keys)), 2))
            # calculate the sum form test directory
            # version -1 sum
            column_sum = sum(map(lambda key: records[key][c], keys[r[0]: r[1] + 1]))
            result = query.sum_version(keys[r[0]], keys[r[1]], c, -1)
            if column_sum != result:
                #print('sum error on [', keys[r[0]], ',', keys[r[1]], ']: ', result, ', correct: ', column_sum)
            else:
                pass
                # #print('sum on [', keys[r[0]], ',', keys[r[1]], ']: ', column_sum)
            # version -2 sum
            column_sum = sum(map(lambda key: records[key][c], keys[r[0]: r[1] + 1]))
            result = query.sum_version(keys[r[0]], keys[r[1]], c, -2)
            if column_sum != result:
                #print('sum error on [', keys[r[0]], ',', keys[r[1]], ']: ', result, ', correct: ', column_sum)
            else:
                pass
            # version 0 sum
            updated_column_sum = sum(map(lambda key: updated_records[key][c], keys[r[0]: r[1] + 1]))
            updated_result = query.sum_version(keys[r[0]], keys[r[1]], c, 0)
            if updated_column_sum != updated_result:
                #print('sum error on [', keys[r[0]], ',', keys[r[1]], ']: ', updated_result, ', correct: ',
                    updated_column_sum)
            else:
                pass

check_with_select()
update()
test_sum()
# def debugger(db : Database) -> None:

#     for i in range(len(db.tables)):
#         table = db.tables[i]

#         # #print("Table Name: ", table.name)
#         # #print("Page Directory: ")
#         # #print(table.page_directory)

#         for j in range(len(table.page_ranges)):
#             # #print("Current page range", j)

#             #Printing base pages of the current page_range
#             # #print("Base Pages:")
#             for k in range(len(table.page_ranges[j].base_pages)):
#                 current_base_page = table.page_ranges[j].base_pages[k]

#                 #Printing physical pages of the current base page
#                 for l in range(len(current_base_page.physical_pages)):
#                     # #print(current_base_page.physical_pages[l].data)


#             #Printing tail pages of the current page_range
#             # #print("Tail Pages:")
#             for k in range(len(table.page_ranges[j].tail_pages)):
#                 current_tail_page = table.page_ranges[j].tail_pages[k]
#                 #Printing physical pages of the current tail page
#                 for l in range(len(current_tail_page.physical_pages)):
#                     # #print(current_tail_page.physical_pages[l].data)

"""
if __name__ == "__main__":
    db = Database()
    # Create a table  with 5 columns
    #   Student Id and 4 grades
    #   The first argument is name of the table
    #   The second argument is the number of columns
    #   The third argument is determining the which columns will be primay key
    #       Here the first column would be student id and primary key
    grades_table = db.create_table('Grades', 5, DataIndex(0))

    # create a query class for the grades table
    query = Query(grades_table)

    # dictionary for records to test the database: test directory
    records = {}
    keys = []

    number_of_records = 1
    seed(3562901)

    for i in range(0, number_of_records):
        key = 92106429 + randint(0, number_of_records)

        # skip duplicate keys
        while key in records:
            key = 92106429 + randint(0, number_of_records)

        records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
        keys.append(key)
        # #print(f"inserting {records[key]}")
        query.insert(*records[key])
        # #print(key, records)
    # #print("insert finished")
    # #print(db.tables[0].get_record_by_rid(1))
    # debugger_2(db)


updated_records = {}
for key in records:
    updated_columns: list[int | None] = [None, None, None, None, None]
    updated_records[key] = records[key].copy()
    for i in range(2, 3): # grades_table.num_columns
        # updated value
        value = randint(0, 20)
        updated_columns[i] = value
        # update our test directory
        updated_records[key][i] = value
    # for i in range(4, 5): # grades_table.num_columns
    #     # updated value
    #     value = randint(0, 20)
    #     updated_columns[i] = value
    #     # update our test directory
    #     updated_records[key][i] = value
    # # #print(f"columns should be updated to {updated_columns}")
    # query.update(key, *updated_columns)
# #print("update finished. records:")
# #print(updated_records)
query.update(key, *[None, 78, None, None, None])
query.update(key, *[None, None, 89, None, None])
#print(db.tables[0].get_record_by_rid(1))
#print(db.tables[0].get_record_by_rid(2))
#print(db.tables[0].get_record_by_rid(3))
#print(db.tables[0].get_record_by_rid(4))
#print(db.tables[0].get_record_by_rid(5))


#print(helper.str_each_el(query.select(keys[0], DataIndex(0), [1] * 5)))

query.delete(keys[0])   # Delete last record inserted
# # #print(helper.str_each_el(query.select(keys[0], DataIndex(0), [1] * 5)))
# # #print(db.tables[0].get_record_by_rid(1))
# #print(db.tables[0].get_record_by_rid(6))
# debugger_2(db)
# # #print(helper.str_each_el(query.select(keys[0], DataIndex(0), [1] * 5)))
# # #print(helper.str_each_el(query.select(keys[0], DataIndex(0), [0, 1, 0, 0, 0])))
    #
 # #print(f"delete successful, key: {keys[0]}" if query.delete(keys[0]) else "delete failed")
"""