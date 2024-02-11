from lstore.db import Database
from lstore.query import Query
from random import choice, randint, sample, seed


def debugger_2(db: Database) -> None:
    print("DATABASE: ")
    for table in db.tables:
        print(table)


# def debugger(db : Database) -> None:

#     for i in range(len(db.tables)):
#         table = db.tables[i]

#         print("Table Name: ", table.name)
#         print("Page Directory: ")
#         print(table.page_directory)
        
#         for j in range(len(table.page_ranges)):
#             print("Current page range", j)

#             #Printing base pages of the current page_range
#             print("Base Pages:")
#             for k in range(len(table.page_ranges[j].base_pages)):
#                 current_base_page = table.page_ranges[j].base_pages[k]

#                 #Printing physical pages of the current base page
#                 for l in range(len(current_base_page.physical_pages)):
#                     print(current_base_page.physical_pages[l].data)


#             #Printing tail pages of the current page_range
#             print("Tail Pages:")
#             for k in range(len(table.page_ranges[j].tail_pages)):
#                 current_tail_page = table.page_ranges[j].tail_pages[k]
#                 #Printing physical pages of the current tail page
#                 for l in range(len(current_tail_page.physical_pages)):
#                     print(current_tail_page.physical_pages[l].data)





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

number_of_records = 2
seed(3562901)

for i in range(0, number_of_records):
    key = 92106429 + randint(0, number_of_records)

    #skip duplicate keys
    while key in records:
        key = 92106429 + randint(0, number_of_records)

    records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
    query.insert(*records[key])
    print(key, records)
print("Insert finished")
debugger_2(db)