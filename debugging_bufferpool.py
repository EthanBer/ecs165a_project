
import os
from random import randint, seed
from lstore.config import config
from lstore.db import Database
from lstore.query import Query
import shutil

if os.path.isdir("./TEST_DB_PATH"):
	shutil.rmtree('./TEST_DB_PATH')

db = Database()
db.open("./TEST_DB_PATH")
grades_table = db.create_table('Grades', 5, 0)


query = Query(grades_table)
number_of_records = 559	
number_of_aggregates = 100
records = {}
seed(3562901)
for i in range(0, number_of_records):
    key = 92106429 + randint(0, number_of_records)

    # skip duplicate keys
    while key in records:
        key = 92106429 + randint(0, number_of_records)

    records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
    query.insert(*records[key])
db.close()

# records = {}
#db.close()
# number_of_records = 1000
# number_of_aggregates = 100