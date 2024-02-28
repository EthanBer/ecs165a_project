
import os
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
query.insert(*[1, 2, 3, 4, 5])
db.close()

# records = {}
#db.close()
# number_of_records = 1000
# number_of_aggregates = 100