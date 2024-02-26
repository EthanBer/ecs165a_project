
from lstore.db import Database
from lstore.query import Query


db = Database()
grades_table = db.create_table('Grades', 5, 0)

db.open("./TEST_DB_PATH")

query = Query(grades_table)
query.insert(*[1, 2, 3, 4, 5])
# records = {}

# number_of_records = 1000
# number_of_aggregates = 100