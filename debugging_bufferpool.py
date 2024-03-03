
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
number_of_records = 1000
number_of_aggregates = 100
records = {}
seed(3562901)
for i in range(0, number_of_records):
	key = 900 + i

	# skip duplicate keys
	# while key in records:
	# 	key = 92106429 + randint(0, number_of_records)

	records[key] = [key, i + 2, i + 3, i + 4, i + 5]
	# if i == 513:
	# 	print(f"51th record was {records[key]}")
	#print(records[key])
	query.insert(*records[key])

for key in records:
	s = query.select(key, 0, [1, 1, 1, 1, 1])
	record = s[0]
	error = False
	for i, column in enumerate(record.columns):
		if column != records[key][i]:
			error = True
	if error:
		raise(Exception("SELECT ERROR"))
		print('select error on', key, ':', record.columns, ', correct:', records[key])
	else:
		pass
		print('select on', key, ':', record.columns)
print("SELECT PASS")

for key in records:
	updated_columns = [None, None, None, None, None]
	for i in range(2, 3):
		# updated value
		value = randint(0, 20)
		updated_columns[i] = value
		# copy record to check
		original = records[key].copy()
		# update our test directory
		records[key][i] = value
		print(f"updated_columns = {updated_columns}")
		query.update(key, *updated_columns)
		# record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
		error = False
		for j, column in enumerate(record.columns):
			if column != records[key][j]:
				error = True
		if error:
			raise(Exception("UPDATE ERROR")) 
			print('update error on', original, 'and', updated_columns, ':', record, ', correct:', records[key])
		else:
			pass
			# print('update on', original, 'and', updated_columns, ':', record)
		updated_columns[i] = None

db.close()