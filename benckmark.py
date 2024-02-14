from lstore.config import config
from lstore.db import Database
from lstore.query import Query
from time import process_time
from random import choice, randrange

# Student Id and 4 grades
db = Database()
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)
keys = []

for size in (100, 1000, 10000, 100000):
	print(f"START: size:{size}")
	for physical_page_size in (4096, 8192, 16384, 32768):
		for pages_per_page_range in (128, 256, 512, 1024):
			print(f"physical_page_size:{physical_page_size}; pages_per_page_range:{pages_per_page_range}")
			config.PHYSICAL_PAGE_SIZE = physical_page_size
			config.PAGES_PER_PAGERANGE = pages_per_page_range
			insert_time_0 = process_time()
			for i in range(0, size):
				query.insert(906659671 + i, 93, 0, 0, 0)
				keys.append(906659671 + i)
			insert_time_1 = process_time()

			print(f"Inserting {size} records took:  \t\t\t", insert_time_1 - insert_time_0)

			# Measuring update Performance
			update_cols = [
				[None, None, None, None, None],
				[None, randrange(0, 100), None, None, None],
				[None, None, randrange(0, 100), None, None],
				[None, None, None, randrange(0, 100), None],
				[None, None, None, None, randrange(0, 100)],
			]

			update_time_0 = process_time()
			for i in range(0, size):
				query.update(choice(keys), *(choice(update_cols)))
			update_time_1 = process_time()
			print(f"Updating {size} records took:  \t\t\t", update_time_1 - update_time_0)

			# Measuring Select Performance
			select_time_0 = process_time()
			for i in range(0, size):
				query.select(choice(keys), 0, [1, 1, 1, 1, 1])
			select_time_1 = process_time()
			print(f"Selecting {size} records took:  \t\t\t", select_time_1 - select_time_0)

			# Measuring Aggregate Performance
			agg_time_0 = process_time()
			for i in range(0, size, 100):
				start_value = 906659671 + i
				end_value = start_value + 100
				result = query.sum(start_value, end_value - 1, randrange(0, 5))
			agg_time_1 = process_time()
			print(f"Aggregate {size} of 100 record batch took:\t", agg_time_1 - agg_time_0)

			# Measuring Delete Performance
			delete_time_0 = process_time()
			for i in range(0, size):
				query.delete(906659671 + i)
			delete_time_1 = process_time()
			print(f"Deleting {size} records took:  \t\t\t", delete_time_1 - delete_time_0)
			print(" ---------------- ")
	print("== END == ")
	print("")