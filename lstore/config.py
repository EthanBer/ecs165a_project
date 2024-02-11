class config:
	
	last_rid = 1
	
	INDIRECTION_COLUMN = 0
	RID_COLUMN = 1
	TIMESTAMP_COLUMN = 2
	SCHEMA_ENCODING_COLUMN = 3
	NULL_COLUMN = 4

	NUM_METADATA_COL = 5
	NUM_METADATA_COL = 5
	
	INDENT = "    " # Use "\t"

	@staticmethod
	def str_each_el(arr: list, delim: str="") -> str:
		return delim.join([str(el) for el in arr])
		# s = ""
		# for el in arr:
		# 	s += el.__str__()
		# return s

	PAGES_PER_PAGERANGE = 16