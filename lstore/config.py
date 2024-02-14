from lstore.ColumnIndex import RawIndex


class config:
	INDIRECTION_COLUMN = RawIndex(0)
	RID_COLUMN = RawIndex(1)
	TIMESTAMP_COLUMN = RawIndex(2)
	SCHEMA_ENCODING_COLUMN = RawIndex(3)
	NULL_COLUMN = RawIndex(4)

	NUM_METADATA_COL = 5
	
	INDENT = "    " # Use "\t"
	PAGES_PER_PAGERANGE = 16 
	PACKING_FORMAT_STR = ">Q"
	PHYSICAL_PAGE_SIZE = 16384


class Metadata:
	def __init__(self, indirection_column: int | None, rid: int, timestamp: int, schema_encoding: int, null_column: int | None):
		self.indirection_column = indirection_column
		self.rid = rid
		self.timestamp = timestamp
		self.schema_encoding = schema_encoding
		self.null_column = null_column

