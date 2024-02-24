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
	PHYSICAL_PAGE_SIZE = 4096
	
	ID_COUNT = 1 # Is to make sure we dont have two files with the same name
	PATH = "./Pages"

	BUFFERPOOL_SIZE = 256

	class byte_position:
		BASE_METADATA_PTR = 0
		BASE_OFFSET = 8 
		TAIL_OFFSET = 0
		CATALOG_LAST_BASE_ID = 0
		CATALOG_LAST_TAIL_ID = 8
		CATALOG_LAST_METADATA_ID = 16
		CATALOG_LAST_RID = 32

	BYTES_PER_INT = 8	

class PhysicalMetadata:
	def __init__(self, rid: int, timestamp: int):
		self.rid = rid
		self.timestamp = timestamp
class Metadata:
	def __init__(self, indirection_column: int | None, schema_encoding: int, null_column: int | None):
		# self.rid = rid
		# self.timestamp = timestamp
		self.indirection_column = indirection_column
		self.schema_encoding = schema_encoding
		self.null_column = null_column

