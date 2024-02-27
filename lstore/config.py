from lstore.ColumnIndex import RawIndex


class config:
	INDIRECTION_COLUMN = RawIndex(0)
	RID_COLUMN = RawIndex(1)
	TIMESTAMP_COLUMN = RawIndex(2)
	SCHEMA_ENCODING_COLUMN = RawIndex(3)
	NULL_COLUMN = RawIndex(4)
	BASE_RID= RawIndex(5)
	NUM_METADATA_COL = 6
	
	INDENT = "    " # Use "\t"
	PAGES_PER_PAGERANGE = 16 
	PACKING_FORMAT_STR = ">Q"
	PHYSICAL_PAGE_SIZE = 4096
	UPDATES_BEFORE_MERGE= 1024
	ID_COUNT = 1 # Is to make sure we dont have two files with the same name
	PATH = "./Pages"

	BUFFERPOOL_SIZE = 256

	class byte_position:
		METADATA_PTR = 0
		OFFSET = 8 
		PAGE_RANGE_ID=16


		CATALOG_TABLE_NUMBER_COLUMNS=0 
		CATALOG_KEY_INDEX= 8
		CATALOG_PAGES_PER_PAGE_RANGE=16
		CATALOG_LAST_BASE_ID = 24
		CATALOG_LAST_TAIL_ID = 32
		CATALOG_LAST_METADATA_ID = 40
		CATALOG_LAST_RID = 48
		CATALOG_TPS=56

	BYTES_PER_INT = 8	

class FullMetadata:
	def __init__(self, rid: int | None, timestamp: int, indirection_column: int | None, schema_encoding: int, null_column: int | None,base_rid:int):
		self.rid = rid
		self.timestamp = timestamp
		self.indirection_column = indirection_column
		self.schema_encoding = schema_encoding
		self.null_column = null_column
		self.base_rid=base_rid
		

class WriteSpecifiedMetadata:
	def __init__(self, indirection_column: int | None, schema_encoding: int, null_column: int | None):
		# self.rid = rid
		# self.timestamp = timestamp
		self.indirection_column = indirection_column
		self.schema_encoding = schema_encoding
		self.null_column = null_column
		
