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
	INITIAL_TPS = (2**64) - 1
	INITIAL_TID = (2**64) - 1
	# BASE_PAGE_FILE_SCHEMA = ["metadata_pointer", "offset", "TPS", "base_data"]
	# TAIL_PAGE_FILE_SCHEMA = ["metadata_pointer", "offset", "tail_data"]
	# METADATA_PAGE_FILE_SCHEMA = ["offset", "metadata"]
	# CATALOG_FILE_SCHEMA = ["num_columns", "key_index", "last_base_page_id", "last_tail_page_id", "last_metadata_page_id", "last_rid"]

	class byte_position:
		# @staticmethod
		# def get_base_offset(field: str) -> int:
		# 	idx = config.BASE_PAGE_FILE_SCHEMA.index(field)
		# 	return config.BYTES_PER_INT * idx
		# @staticmethod
		# def get_tail_offset(field: str) -> int:
		# 	idx = config.TAIL_PAGE_FILE_SCHEMA.index(field)
		# 	return config.BYTES_PER_INT * idx
		class base_tail:
			METADATA_PTR = 0*8
			OFFSET = 1*8
			TPS = 2*8
			DATA = 3*8
		class metadata:
			OFFSET = 0*8
			DATA = 1*8
		class catalog:
			NUMBER_COLUMNS = 0*8
			KEY_INDEX = 1*8
			LAST_BASE_PAGE_ID = 2*8
			LAST_TAIL_PAGE_ID = 3*8
			LAST_BASE_METADATA_PAGE_ID = 4*8
			LAST_TAIL_METADATA_PAGE_ID = 5*8
			LAST_BASE_RID = 6*8
			LAST_TAIL_RID = 7*8

	BYTES_PER_INT = 8	

class FullMetadata:
	def __init__(self, rid: int | None, timestamp: int, indirection_column: int | None, schema_encoding: int, null_column: int | None,base_rid:int):
		self.rid = rid
		self.timestamp = timestamp
		self.indirection_column = indirection_column
		self.schema_encoding = schema_encoding
		self.null_column = null_column
		self.base_rid=base_rid
		

class WriteSpecifiedBaseMetadata:
	def __init__(self, indirection_column: int | None, schema_encoding: int, null_column: int | None):
		# self.rid = rid
		# self.timestamp = timestamp
		self.indirection_column = indirection_column
		self.schema_encoding = schema_encoding
		# self.null_column = null_column
		
class WriteSpecifiedTailMetadata:
	def __init__(self, indirection_column: int | None, schema_encoding: int, null_column: int | None, base_rid: int):
		# self.rid = rid
		# self.timestamp = timestamp
		self.base_rid = base_rid
		self.indirection_column = indirection_column
		self.schema_encoding = schema_encoding
		self.null_column = null_column
