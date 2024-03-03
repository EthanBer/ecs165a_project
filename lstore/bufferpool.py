from __future__ import annotations
from codecs import ascii_encode
from collections import namedtuple
from curses import raw
from functools import reduce
import pickle
import stat
import struct
import time
from types import FunctionType
import typing
import glob
import os
from lstore.index import Index
from lstore.page_directory_entry import RID, BaseMetadataPageID, BasePageID, BaseRID, BaseTailPageID, MetadataPageID, PageDirectoryEntry, PageID, TailMetadataPageID, TailPageID, TailRID
from lstore.ColumnIndex import DataIndex, RawIndex
from lstore.config import FullMetadata, WriteSpecifiedBaseMetadata, WriteSpecifiedTailMetadata, config
from lstore.helper import helper
from lstore.record_physical_page import PhysicalPage, Record
from typing import Any, List, Literal, NamedTuple, Sequence, Tuple, Type, TypeAlias, TypeGuard, TypeVar, Generic, Annotated, cast, get_args

TPageType = Literal["base", "tail", "base_metadata", "tail_metadata"]
class BufferpoolIndex(int):
	pass

TNOT_FOUND = Literal[-1]
class BufferpoolRecordSearchResult(NamedTuple):
	found: bool 
	data_buff_indices: List[None | TNOT_FOUND | BufferpoolIndex]
	metadata_buff_indices: List[TNOT_FOUND | BufferpoolIndex]
	record_offset: int | None

class BufferpoolPageSearchResult(NamedTuple):
	found: bool 
	data_buff_indices: List[None | TNOT_FOUND | BufferpoolIndex]
	metadata_buff_indices: List[TNOT_FOUND | BufferpoolIndex]

class FilePageReadResult(NamedTuple):
	metadata_physical_pages: list[PhysicalPage | None]
	data_physical_pages: list[PhysicalPage | None]

# this result is returned when getting a full record.
class FullFilePageReadResult:
	def __init__(self, metadata_physical_pages: list[PhysicalPage], data_physical_pages: list[PhysicalPage], page_type: str): # should be only "base" or "tail" page type
		self.metadata_physical_pages = metadata_physical_pages
		self.data_physical_pages = data_physical_pages
		self.page_type = page_type

class BufferpoolEntry:
	def __init__(self, pin_count: int, physical_page: PhysicalPage, dirty_bit: bool, physical_page_id: PageID, physical_page_index: RawIndex, table: Table):
		# if pin_count != None: # None for pin_count will not change pin_count
		self.pin_count = pin_count 
		self.physical_page = physical_page
		self.dirty_bit = dirty_bit
		self.physical_page_id = physical_page_id
		self.physical_page_index = physical_page_index
		self.table = table

# NOTE: all classes that start with PB* are PseudoBuffIntValues with a specific int type
# for example PBBaseRID is a PseudoBuffIntValue class that returns a BaseRID. 
# this doesn't ensure correctness but at least it communicates somewhat that it should only be used for BaseRIDs
# I was unable to make this class generic (while also being somewhat type-safe)
class PsuedoBuffIntValue:
	def __init__(self, file_handler: FileHandler, page_sub_path: PageID | Literal["catalog"], byte_position: int) -> None:
		self.flushed = False
		self.dirty = False
		self.page_sub_path = page_sub_path
		self.page_paths = [file_handler.page_path(page_sub_path)]
		self.file_handler = file_handler
		self.byte_positions = [byte_position]
		self._value = file_handler.read_int_value(page_sub_path, byte_position)
	
	def flush(self) -> None:
		if self.dirty:
			for i in range(len(self.page_paths)):
				self.file_handler.write_position(self.page_paths[i], self.byte_positions[i], self._value)
		self.flushed = True
	
	def value(self, increment: int=0) -> int:
		val = self._value
		if self.flushed:
			raise(Exception("PseudoBuffInt*Value objects can only be flushed once; value() was called after flushing"))
		if increment != 0:
			self._value += increment 
			self.dirty = True
		return val
	
	def value_assign(self, new_value: int) -> None:
		self._value = new_value
		self.dirty = True

	# will flush the value to memory to THIS location also. 
	def add_flush_location(self, page_sub_path: PageID | Literal["catalog"], byte_position: int) -> None:
		self.page_paths.append(self.file_handler.page_path(page_sub_path))
		self.byte_positions.append(byte_position)

	def __del__(self) -> None: # ensure that the value was flushed, if it was dirty
		if not self.flushed and self.dirty:
			raise(Exception("unflushed int buffer value"))

class PBBasePageID(PsuedoBuffIntValue):
	def __init__(self, file_handler: FileHandler, page_sub_path: PageID | Literal["catalog"], byte_position: int) -> None:
		super().__init__(file_handler, page_sub_path, byte_position)
	
	def value(self, increment: int = 0) -> BasePageID:
		val = self._value
		if self.flushed:
			raise(Exception("PseudoBuffInt*Value objects can only be flushed once; value() was called after flushing"))
		if increment != 0:
			self._value += increment 
			self.dirty = True
		return BasePageID(val)

class PBTailPageID(PsuedoBuffIntValue):
	def __init__(self, file_handler: FileHandler, page_sub_path: PageID | Literal["catalog"], byte_position: int) -> None:
		super().__init__(file_handler, page_sub_path, byte_position)

	def value(self, increment: int = 0) -> TailPageID:
		val = self._value
		if self.flushed:
			raise(Exception("PseudoBuffInt*Value objects can only be flushed once; value() was called after flushing"))
		if increment != 0:
			self._value += increment 
			self.dirty = True
		return TailPageID(val)

class PBBaseMetadataPageID(PsuedoBuffIntValue):
	def __init__(self, file_handler: FileHandler, page_sub_path: PageID | Literal["catalog"], byte_position: int) -> None:
		super().__init__(file_handler, page_sub_path, byte_position)
	
	def value(self, increment: int = 0) -> BaseMetadataPageID:
		val = self._value
		if self.flushed: 
			raise(Exception("PseudoBuffInt*Value objects can only be flushed once; value() was called after flushing"))
		if increment != 0:
			self._value += increment 
			self.dirty = True
		return BaseMetadataPageID(val)
	
class PBTailMetadataPageID(PsuedoBuffIntValue):
	def __init__(self, file_handler: FileHandler, page_sub_path: PageID | Literal["catalog"], byte_position: int) -> None:
		super().__init__(file_handler, page_sub_path, byte_position)
	
	def value(self, increment: int = 0) -> TailMetadataPageID:
		val = self._value
		if self.flushed:
			raise(Exception("PseudoBuffInt*Value objects can only be flushed once; value() was called after flushing"))
		if increment != 0:
			self._value += increment 
			self.dirty = True
		return TailMetadataPageID(val)

class PBBaseRID(PsuedoBuffIntValue):
	def __init__(self, file_handler: FileHandler, page_sub_path: PageID | Literal["catalog"], byte_position: int) -> None:
		super().__init__(file_handler, page_sub_path, byte_position)
	def value(self, increment: int = 0) -> BaseRID:
		val = self._value
		if self.flushed:
			raise(Exception("PseudoBuffInt*Value objects can only be flushed once; value() was called after flushing"))
		if increment != 0:
			self._value += increment 
			self.dirty = True
		return BaseRID(val)

class PBTailRID(PsuedoBuffIntValue):
	def __init__(self, file_handler: FileHandler, page_sub_path: PageID | Literal["catalog"], byte_position: int) -> None:
		super().__init__(file_handler, page_sub_path, byte_position)
	def value(self, increment: int = 0) -> TailRID:
		val = self._value
		if self.flushed:
			raise(Exception("PseudoBuffInt*Value objects can only be flushed once; value() was called after flushing"))
		if increment != 0:
			self._value += increment 
			self.dirty = True
		return TailRID(val)

U = TypeVar('U')
V = TypeVar('V')
class PseudoBuffDictValue(Generic[U, V]):
	def __init__(self, file_handler: FileHandler, page_sub_path: Literal["page_directory", "indices"]):
		self.page_sub_path = page_sub_path
		self.page_path = file_handler.page_path(page_sub_path)
		self.file_handler = file_handler
		self._value = file_handler.read_dict_value(page_sub_path)
		self.flushed = False
		self.dirty = False
		
	def flush(self) -> None:
		with open(self.page_path, "w+b") as handle:
			pickle.dump(self._value, handle)
		handle.close()
		self.flushed = True
	
	def value_get(self) -> dict[U, V]:
		if self.flushed:
			raise(Exception("PseudoBuffDictValues can only be flushed once; tried to get value after flush"))
		return self._value
	
	def __getitem__(self, key: U) -> V:
		if self.flushed:
			raise(Exception("PseudoBuffDictValues can only be flushed once; tried to get value after flush"))
		return self._value[key]
	
	def value_assign(self, new_key: U, new_value: V) -> dict[U, V]:
		if self.flushed:
			raise(Exception("PseudoBuffDictValues can only be flushed once; tried to set value after flush"))
		self.dirty = True
		self._value[new_key] = new_value
		return self._value
	
	def __del__(self) -> None:
		if not self.flushed and self.dirty:
			raise(Exception("unflushed dict buffer value "))


class FileHandler:
	def __init__(self, table: Table) -> None: # path is the database-level path
		# NOTE: these next_*_id variables represent the *next* id to be written, not necessarily the last one. the last 
		# written id is the next_*_id variable minus 1
		self.table = table
		self.table_path = os.path.join(table.db_path, self.table.name)
		self.is_flushed = False

		## FILE INITIALIZATION
		if not os.path.isfile(self.table_file_path("catalog")): # if the catalog file exists, all other files should also exist..
			self.initialize_table_files() # catalog, index, page_directory
			self.initialize_base_tail_page(BasePageID(1), BaseMetadataPageID(1)) # first metadata ID is 1
			self.initialize_base_tail_page(TailPageID(1), TailMetadataPageID(1)) # first metadata ID is 1
			self.initialize_metadata_file(BaseMetadataPageID(1))
			self.initialize_metadata_file(TailMetadataPageID(1))
		## END FILE INIT
		
		self.next_base_page_id = PBBasePageID(self, "catalog", config.byte_position.catalog.LAST_BASE_PAGE_ID)
		self.next_tail_page_id = PBTailPageID(self, "catalog", config.byte_position.catalog.LAST_TAIL_PAGE_ID)
		self.next_base_metadata_page_id = PBBaseMetadataPageID(self, "catalog", config.byte_position.catalog.LAST_BASE_METADATA_PAGE_ID)
		self.next_tail_metadata_page_id = PBTailMetadataPageID(self, "catalog", config.byte_position.catalog.LAST_TAIL_METADATA_PAGE_ID)
		self.next_base_rid = PBBaseRID(self, "catalog", config.byte_position.catalog.LAST_BASE_RID)
		#print(f"next base rid initialized to {self.next_base_rid.value()}")
		self.next_tail_rid = PBTailRID(self, "catalog", config.byte_position.catalog.LAST_TAIL_RID)

		# TODO: populate the offset byte with 0 when creating a new page
		self.base_offset = PsuedoBuffIntValue(self, BasePageID(self.next_base_page_id.value()), config.byte_position.base_tail.OFFSET) # the current offset is based on the last written page
		self.base_offset.add_flush_location(BaseMetadataPageID(self.next_base_metadata_page_id.value()), config.byte_position.metadata.OFFSET) # flush the offset in the corresponding metadata file along with the base file
		self.tail_offset = PsuedoBuffIntValue(self, TailPageID(self.next_tail_page_id.value()), config.byte_position.base_tail.OFFSET) # the current offset is based on the last written page
	
	def base_path(self, base_page_id: BasePageID) -> str:
		return os.path.join(self.table_path, f"base_{base_page_id}")
	
	def tail_path(self, tail_page_id: TailPageID) -> str:
		return os.path.join(self.table_path, f"tail_{tail_page_id}")
	
	def metadata_path(self, metadata_page_id: BaseMetadataPageID | TailMetadataPageID) -> str:
		metadata_page_type = "base" if isinstance(metadata_page_id, BaseMetadataPageID) else "tail"
		return os.path.join(self.table_path, f"{metadata_page_type}_metadata_{metadata_page_id}")

	# this calculated property gives the path for a "table file". 
	# Table files are files which apply to the entire table. These files are, as of now, 
	# "catalog", "page_directory.pickle", and "indices.pickle". The page directory and indices
	# files are only specified by their names (even though they will be persisted separately with pickle)
	def table_file_path(self, file_name: Literal["catalog", "page_directory", "indices"]) -> str:
		path = os.path.join(self.table_path, file_name)
		if file_name == "page_directory" or file_name == "indices":
			path += ".pickle" # these files have the .pickle extension
		return path

	def page_id_to_path(self, page_id: PageID) -> str:
		path: str = ""
		if isinstance(page_id, BasePageID):
			path = self.base_path(page_id)
		elif isinstance(page_id, TailPageID):
			path = self.tail_path(page_id)
		elif isinstance(page_id, BaseMetadataPageID) or isinstance(page_id, TailMetadataPageID):
			path = self.metadata_path(page_id)
		else:
			raise(Exception(f"page_id had unexpected type of {type(page_id)}"))
		return path

	@staticmethod
	def write_position(page_path: str, byte_position: int, value: int) -> bool:
		with open(page_path, "r+b") as file:
			file.seek(byte_position)
			file.write(value.to_bytes(config.BYTES_PER_INT, byteorder="big"))
		file.close()
		return True

	def read_value_page_directory(self) -> dict[int, 'PageDirectoryEntry']:
		page_path = self.page_path("page_directory")
		ret: dict[int, 'PageDirectoryEntry'] 
		with open(page_path, "rb") as handle:
			ret = pickle.load(handle) # this is not typesafe at all.... ohwell
		handle.close()
		return ret

	def read_int_value(self, page_sub_path: PageID | Literal["catalog"], byte_position: int) -> int:
		page_path = self.page_path(page_sub_path)
		ret: int = 0
		with open(page_path, "rb") as file:
			assert byte_position is not None
			file.seek(byte_position)
			ret = int.from_bytes(file.read(8), "big")
		file.close()
		return ret
	
	def read_dict_value(self, page_sub_path: Literal["page_directory", "indices"]) -> dict:
		page_path = self.page_path(page_sub_path)
		ret: dict
		with open(page_path, "rb") as handle:
			ret = pickle.load(handle)
		handle.close()
		return ret

	@staticmethod
	def is_valid_table_file_name(name: Any) -> TypeGuard[Literal["catalog", "page_directory", "indices"]]:
		return name == "catalog" or name == "page_directory" or name == "indices"

	# returns the full page path, given a particular pageID OR 
	# the special catalog/page_directory files
	def page_path(self, page_sub_path: PageID | Literal["catalog", "page_directory", "indices"]) -> str:
		if isinstance(page_sub_path, BasePageID) or isinstance(page_sub_path, TailPageID) or isinstance(page_sub_path, BaseMetadataPageID) or isinstance(page_sub_path, TailMetadataPageID):
			return self.page_id_to_path(page_sub_path)
		elif FileHandler.is_valid_table_file_name(page_sub_path):
			return self.table_file_path(page_sub_path)
		else:
			raise(Exception(f"unexpected page_sub_path {page_sub_path}"))

	# reads the full base or tail page written to disk. will follow metadata pointer for metadata as well
	# set projected_columns_index to None to get all columns.
	# returns None for any pages not requested
	def read_projected_cols_of_page(self, page_id: BaseTailPageID, projected_columns_index: list[Literal[0, 1]] | None = None) -> FilePageReadResult | None: 
		projected_columns_idx: Any = ([1] * self.table.num_columns) if projected_columns_index is None else projected_columns_index  # type: ignore
		assert projected_columns_idx is not None
		physical_pages: list[PhysicalPage | None] = [None] * self.table.num_columns
		metadata_pages: list[PhysicalPage | None] = [None] * config.NUM_METADATA_COL

		metadata_buff = PBBaseMetadataPageID(self, page_id, config.byte_position.base_tail.METADATA_PTR) if isinstance(page_id, BasePageID) else PBTailMetadataPageID(self, page_id, config.byte_position.base_tail.METADATA_PTR)
		val = metadata_buff.value()
		# assert isMetadataPageID(val)
		assert isinstance(val, BaseMetadataPageID) or isinstance(val, TailMetadataPageID)

		metadata_path = self.metadata_path(val)
		path = self.page_id_to_path(page_id)
		if not os.path.isfile(metadata_path) or not os.path.isfile(path):
			return None
		
		offset = self.read_int_value(page_id, config.byte_position.base_tail.OFFSET)
		# read selected metadata
		with open(metadata_path, "rb") as metadata_file:
			metadata_file.seek(config.byte_position.metadata.DATA)
			for i in range(config.NUM_METADATA_COL):
				metadata_pages[i] = PhysicalPage(data=bytearray(metadata_file.read(config.PHYSICAL_PAGE_SIZE)), offset=offset)
		metadata_file.close()

		# read selected data
		with open(path, "rb") as file: 
			file.seek(config.byte_position.base_tail.DATA)
			for i in range(self.table.num_columns):
				if projected_columns_idx[i] == 1:
					physical_pages[i] = PhysicalPage(data=bytearray(file.read(config.PHYSICAL_PAGE_SIZE)), offset=offset)
					# physical_pages.append(PhysicalPage(data=bytearray(file.read(config.PHYSICAL_PAGE_SIZE)), offset=offset))
				else:
					# physical_pages.append(None)
					file.seek(config.PHYSICAL_PAGE_SIZE, 1) # seek 4096 (or size) bytes forward from current position (the 1 means "from current position")
		file.close()
		return FilePageReadResult(metadata_pages, physical_pages)

	def insert_base_record(self, metadata: WriteSpecifiedBaseMetadata, *columns: int | None) -> int: # returns RID of inserted record
		curr_offset = self.base_offset.value()
		metadata_page_id = self.next_base_metadata_page_id.value()
		base_page_id = self.next_base_page_id.value()
		proj_data_cols = list(map(lambda col: 1 if col is not None else 0, columns))
		if curr_offset == config.PHYSICAL_PAGE_SIZE - config.BYTES_PER_INT:
			self.next_base_page_id.value(1)
			self.next_base_metadata_page_id.value(1)
			metadata_page_id = self.next_base_metadata_page_id.value()
			base_page_id = self.next_base_page_id.value()
			print(f"set new base_page_id to {base_page_id}")
			self.initialize_metadata_file(metadata_page_id)
			self.initialize_base_tail_page(base_page_id, metadata_page_id)
			self.base_offset.flush() # flush the old base_offset before making a new one
			self.base_offset = PsuedoBuffIntValue(self, base_page_id, config.byte_position.base_tail.OFFSET)
			curr_offset = self.base_offset.value()

		# get the indices in the bufferpool where data_pages and metadata_pages are
		# append a new record at the curr_offset
		# increment curr_offset
		found, metadata_buff_indices, data_buff_indices = self.table.db_bpool.is_page_in_bufferpool(self.table, base_page_id, proj_data_cols) # type: ignore[arg-type]
		assert self.table.db_bpool.bring_from_disk(self.table, base_page_id, proj_data_cols, None) # type: ignore[arg-type]
		
		return -1

	def insert_tail_record(self, metadata: WriteSpecifiedTailMetadata, *columns: int | None) -> int: # returns RID of inserted record
		null_bitmask = 0
		total_cols = self.table.total_columns
		if metadata.indirection_column == None: # set 1 for null indirection column
			null_bitmask = helper.ith_total_col_shift(total_cols, config.INDIRECTION_COLUMN)
		for idx, column in enumerate(columns):
			if column is None:
				null_bitmask = null_bitmask | helper.ith_total_col_shift(len(columns), idx, False) #
			
		list_columns: list[int | None] = list(columns)
		data_columns = list_columns
		metadata_columns: list[int | None] = []
		tid = self.next_tail_rid.value(-1) # tail RIDs decrease
		metadata_columns.insert(config.INDIRECTION_COLUMN, metadata.indirection_column)
		metadata_columns.insert(config.RID_COLUMN, tid)
		metadata_columns.insert(config.TIMESTAMP_COLUMN, int(time.time()))
		metadata_columns.insert(config.SCHEMA_ENCODING_COLUMN, metadata.schema_encoding)
		metadata_columns.insert(config.NULL_COLUMN, null_bitmask)
		metadata_columns.insert(config.BASE_RID, metadata.base_rid)
		# cols = tuple(data_columns + metadata_columns)
		curr_offset = self.tail_offset.value()
		metadata_page_id = self.next_tail_metadata_page_id.value()
		tail_page_id = self.next_tail_page_id.value()
		if curr_offset == config.PHYSICAL_PAGE_SIZE: 
			self.next_tail_page_id.value(1)
			self.next_tail_metadata_page_id.value(1)
			metadata_page_id = self.next_tail_metadata_page_id.value()
			tail_page_id = self.next_tail_page_id.value()
			# print(f"set new base_page_id to {base_page_id}")
			self.initialize_metadata_file(metadata_page_id)
			self.initialize_base_tail_page(tail_page_id, metadata_page_id)
			self.tail_offset.flush() # flush the old base_offset before making a new one
			self.tail_offset = PsuedoBuffIntValue(self, tail_page_id, config.byte_position.base_tail.OFFSET)
			curr_offset = self.tail_offset.value()
			# print(f"set curr_offset to {curr_offset}")
			# self.base_offset.value(-config.PHYSICAL_PAGE_SIZE) # set to 0
			assert self.tail_offset.value() == 0

		metadata_path = self.metadata_path(TailMetadataPageID(metadata_page_id))
		with open(metadata_path, "r+b") as mfile:
			mfile.seek(8)
			mfile.seek(curr_offset, 1)
			for mcol in metadata_columns:
				mfile.write(int.to_bytes(mcol if mcol is not None else 0, config.BYTES_PER_INT, "big"))
				mfile.seek(config.PHYSICAL_PAGE_SIZE - config.BYTES_PER_INT, 1)
		mfile.close()
		

		tail_path = self.tail_path(TailPageID(tail_page_id))
		# print(f"base path = {base_path}")
		with open(tail_path, "r+b") as tfile:
			tfile.seek(24)
			tfile.seek(curr_offset, 1)
			for dcol in data_columns:
				tfile.write(int.to_bytes(dcol if dcol is not None else 0, config.BYTES_PER_INT, "big"))
				tfile.seek(config.PHYSICAL_PAGE_SIZE - config.BYTES_PER_INT, 1) 
		tfile.close()
		pg_dir_entry = PageDirectoryEntry(TailPageID(tail_page_id), TailMetadataPageID(metadata_page_id), self.tail_offset.value(), "tail")
		self.table.page_directory_buff.value_assign(tid, pg_dir_entry)
		self.tail_offset.value(config.BYTES_PER_INT)
		return tid

	def initialize_table_files(self) -> None:
		# initialize catalog file
		catalog_path = self.table_file_path("catalog")
		open(catalog_path, "xb")
		with open(catalog_path, "w+b") as catalog_file:
			helper.write_int(catalog_file, self.table.num_columns)
			helper.write_int(catalog_file, self.table.key_index)
			# initialze IDs (page ids, rid)
			helper.write_int(catalog_file, 1)
			helper.write_int(catalog_file, 1)
			helper.write_int(catalog_file, 1)
			helper.write_int(catalog_file, 1)
			helper.write_int(catalog_file, 1) # RIDs start at one..
			helper.write_int(catalog_file, config.INITIAL_TID) # but TIDs start at 2^64 - 1
		catalog_file.close()

		page_dir_path = self.table_file_path('page_directory')
		open(page_dir_path, "x")
		with open(page_dir_path, "w+b") as page_directory_file:
			pickle.dump({}, page_directory_file)
			pass # just create the file, it should be empty
		page_directory_file.close()

		# final_path=os.path.join(newpath,"page_directory")
		index_path = self.table_file_path("indices")
		open(index_path, "x")
		with open(index_path, "w+b") as index_file:
			pickle.dump(Index(self.table.num_columns), index_file)
		index_file.close()

	def initialize_base_tail_page(self, page_id: BasePageID | TailPageID, metadata_id: BaseMetadataPageID | TailMetadataPageID) -> None:
		#print(f"initializing page id {page_id} of type {type(page_id)}")
		page_path = self.page_id_to_path(page_id)
		open(page_path, "xb") # create the file
		with open(page_path, "w+b") as base_file:
			# base_file.write(metadata_id.to_bytes(config.BYTES_PER_INT, "big"))
			helper.write_int(base_file, metadata_id) # the first base page points to metadata page 1
			helper.write_int(base_file, 0) # offset starts at 0
			helper.write_int(base_file, config.INITIAL_TPS) # TPS starts at 2**64
			for _ in range(self.table.num_columns): # write empty physical page for first physical pages
				base_file.write(bytearray(config.PHYSICAL_PAGE_SIZE))
		base_file.close()

	def initialize_metadata_file(self, page_id: BaseMetadataPageID | TailMetadataPageID) -> None:
		page_path = self.page_id_to_path(page_id)
		open(page_path, "xb") 
		with open(page_path, "w+b") as file: # open metadata file
			helper.write_int(file, 0) # starting offset = 0
			for _ in range(config.NUM_METADATA_COL):
				file.write(bytearray(config.PHYSICAL_PAGE_SIZE)) # write the metadata columns
		file.close()
	
	def flush(self) -> None:
		self.next_base_page_id.flush()
		self.next_tail_page_id.flush()
		self.next_base_metadata_page_id.flush()
		self.next_tail_metadata_page_id.flush()
		self.next_base_rid.flush()
		self.next_tail_rid.flush()
		self.base_offset.flush()
		self.tail_offset.flush()
		self.is_flushed = True

	def __del__(self) -> None:
		if not self.is_flushed:
			raise Exception("File Handler of table: %s was not flushed", self.table)
		

PageDirectory = dict[int, PageDirectoryEntry]

class Table:
	"""
	:param name: string         #Table name
	:param num_columns: int     #Number of Columns: all columns are integer
	:param key: int             #Index of table key in columns
	"""

	def __init__(self, name: str, num_columns: int, key_index: DataIndex, db_path: str, db_bpool: Bufferpool):
		self.name: str = name
		self.key_index = DataIndex(key_index)
		self.num_columns: int = num_columns # data columns only
		self.total_columns = self.num_columns + config.NUM_METADATA_COL # inclding metadata
		self.db_path = db_path
		self.file_handler = FileHandler(self)
		self.page_directory_buff = PseudoBuffDictValue[int, PageDirectoryEntry](self.file_handler, "page_directory")
		# self.last_rid = 1

		# ## second milestone
		# self.last_physical_page_id=None
		# self.last_tail_id=None  
		# ####
		
		
		# Page Directory:
		# {Rid: (Page, offset)}
		from lstore.index import Index
		self.index = Index(self.num_columns)

		self.db_bpool = db_bpool
		# create a B-tree index object for the key index (hard-coded for M1)
		self.index.create_index(self.key_index)

	def __del__(self) -> None:
		pass
		# self.file_handler.flush()
		#print(f"deleting TABLE {self.name}")



	def ith_total_col_shift(self, col_idx: RawIndex) -> int: # returns the bit vector shifted to the indicated col idx
		return 0b1 << (self.total_columns - col_idx - 1)


	# TODO: uncomment

	
	# def bring_base_pages_to_memory(self)-> None:
	# 	#list_base_pages=[]
	# 	for table_name in os.listdir(self.db_path):
	# 		if self.name==table_name:
	# 			table_path=os.path.join(self.db_path,table_name)
	# 			catalog_path = os.path.join(self.db_path,"catalog")
	# 			with open(catalog_path, 'rb') as catalog:
	# 				#get the catalog to create the table 
	# 				table_num_columns= int.from_bytes(catalog.read(8))
	# 				table_key_index= int.from_bytes(catalog.read(8))
	# 				table_pages_per_range = int.from_bytes(catalog.read(8))
	# 				table_last_page_id = int.from_bytes(catalog.read(8))
	# 				table_last_tail_id= int.from_bytes(catalog.read(8))
	# 				table_last_rid= int.from_bytes(catalog.read(8))
				
	# 			for file in os.listdir(table_path):
	# 				if file =="*page*":
	# 					page_id=int(file.split("_")[1]) # take the page id, may not work :( 
	# 	#                 #page= BasePage(table_num_columns, DataIndex(table_key_index))
	# 	#                 #page.id=page_id
	# 	#                 page_path = os.path.join(table_path,page_id)
	# 	#                 with open(page_path, "rb") as page_file:
	# 	#                     metadata_id= int(page_file.read(8))
	# 	#                     offset=  int(page_file.read(8))
	# 	#                     page_range_id=int(page_file.read(8))
	# 	#                     if page_range_id== self.page_range_id:
	# 	#                         metadata_path=os.path.join(table_path,metadata_id)
	# 	#                         with open(metadata_path,"rb") as metadata_file:
	# 	#                             rid=metadata_file.read(offset)
	# 	#                             timestamp=metadata_file.read(offset)
	# 	#                             indirection_column=metadata_file.read(offset)
	# 	#                             schema_encoding=metadata_file.read(offset)
	# 	#                             null_column=metadata_file.read(offset)
	# 	#                         list_physical_pages=[]
	# 	#                         list_physical_pages.append(PhysicalPage(bytearray(rid), offset))
	# 	#                         list_physical_pages.append(PhysicalPage(bytearray(timestamp), offset))
	# 	#                         list_physical_pages.append(PhysicalPage(bytearray(indirection_column), offset))
	# 	#                         list_physical_pages.append(PhysicalPage(bytearray(schema_encoding), offset))
	# 	#                         list_physical_pages.append(PhysicalPage(bytearray(null_column), offset))
	# 	#                         while True:
	# 	#                             physical_page_information=page_file.read(offset)
									
	# 	#                             if not physical_page_information:
	# 	#                                 break
									
	# 	#                             physical_page_data = bytearray(physical_page_information)
	# 	#                             physical_page = PhysicalPage(physical_page_data,offset)
						
	# 					file_page_read_result=self.file_handler.read_full_page(BasePageID(page_id))     

	# 					data = file_page_read_result.data_physical_pages
	# 					metadata = file_page_read_result.metadata_physical_pages
	# 					file_result = FilePageReadResult(metadata, data, 'base')
						
	# 					self.get_updated_base_page(file_result, BasePageID(page_id))

	
	# def merge(self):
	# 	list_base_page=self.bring_base_pages_to_memory()
	# 	pass

	# def get_updated_base_page(self,file_page_read_result: FilePageReadResult,page_id: BasePageID) -> None:
	# 	table.file_handler.write_new_base_page()
	# 	table.file_handler.write_new_tail_page()
	# 	object_to_get_tps=PsuedoBuffIntValue(self.file_handler, page_id, config.byte_position.base_tail.TPS)
	# 	#tps=self.get_updated_base_page(file_page_read_result,object_to_get_tps.value())
	# 	tps=object_to_get_tps.value()
	# 	physical_pages=file_page_read_result.data_physical_pages
	# 	metadata=file_page_read_result.metadata_physical_pages
	# 	total_columns=len(physical_pages)+ len(metadata)
	# 	if physical_pages[0] is not None:
	# 		offset=physical_pages[0].offset 
	# 	num_records=offset/8

	# 	for i in range(int(num_records)):
	# 		for j in range(len(physical_pages)): #iterate through all the columns of a record
	# 			not_null_indirection_column=helper.not_null(metadata[config.INDIRECTION_COLUMN])
	# 			indirection_column=not_null_indirection_column.data[8*i : 8*(i+1)]
	# 			not_null_schema_encoding=helper.not_null(metadata[config.SCHEMA_ENCODING_COLUMN])
	# 			schema_encoding=not_null_schema_encoding.data[8*i : 8*(i+1)]
	# 			not_null_null_column=helper.not_null(metadata[config.NULL_COLUMN])
	# 			null_column=not_null_null_column.data[8*i : 8*(i+1)]


	# 			tail_indirection_column=indirection_column
	# 			tail_schema_encoding=schema_encoding
	# 			tail_physical_page=physical_pages
	# 			tail_metadata_page=metadata
	# 			tail_offset=i
	# 			tail_null_column=null_column

	# 			## check tps 
	# 			if int.from_bytes(tail_indirection_column) < tps:
	# 				break
	# 			## check if deleted 
	# 			if int.from_bytes(tail_null_column) & 1 << (total_columns-j)!=0: ## check this 
	# 				break
			
	# 			while int.from_bytes(tail_schema_encoding) & 1 << (total_columns-j)!=0: #column has been updated
	# 			#loop for retreiving information not updated 
	# 				tail_page_directory_entry = self.page_directory_buff[int.from_bytes(tail_indirection_column)]
	# 				tail_offset=tail_page_directory_entry.offset
	# 				tail_page_id = tail_page_directory_entry.page_id  ## tail page id 
					
	# 				tail=self.file_handler.read_full_page(tail_page_id)
					
	# 				tail_physical_page=tail.data_physical_pages
	# 				tail_metadata_page=tail.metadata_physical_pages

	# 				not_none_tail_indirection_column=helper.not_null(tail_metadata_page[config.INDIRECTION_COLUMN])
	# 				tail_indirection_column=not_none_tail_indirection_column.data[8*tail_offset: 8*(tail_offset+1)]
	# 				not_none_tail_schema_encoding=helper.not_null(tail_metadata_page[config.SCHEMA_ENCODING_COLUMN])
	# 				tail_schema_encoding=not_none_tail_schema_encoding.data[8*tail_offset : 8*(tail_offset+1)]
	# 				not_none_tail_null_column=helper.not_null(metadata[config.NULL_COLUMN])
	# 				tail_null_column=not_none_tail_null_column.data[8*tail_offset : 8*(tail_offset+1)]
					
	# 			not_null_physical_pages = helper.not_null(physical_pages[j])
	# 			not_tail_physical_pages = helper.not_null(tail_physical_page[i])
	# 			not_null_physical_pages.data[8*i : 8*(i+1)]=not_tail_physical_pages.data[tail_offset*8:(tail_offset+1)*8]
	# 	#change schema encoding of the updated entry of the base page 
	# 		please_give_us_an_A = helper.not_null(metadata[config.SCHEMA_ENCODING_COLUMN])
	# 		please_give_us_an_A.data[i*8:8*(i+1)]=0
		
	# 	final_physical_pages=metadata+physical_pages
	# 	#create new base page file with the updated information
	# 	self.file_handler.write_new_base_page()
	# 	object_to_get_tps._value = int.from_bytes(indirection_column)
	# 	object_to_get_tps.flush()

class BufferedPage:
	def __del__(self) -> None:
		self.bufferpool.change_pin_count(self.data_buff_indices, -1)
		self.bufferpool.change_pin_count(self.metadata_buff_indices, -1)
		pass
	def __init__(self, bufferpool: Bufferpool, table: Table, data_buff_indices: List[BufferpoolIndex | None], metadata_buff_indices: List[BufferpoolIndex], projected_columns_index: List[Literal[0, 1]]):
		# self.initialized = False
		self.bufferpool = bufferpool
		self.data_buff_indices = data_buff_indices # None if not in projected_columns_index 
		self.metadata_buff_indices = metadata_buff_indices
		self.table = table
		self.projected_columns_index = projected_columns_index
		bufferpool.change_pin_count(data_buff_indices, +1) # increment pin counts of relevant bufferpool frames
		bufferpool.change_pin_count(metadata_buff_indices, +1) # increment pin counts of relevant bufferpool frames


class BufferedRecord:
	def __del__(self) -> None:
		self.bufferpool.change_pin_count(self.metadata_buff_indices + self.data_buff_indices, -1)
		pass
	def __init__(self, bufferpool: Bufferpool, table: Table, buff_indices: List[BufferpoolIndex], record_offset: int, record_id: RID, projected_columns_index: List[Literal[0, 1]]):
		self.bufferpool = bufferpool
		self.metadata_buff_indices = metadata_buff_indices
		self.data_buff_indices = data_buff_indices
		self.table = table
		self.record_offset = record_offset
		self.record_id = record_id
		self.projected_columns_index = projected_columns_index
		bufferpool.change_pin_count(buff_indices, +1) # increment pin counts of relevant bufferpool frames

	def add_buff_idx(self, buff_idx: BufferpoolIndex) -> None:
		self.buff_indices.append(buff_idx)
		self.bufferpool[buff_idx].pin_count += 1 # will be decremented later because it was addded to base_buff_indices

	def unpin_buff_indices(self, buff_indices: List[BufferpoolIndex]) -> None:
		for buff_idx in buff_indices:
			self.bufferpool[buff_idx].pin_count -= 1
			self.buff_indices.remove(buff_idx)

	def get_value(self) -> Record:
		num_cols = len(self.projected_columns_index) # number of data_cols
		# will be set to none if that physical page is not in the projected columns
		metadata_cols: Annotated[List[PhysicalPage], num_cols] = [] # no record should have partial metadata.  
		data_cols: Annotated[List[PhysicalPage | None], num_cols] = [None] * num_cols
		for m_buff_idx in self.metadata_buff_indices:
			col_idx = self.bufferpool[m_buff_idx].physical_page_index
			assert col_idx is not None, "column held by BufferedRecord was None!"
			physical_page = self.bufferpool[m_buff_idx].physical_page
			metadata_cols.append(physical_page)
		for d_buff_idx in self.data_buff_indices:
			if d_buff_idx is None:
				data_cols.append(None)
			else:
				col_idx = self.bufferpool[d_buff_idx].physical_page_index
				assert col_idx is not None, "column held by BufferedRecord was None!"
				physical_page = self.bufferpool[d_buff_idx].physical_page
				data_cols.append(physical_page)
				data_cols[col_idx.toDataIndex()] = physical_page

		all_cols = metadata_cols + data_cols

		def get_no_none_check(col_idx: RawIndex, record_offset: int) -> int:
			return helper.unpack_data(helper.not_null(all_cols[col_idx]).data, record_offset)
		
		def get_check_for_none(col_idx: RawIndex, record_offset: int) -> int | None:
			val = get_no_none_check(col_idx, record_offset)
			if val == 0:
				# breaking an abstraction barrier for convenience right now. TODO: fix?
				thing = helper.unpack_data(helper.not_null(all_cols[config.NULL_COLUMN]).data, record_offset)
				# thing = helper.unpack_col(self, config.NULL_COLUMN, record_offset / config.BYTES_PER_INT)
				# thing = struct.unpack(config.PACKING_FORMAT_STR, self.physical_pages[config.NULL_COLUMN].data[(record_idx * 8):(record_idx * 8)+8])[0]
				# is_none = (self.physical_pages[config.NULL_COLUMN].data[(record_idx * 8):(record_idx * 8)+8] == b'x01')

				# is_none = ( thing >> ( self.num_columns + config.NUM_METADATA_COL - col_idx - 1 ) ) & 1
				is_none = helper.ith_bit(thing, self.table.total_columns, col_idx)
				if is_none == 1:
					return None
			return val
			

		indirection_column, rid, schema_encoding, timestamp,  null_col, base_rid = \
			get_check_for_none(config.INDIRECTION_COLUMN, self.record_offset), \
			get_check_for_none(config.RID_COLUMN, self.record_offset), \
			get_no_none_check(config.SCHEMA_ENCODING_COLUMN, self.record_offset), \
			get_no_none_check(config.TIMESTAMP_COLUMN, self.record_offset), \
			get_check_for_none(config.NULL_COLUMN, self.record_offset), \
			get_no_none_check(config.BASE_RID, self.record_offset), \

		
		# check the first place where the self.data_buff_indices is not None (ie the first requested column),
		# then get the page type of that index
		# this is just outside the metadata column range. thus it should reveal the actual page type of base or tail (both base and tail pages will have other "metadata" type pages in the bufferpool)
		first_non_null_data_buff_idx = self.data_buff_indices[[i for i in range(len(self.projected_columns_index)) if self.data_buff_indices[i] is not None][0]]
		assert first_non_null_data_buff_idx is not None
		page_type = self.bufferpool.get_page_type(first_non_null_data_buff_idx)
		# page_type = self.bufferpool[self.buff_indices[config.NUM_METADATA_COL]].page_type
		is_base_page = False
		if page_type == "base":
			is_base_page = True
		else:
			raise(Exception("should not be getting metadata page type in get_record."))

		columns: Annotated[List[int | None], num_cols] = [None] * num_cols

		# the data_cols variable contains only the data columns in the order they 
		# were specified in the projected_columns.
		j = 0
		for i, proj in enumerate(self.projected_columns_index):
			if proj == 1:
				data_col = data_cols[i]
				assert data_col is not None, "this record is missing a column specified in the projected_columns_index"
				columns[i] = helper.unpack_data(data_col.data, self.record_offset)
				j += 1

		def rid_type(rid: int | None) -> RID:
			if rid is None:
				return None
			return BaseRID(rid) if rid < self.table.file_handler.next_tail_rid.value() else TailRID(rid)
		return Record(FullMetadata(rid_type(rid), timestamp, rid_type(indirection_column), schema_encoding, null_col, rid_type(base_rid)), is_base_page, *columns)


class Bufferpool:
	TProjected_Columns = List[Literal[0, 1]]
	def __init__(self, path: str) -> None: 
		self.entries: Annotated[List[BufferpoolEntry | None], config.BUFFERPOOL_SIZE] = [None] * config.BUFFERPOOL_SIZE
		self.path = path
		self.curr_clock_hand = 0
	
	# def get_item(self, key: int) -> BufferpoolEntry | None:
	# 	entry = self.entries[key]
	# 	return entry

	# look up an rid in the page directory to get its pageID
	@staticmethod
	def rid_to_page_id(table: Table, rid: RID) -> BaseTailPageID:
		page_dir = table.page_directory_buff.value_get()
		if not rid in page_dir:
			raise(Exception("RECORD NOT PRESENT"))
			return BufferpoolRecordSearchResult(False, [], [], None)
		page_directory_entry = page_dir[rid]
		return page_directory_entry.page_id

	# look up an rid in the page directory to get its offset
	@staticmethod
	def rid_to_offset(table: Table, rid: RID) -> int:
		page_dir = table.page_directory_buff.value_get()
		if not rid in page_dir:
			raise(Exception("RECORD NOT PRESENT"))
			return BufferpoolRecordSearchResult(False, [], [], None)
		page_directory_entry = page_dir[rid]
		return page_directory_entry.offset

	def __getitem__(self, key: int) -> BufferpoolEntry:
		entry = self.entries[key]
		assert entry is not None, "tried to get a BufferpoolEntry but it was none (and allow_none was set to its default, False)" 
		return entry
	
	def maybe_get_entry(self, key: int) -> BufferpoolEntry | None:
		return self.entries[key]
	
	def __setitem__(self, key: int, item: BufferpoolEntry | None) -> None:
		self.entries[key] = item

	def close_bufferpool(self) -> None:
		for i in [BufferpoolIndex(_) for _ in range(config.BUFFERPOOL_SIZE)]:
			if self.entries[i]!=None:
				if self[i].dirty_bit==True:
					table = self[i].table
					self.write_to_disk(table, i)
				self.change_bufferpool_entry(None,i)
				# bufferpool_entry=BufferpoolEntry(0, None, False,  None,  None,  None,  None)
	
	def change_bufferpool_entry(self, entry: BufferpoolEntry | None, buff_idx: BufferpoolIndex) -> None:
		self[buff_idx] = entry

	def get_page_type(self, buff_idx: BufferpoolIndex) -> TPageType:
		page_id = self[buff_idx].physical_page_id
		if isinstance(page_id, BasePageID):
			return "base"
		elif isinstance(page_id, TailPageID):
			return "tail"
		elif isinstance(page_id, BaseMetadataPageID):
			return "base_metadata"
		elif isinstance(page_id, TailMetadataPageID):
			return "tail_metadata"
		else:
			raise(Exception("couldn't get page type for a page in the bufferpool because the ID type didn't match any expected type"))

	@staticmethod
	def make_page_id(page_id: int, page_type: TPageType) -> PageID:
		match page_type:
			case "base":
				return BasePageID(page_id)
			case "tail":
				return TailPageID(page_id)
			case "base_metadata":
				return BaseMetadataPageID(page_id)
			case "tail_metadata":
				return TailMetadataPageID(page_id)

	def change_pin_count(self, buff_indices: list[BufferpoolIndex], change: int) -> None:
		for idx in buff_indices:
			if idx is not None:
				self[idx].pin_count += change
		# print(f"pin counts are now {list(map(lambda e: e.pin_count if e is not None else None, self.entries))}")

	
	def insert_base_record(self, table: Table, metadata: WriteSpecifiedBaseMetadata, *columns: int | None) -> int: # returns RID of inserted record
		return table.file_handler.insert_base_record(metadata, *columns)

	def insert_tail_record(self, table: Table, metadata: WriteSpecifiedTailMetadata, *columns: int | None) -> int:
		return table.file_handler.insert_tail_record(metadata, *columns)


	# checks if the requested columns (specified by projected_columns_index) of a page are inside the bufferpool.
	# if they are, return the bufferpool frame indices in which the columns can be found. 
	def is_page_in_bufferpool(self, table: Table, page_id: PageID, projected_columns_index: List[Literal[0] | Literal[1]]) -> BufferpoolPageSearchResult:
		requested_columns: list[DataIndex] = [DataIndex(i) for i, binary_item in enumerate(projected_columns_index) if binary_item == 1]
		
		# the ith element of this array is:
			# None, if not requested;
			# -1, if requested but not found;
			# the ith data column, if found and requested.
		data_buff_indices: list[None | TNOT_FOUND | BufferpoolIndex] = [None] * table.num_columns
		for col in requested_columns:
			data_buff_indices[col] = -1
		# after this point, data_columns_found array is NOT_FOUND if requested and None if not requested.

		metadata_buff_indices: List[BufferpoolIndex | TNOT_FOUND] = [-1] * config.NUM_METADATA_COL
		#column_list = [None] * len(projected_columns_index)

		for i in [BufferpoolIndex(_) for _ in range(config.BUFFERPOOL_SIZE)]:
			if self.maybe_get_entry(i) is None:
				continue
			if helper.eq(self[i].physical_page_id, page_id, False):
				print(f"potential match... for entry idx {i} and type {type(self[i].physical_page_id)}")
				raw_idx = self[i].physical_page_index
				assert raw_idx is not None, "non None in ids_of_physical_pages but None in index_of_physical_page_in_page?"
				if raw_idx < config.NUM_METADATA_COL:
					print(f"adding metadata idx {raw_idx} with value {i} for record type ")
					metadata_buff_indices[raw_idx] = i
				else:
					data_idx = raw_idx.toDataIndex() 
					if data_idx in requested_columns: # is a data column we requested?
						print("adding data buff idx")
						data_buff_indices[data_idx] = i
						# print("FOUND something")
					else:
						print(f"data_idx was {data_idx} but requested_columns was {requested_columns}")
						continue

		found = (len([True for idx in data_buff_indices if idx == -1]) == 0) and (len([True for idx in metadata_buff_indices if idx == -1]) == 0)
		return BufferpoolPageSearchResult(found, data_buff_indices, metadata_buff_indices)

	def is_record_in_bufferpool(self, table: Table, rid : RID, projected_columns_index: list[Literal[0] | Literal[1]]) -> BufferpoolRecordSearchResult: ## this will be called inside the bufferpool functions
		page_id = self.rid_to_page_id(table, rid)
		page_res = self.is_page_in_bufferpool(table, page_id, projected_columns_index)
		return BufferpoolRecordSearchResult(page_res.found, page_res.data_buff_indices, page_res.metadata_buff_indices, self.rid_to_offset(table, rid))

	def delete_nth_record(self, table : Table, page_id: PageID, offset :int) -> bool:
		bitmask = table.ith_total_col_shift(config.RID_COLUMN)
		null_column_marked=False
		rid_column_marked=False
		for entry in self.entries:
			if entry is not None and entry.physical_page_id == page_id and entry.physical_page_index == config.NULL_COLUMN:
				entry.physical_page.data[offset:offset+8] = int.to_bytes(bitmask, config.BYTES_PER_INT, "big")
				null_column_marked=True
			if entry is not None and entry.physical_page_id == page_id and entry.physical_page_index == config.RID_COLUMN:
				entry.physical_page.data[offset:offset+8] = int.to_bytes(0, config.BYTES_PER_INT, "big")
				rid_column_marked=True
		return rid_column_marked and null_column_marked
	
	# updates a column of a specified record in place.
	# returns True on success
	def update_col_record_inplace(self, table: Table, rid: RID, raw_col_idx: RawIndex, new_value: int) -> bool:
		is_metadata = raw_col_idx < config.NUM_METADATA_COL
		proj_data_idx: List[Literal[0, 1]] = [0] * table.num_columns
		proj_metadata_idx: List[Literal[0, 1]] = [0] * config.NUM_METADATA_COL
		assert rid is not None
		page_dir_entry = table.page_directory_buff[rid]
		buff_idx: BufferpoolIndex | Literal[-1] = -1

		desired_page_type = "base" if isinstance(rid, BaseRID) else "tail"

		if is_metadata: # this is a metadata column
			proj_metadata_idx[raw_col_idx] = 1
			desired_page_type += "_metadata" # base_metadata type
		else:
			proj_data_idx[raw_col_idx] = 1
			# desired_page_type remains as "base"
		
		# col_idx = raw_col_idx.toDataIndex()

		# ## check if that column is in the bufferpool already
		# for i in [BufferpoolIndex(_) for _ in range(config.BUFFERPOOL_SIZE)]: # search the entire bufferpool for columns
		# 	if self.maybe_get_entry(i) is None:
		# 		continue
		# 	if self[i].physical_page_id == page_dir_entry.page_id and type(self[i].physical_page_id) == type(page_dir_entry.page_id):
		# 		raw_idx = self[i].physical_page_index
		# 		assert raw_idx is not None, "non None in ids_of_physical_pages but None in index_of_physical_page_in_page?"
		# 		# data_idx = raw_idx.toDataIndex()
		# 		if raw_idx == raw_col_idx: # is the column we want?
		# 			buff_idx = i
		# 			break # found the column. since we are only looking for one column, we can break here
		
		

		## if the column is not in the bufferpool, bring it in
		# if buff_idx == -1:
		# 	read_res = table.file_handler.read_projected_cols_of_page(page_dir_entry.page_id, proj_data_idx, proj_metadata_idx)
		# 	assert read_res is not None
		# 	metadata_physical_pages, data_physical_pages = read_res
		# 	slots = self.evict_n_slots(1) # l
		# 	assert slots is not None and len(slots) == 1
		# 	physical_page = metadata_physical_pages[raw_col_idx] if is_metadata else data_physical_pages[raw_col_idx.toDataIndex()]
		# 	assert physical_page is not None
		# 	buff_idx = slots[0]
		# 	self[buff_idx] = BufferpoolEntry(0, physical_page, False, page_dir_entry.page_id, raw_col_idx, table)
		# 	# self.change_bufferpool_entry(BufferpoolEntry(0, ))

		page_id = self.rid_to_page_id(table, rid)
		indexes = self.get_page(table, page_id, proj_data_idx)
		page_offset = self.rid_to_offset(table, rid)
		
		assert indexes is not None	# should this be an assert???

		# now, we can actually update the value now that we know it's in the bufferpool
		self[indexes[raw_col_idx]].physical_page.data[page_offset : page_offset+config.BYTES_PER_INT] = helper.encode(new_value) 
		self[indexes[raw_col_idx]].dirty_bit = True # the value has been changed, it should be dirty
		return True
	
	def update_nth_record(self, page_id: PageID, offset: int, col_idx: int, new_value: int) -> bool:
		record_column_entry = None
		for entry in self.entries:
			if entry is not None and entry.physical_page_id == page_id and entry.physical_page_index == col_idx:
				record_column_entry = entry
		
		if record_column_entry is None: 
			return False
		record_column_entry.physical_page.data[offset: offset+8] = int.to_bytes(new_value, config.BYTES_PER_INT,"big")
		return True

	def get_updated_col(self, table: Table, record: Record, col_idx: DataIndex) -> int | None:
		if record.metadata.rid == None:
			print("deleted record")
			return None # deleted record
		# table: 'Table' = next(table for table in self.tables if table.name == table_name)
		desired_col: int | None = record[col_idx]
		schema_encoding = record.metadata.schema_encoding
		if helper.ith_bit(schema_encoding, table.num_columns, col_idx, False) == 0b1:
			curr_rid = record.metadata.indirection_column
			assert curr_rid is not None, "record rid wasn't none, so none of the indirections should be none either"
			proj_col: List[Literal[0, 1]] = [0] * table.num_columns
			proj_col[col_idx] = 1 # only get the desired column
			curr_record = self.get_record(table, curr_rid, proj_col)
			assert curr_record is not None, "a record with a non-None RID was not found"
			curr_schema_encoding = curr_record.get_value().metadata.schema_encoding ## this schema encoding doesn't work properly
			while helper.ith_bit(curr_schema_encoding, table.num_columns, col_idx, False) == 0b0: # while not found
				assert curr_record is not None, "a record with a non-None RID was not found"
				curr_rid = curr_record.get_value().metadata.indirection_column
				assert curr_rid is not None
				# curr_record.unpin()
				curr_record = self.get_record(table, curr_rid, proj_col)
				assert curr_record is not None
				curr_schema_encoding = curr_record.get_value().metadata.schema_encoding
			desired_col = curr_record.get_value()[col_idx]
			# curr_record.unpin()
		return desired_col 
	
	def get_version_col(self, table: Table, record: Record, col_idx: DataIndex, relative_version: int) -> int | None:
		# table.file_handler.write_new_base_page()
		if record.metadata.rid == None:
			return None # deleted record.
		# table: Table = next(table for table in self.tables if table.name == table_name)
		desired_col: int | None = record[col_idx]
		schema_encoding = record.metadata.schema_encoding
		if helper.ith_bit(schema_encoding, table.num_columns, col_idx, False) == 0b1:
			curr_rid = record.metadata.indirection_column
			assert curr_rid is not None, "record rid wasn't none, so none of the indirections should be none either"
			proj_col: List[Literal[0, 1]] = [0] * table.num_columns
			proj_col[col_idx] = 1 # only get the desired column
			buff_record = self.get_record(table, curr_rid, proj_col)
			assert buff_record is not None
			record = buff_record.get_value()
			curr_record = record
			assert curr_record is not None, "a record with a non-None RID was not found"
			curr_schema_encoding = curr_record.metadata.schema_encoding
			counter = 0
			overversioned = False
			assert curr_record is not None, "a record with a non-None RID was not found"
			curr_rid = curr_record.metadata.indirection_column
			while counter > relative_version or helper.ith_bit(curr_schema_encoding, table.num_columns, col_idx, False) == 0b0: # while not found
				assert curr_rid is not None
				temp = self.get_record(table, curr_rid, proj_col)
				if temp is None:
					overversioned = True
					break
				assert curr_record is not None
				curr_rid = temp.get_value().metadata.indirection_column
				assert curr_rid is not None, "potential incomplete delete?"
				record_buff = self.get_record(table, curr_rid, proj_col)
				assert record_buff is not None
				curr_record = record_buff.get_value()
				curr_schema_encoding = curr_record.metadata.schema_encoding
				counter -= 1
			if overversioned is True:
				curr_record = record
			desired_col = curr_record[col_idx]
		return desired_col
	
	def get_version_record(self, table: Table, record_id: RID, projected_columns_index: list[Literal[0] | Literal[1]], relative_version: int) -> Record | None:
		table.file_handler.flush()
		# table: Table = next(table for table in self.tables if table.name == table_name)

		# If there are multiple writers we probably need a lock here so the indirection column is not modified after we get it
		buffered_record = self.get_record(table, record_id, projected_columns_index)
		if buffered_record is None:
			return None
		columns: List[int | None] = []
		for i in range(len(projected_columns_index)):
			if projected_columns_index[i] == 1:
				columns.append(self.get_version_col(table, buffered_record.get_value(), DataIndex(i), relative_version))
			else:
				columns.append(None)
		version_record = Record(buffered_record.get_value().metadata, True, *columns)
		return version_record

	# THIS FUNCTION RECEIVES ONLY **BASE** RECORDS
	def get_updated_record(self, table: Table, record_id: RID, projected_columns_index: list[Literal[0] | Literal[1]]) -> Record | None:
		# table.file_handler.flush()
		# table: Table = next(table for table in self.tables if table.name == table_name)

		# If there are multiple writers we probably need a lock here so the indirection column is not modified after we get it
		buffered_record = self.get_record(table, record_id, projected_columns_index)
		if buffered_record is None:
			return None
		columns: List[int | None] = []
		for i in range(len(projected_columns_index)):
			if projected_columns_index[i] == 1:
				columns.append(self.get_updated_col(table, buffered_record.get_value(), DataIndex(i)))
			else:
				columns.append(None)
		updated_record = Record(buffered_record.get_value().metadata, True, *columns)
		# buffered_record.unpin()
		return updated_record

		# read_res = table.file_handler.read_projected_cols_of_page(page_directory_entry.page_id, proj_data_cols, proj_metadata_cols)

	# ONLY THIS FUNCTION reads from disk
	def bring_from_disk(self, table: Table, page_id: BaseTailPageID, proj_data_cols: List[Literal[0, 1]] | None = None, save: List[BufferpoolIndex] = []) -> bool:
		if proj_data_cols is None:
			proj_data_cols = [1] * table.num_columns # type: ignore[assignment]

		assert proj_data_cols is not None

		num_slots = config.NUM_METADATA_COL # evict enough slots for metadata and data
		for c in proj_data_cols :
			if c == 1:
				num_slots += 1
		if num_slots == 0:
			return True # no slots is basically a no-op
		print(f"evicted {num_slots} slots ")
		evicted_buff_idx: List[BufferpoolIndex] | None = self.evict_n_slots(num_slots, save)
		if evicted_buff_idx is None:
			return False
		read_res = table.file_handler.read_projected_cols_of_page(page_id, proj_data_cols)
		assert read_res is not None
		metadata_physical_pages, data_physical_pages = read_res
		print(f"read {proj_data_cols} from {page_id} and got {data_physical_pages}")
		# assert t_ is not None
		all_physical_pages = metadata_physical_pages + data_physical_pages

		j = 0
		for i, physical_page_ in enumerate(all_physical_pages):
			if physical_page_ is not None:
				buff_idx = evicted_buff_idx[j]
				self[buff_idx] = BufferpoolEntry(0, physical_page_, False, page_id, RawIndex(i), table)
				print(f"set {buff_idx} to {page_id} and type {type(page_id)}")
				j += 1 # use up one slot
		return True


	# TODO remove
	def get_page(self, table: Table, page_id: BaseTailPageID, projected_columns_index: list[Literal[0] | Literal[1]]) -> BufferedPage | None: # type: ignore[return]
		requested_columns: list[DataIndex] = [DataIndex(i) for i, binary_item in enumerate(projected_columns_index) if binary_item == 1]
		found, data_buff_indices, metadata_buff_indices = self.is_page_in_bufferpool(table, page_id, projected_columns_index)
		assert len(data_buff_indices) == table.num_columns
		assert len(metadata_buff_indices) == config.NUM_METADATA_COL

		# None means not requested

		data_cols_to_get: List[DataIndex] = []
		metadata_cols_to_get: List[int] = [] # should be from 0 to config.NUM_METADATA_COL - 1

		# the main difference here is that a certain column MAY be requested, 
		# but all columns of metadata are requested.
		# as a reminder, -1 for a buff_idx here means not found, None means not requested.
		buff_idx_to_save: list[BufferpoolIndex] = [] # these are only temporarily "pinned" through the save array

		for j, buff_idx in enumerate(data_buff_indices):
			if buff_idx == -1 and j in requested_columns:
				data_cols_to_get.append(DataIndex(j))
			elif isinstance(buff_idx, BufferpoolIndex):
				buff_idx_to_save.append(buff_idx)

		for j, buff_idx in enumerate(metadata_buff_indices):
			if buff_idx == -1:
				metadata_cols_to_get.append(DataIndex(j))
			else:
				buff_idx_to_save.append(buff_idx) # type: ignore[arg-type]

		# NOTE: this is the new proj_cols, to get only whatever we don't have already
		proj_data_cols: List[Literal[0, 1]] = [0] * table.num_columns
		for i in range(len(data_cols_to_get)):
			proj_data_cols[data_cols_to_get[i]] = 1

		if not self.bring_from_disk(table, page_id, proj_data_cols, buff_idx_to_save):
			return None


		found, data_buff_indices, metadata_buff_indices = self.is_page_in_bufferpool(table, page_id, projected_columns_index)
		assert found, "record not found after bringing it into bufferpool"

		# filtering out the -1s should be the same as taking out the Nones
		# ie taking out all not requesteds should leave us with all columns
		filtered_data_buff_indices =  [idx for idx in data_buff_indices if (idx != -1) and (isinstance(idx, BufferpoolIndex)) ]
		filtered_metadata_buff_indices =  [idx for idx in metadata_buff_indices if (idx != -1) and (isinstance(idx, BufferpoolIndex)) ]
		relevant_data_buff_indices = [idx for idx in data_buff_indices if idx is not None]
		relevant_metadata_buff_indices = [idx for idx in metadata_buff_indices if idx is not None]
		assert len(filtered_data_buff_indices) == len(relevant_data_buff_indices)
		assert len(filtered_metadata_buff_indices) == len(relevant_metadata_buff_indices)

		for idx in data_buff_indices:
			assert (idx != -1 and isinstance(idx, BufferpoolIndex)) or idx is None # should be either None (not requested) or a BufferpoolIndex

		for idx in metadata_buff_indices:
			assert (idx != -1 and isinstance(idx, BufferpoolIndex)) # should all be BufferpoolIndex(s)

		return BufferedPage(self, table, data_buff_indices, metadata_buff_indices, projected_columns_index) # type: ignore[arg-type]



	# TODO: remove type ignore
	# TODO: get updated value with schema encoding (maybe not this function)
	# TODO: specialize for tail records to only put the non-null columns in bufferpool
	def get_record(self, table: Table, rid: RID, projected_columns_index: list[Literal[0] | Literal[1]]) -> BufferedRecord | None:
		page_res = self.get_page(table, Bufferpool.rid_to_page_id(table, rid), projected_columns_index)
		assert page_res is not None
		r = BufferedRecord(self, table, page_res.metadata_buff_indices, page_res.data_buff_indices, Bufferpool.rid_to_offset(table, rid), rid, projected_columns_index)
		return r
		

	# returns buffer index of evicted physical page. 
	# does not evict anything in the `save` array.
	def evict_physical_page_clock(self, save: list[BufferpoolIndex] = []) -> BufferpoolIndex | None: 
		start_hand = self.curr_clock_hand % config.BUFFERPOOL_SIZE
		def curr_hand() -> BufferpoolIndex:
			return BufferpoolIndex(self.curr_clock_hand % config.BUFFERPOOL_SIZE)
		self.curr_clock_hand += 1
		while curr_hand() != start_hand:
			i = curr_hand()
			if self.maybe_get_entry(i) is None:
				return i
			if self[i].pin_count == 0:
				if self[i].dirty_bit == True: 
					self.write_to_disk(self[i].table, i)
				self.remove_from_bufferpool(i) # remove from the buffer without writing in disk
				return i
			self.curr_clock_hand += 1
		return None

	# NOTE: just pin the indices you want to keep rather than populating the save array... 
	def evict_n_slots(self, n: int, save: list[BufferpoolIndex] = []) -> List[BufferpoolIndex] | None: # returns buffer indices freed, or None if not all slots could be evicted
		evicted_buff_idx: list[BufferpoolIndex] = []
		for _ in range(n):
			evicted = self.evict_physical_page_clock(save)
			if evicted is None:
				return None
			evicted_buff_idx.append(evicted)
		return evicted_buff_idx

	def remove_from_bufferpool(self,index: BufferpoolIndex) -> None:
		self[index] = None

	def write_to_disk(self, table: Table, index: BufferpoolIndex) -> None:
		page_id = self[index].physical_page_id
		physical_page_index = self[index].physical_page_index
		assert page_id is not None
		assert physical_page_index is not None
		physical_page = self[physical_page_index].physical_page
		assert physical_page is not None
		path = os.path.join(self.path, table.file_handler.page_id_to_path(page_id))
		with open(path, 'r+b') as file:
			file.seek(physical_page_index.toDataIndex() * config.PHYSICAL_PAGE_SIZE)
			if self[index].physical_page != None:
				file.write(physical_page.data)
