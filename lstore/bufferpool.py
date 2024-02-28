from __future__ import annotations
from collections import namedtuple
import pickle
import struct
import time
from types import FunctionType
import typing
import glob
import os

# from lstore import file_handler
from lstore.index import Index
from lstore.page_directory_entry import BaseMetadataPageID, BasePageID, BaseRID, MetadataPageID, PageDirectoryEntry, PageID, TailMetadataPageID, TailPageID, TailRID
from lstore.ColumnIndex import DataIndex, RawIndex
# from lstore.base_tail_page import BasePage
from lstore.config import FullMetadata, WriteSpecifiedMetadata, config
from lstore.helper import helper
from lstore.record_physical_page import PhysicalPage, Record
from typing import Any, List, Literal, Tuple, Type, TypeAlias, TypeGuard, TypeVar, Generic, Annotated, cast

TPageType = Literal["base", "tail", "base_metadata", "tail_metadata"]
class BufferpoolIndex(int):
	pass
class BufferpoolSearchResult:
	TNOT_FOUND = Literal[-1]
	def __init__(self, found: bool, data_buff_indices: List[None | TNOT_FOUND | BufferpoolIndex], metadata_buff_indices: List[TNOT_FOUND | BufferpoolIndex], record_offset: int | None):
		self.found = found
		self.data_buff_indices = data_buff_indices
		self.metadata_buff_indices = metadata_buff_indices
		self.record_offset = record_offset

# this result is returned when getting only partial records
class FilePageReadResult:
	def __init__(self, metadata_physical_pages: list[PhysicalPage | None], data_physical_pages: list[PhysicalPage | None], page_type: str): # should be only "base" or "tail" page type
		self.metadata_physical_pages = metadata_physical_pages
		self.data_physical_pages = data_physical_pages
		self.page_type = page_type

# this result is returned when getting a full record.
class FullFilePageReadResult:
	def __init__(self, metadata_physical_pages: list[PhysicalPage], data_physical_pages: list[PhysicalPage], page_type: str): # should be only "base" or "tail" page type
		self.metadata_physical_pages = metadata_physical_pages
		self.data_physical_pages = data_physical_pages
		self.page_type = page_type

class BufferpoolEntry:
	def __init__(self, pin_count: int, physical_page: PhysicalPage, dirty_bit: bool, physical_page_id: PageID, physical_page_index: RawIndex, page_type: str, table: Table):
		# if pin_count != None: # None for pin_count will not change pin_count
		self.pin_count = pin_count 
		self.physical_page = physical_page
		self.dirty_bit = dirty_bit
		self.physical_page_id = physical_page_id
		self.physical_page_index = physical_page_index
		self.table = table

class PsuedoBuffIntValue:
	def __init__(self, file_handler: FileHandler, page_sub_path: PageID | Literal["catalog"], byte_position: int) -> None:
		self.flushed = False
		self.dirty = False
		self.page_sub_path = page_sub_path
		self.page_paths = [file_handler.page_path(page_sub_path)]
		self.file_handler = file_handler
		self.byte_positions = [byte_position]
		self._value = file_handler.read_int_value(page_sub_path, byte_position)
		#print(f"INITIING TO {self._value} and {self.page_paths}")
		# self._value = file_handler.read_value(page_sub_path, byte_position, "int")
	def flush(self) -> None:
		if self.dirty:
			for i in range(len(self.page_paths)):
				self.file_handler.write_position(self.page_paths[i], self.byte_positions[i], self._value)
		self.flushed = True
		#print(f"FLUSHED {self._value} in {self.page_paths}")
	def value(self, increment: int=0) -> int:
		val = self._value
		if self.flushed:
			raise(Exception("PseudoBuffInt*Value objects can only be flushed once; value() was called after flushing"))
		if increment != 0:
			self._value += increment 
			#print("incremented thing")
			self.dirty = True
		#print(f"value {val} accessed with increment {increment}, with page_paths = {self.page_paths} and offset = {self.byte_positions}")
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
			# #print(self.page_path, self._value, self.byte_position)
			#print(f"ERROR IN  {self._value} in {self.page_paths}")
			raise(Exception("unflushed int buffer value"))
		# self.flush()

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
	# will flush the value to memory to THIS location also. 

class PBTailPageID(PsuedoBuffIntValue):
	def __init__(self, file_handler: FileHandler, page_sub_path: PageID | Literal["catalog"], byte_position: int) -> None:
		super().__init__(file_handler, page_sub_path, byte_position)
	def value(self, increment: int = 0) -> TailPageID:
		# super().value()e
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
		# super().value()
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
		# super().value()
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
		# super().value()
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
		# super().value()
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
		# self._value = file_handler.read_value(page_sub_path, byte_position, "int")
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
			#print("I AM A PROBLEMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMM " , self.value_get())
			raise(Exception("unflushed dict buffer value "))

class FileHandler:
	def __init__(self, table: Table) -> None: # path is the database-level path
		# self.last_base_page_id = self.get_last_base_page_id()

		# NOTE: these next_*_id variables represent the *next* id to be written, not necessarily the last one. the last 
		# written id is the next_*_id variable minus 1
		self.table = table
		self.table_path = os.path.join(table.db_path, self.table.name)
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
		self.next_tail_rid = PBTailRID(self, "catalog", config.byte_position.catalog.LAST_TAIL_ID)
		# TODO: populate the offset byte with 0 when creating a new page
		self.base_offset = PsuedoBuffIntValue(self, BasePageID(self.next_base_page_id.value() - 1), config.byte_position.base_tail.OFFSET) # the current offset is based on the last written page
		self.base_offset.add_flush_location(BaseMetadataPageID(self.next_base_metadata_page_id.value() - 1), config.byte_position.metadata.OFFSET) # flush the offset in the corresponding metadata file along with the base file
		self.tail_offset = PsuedoBuffIntValue(self, TailPageID(self.next_tail_page_id.value() - 1), config.byte_position.base_tail.OFFSET) # the current offset is based on the last written page
		# t_base = self.read_projected_cols_of_page(BasePageID(self.next_base_page_id.value() - 1)) # could be empty PhysicalPages, to start. but the page files should still exist, even when they are empty
		# if t_base is None:
		# 	raise(Exception("the base_page_id just before the next_page_id must have a folder."))
		# t_tail = self.read_projected_cols_of_page(TailPageID(self.next_base_page_id.value() - 1))
		# if t_tail is None:
		# 	raise(Exception("the tail_page_id just before the next_page_id must have a folder."))
		base_page = self.read_full_page(BasePageID(self.next_base_page_id.value() - 1))
		tail_page = self.read_full_page(TailPageID(self.next_tail_page_id.value() - 1))
		
		self.base_page_to_commit: Annotated[list[PhysicalPage], self.table.total_columns] = base_page.metadata_physical_pages + base_page.data_physical_pages
		self.tail_page_to_commit: Annotated[list[PhysicalPage], self.table.total_columns] = tail_page.metadata_physical_pages + tail_page.data_physical_pages
		#print("SEET table_path to" + self.table_path)
		# check that physical page sizes and offsets are the same
		# assert len(
		# 	set(map(lambda physicalPage: physicalPage.size, self.page_to_commit))) <= 1
		# assert len(
		# 	set(map(lambda physicalPage: physicalPage.offset, self.page_to_commit))) <= 1
	def base_path(self, base_page_id: BasePageID) -> str:
		return os.path.join(self.table_path, f"base_{base_page_id}")
		# return os.path.join(config.PATH, self.table.name, f"base_{base_page_id}")
	def tail_path(self, tail_page_id: TailPageID) -> str:
		return os.path.join(self.table_path, f"tail_{tail_page_id}")
		# return os.path.join(config.PATH, self.table.name, f"tail_{tail_page_id}")
	def metadata_path(self, metadata_page_id: BaseMetadataPageID | TailMetadataPageID) -> str:
		# assert isinstance(metadata_page_id, BaseMetadataPageID) or isinstance(metadata_page_id, TailMetadataPageID)
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

	# @staticmethod
	# def table_file_path_static(self, file_name: Literal["catalog", "page_directory", "indices"], table_name: str) -> str:
	# 	path = os.path.join(os.path.join(), file_name)
	# 	if file_name == "page_directory" or file_name == "indices":
	# 		path += ".pickle" # these files have the .pickle extension
	# 	return path


	# def get_last_ids(self) -> tuple[int, int, int]: # writing boolean specifies whether this id will be written to by the user.
	# 	with open(self.catalog_path, "r") as file:
	# 		return (int(file.read(8)), int(file.read(8)), int(file.read(8)))

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
			# file.write(struct.pack(config.PACKING_FORMAT_STR, value))
			file.write(value.to_bytes(config.BYTES_PER_INT, byteorder="big"))
		file.close()
		return True


	def write_new_tail_page(self) -> bool:
			# check that physical page sizes and offsets are the same
		assert len(
			set(map(lambda physicalPage: physicalPage.size, self.tail_page_to_commit))) <= 1
		assert len(
			set(map(lambda physicalPage: physicalPage.offset, self.tail_page_to_commit))) <= 1
		
		# STEP 1. get offset for this page. increment offset to the end
		offset_to_write = self.tail_page_to_commit[0].offset # amount of bytes to write
		curr_offset = self.tail_offset.value()
		# if we can fit all of the remaining physical page data into this page, then only increment
		# by that value. otherwise, set the offset value to config.PHYSICAL_PAGE_SIZE
		# self.base_offset.value(offset_to_write if curr_offset + offset_to_write <= config.PHYSICAL_PAGE_SIZE else config.PHYSICAL_PAGE_SIZE - curr_offset)
		# old_metadata_pointer_buff = PseudoBuffIntTypeValue[MetadataPageID](self, )
		self.tail_offset.flush() # flush the old offset (this is ahead of what we will write, but it's okay)


		# STEP 2. write whatever we can into this page, both metadata and tail
		p_metadata_1 = self.metadata_path(TailMetadataPageID(self.next_tail_metadata_page_id.value() - 1))
		offset_written = config.PHYSICAL_PAGE_SIZE - curr_offset
		with open(p_metadata_1, "r+b") as old_metadata_file:
			old_metadata_file.seek(8)
			for i in range(0, config.NUM_METADATA_COL):
				old_metadata_file.seek(curr_offset, 1) # skip over already written stuff
				old_metadata_file.write(self.base_page_to_commit[i][0:offset_written]) 
		old_metadata_file.close()

		p_tail_1 = self.tail_path(TailPageID(self.next_tail_page_id.value() - 1))
		with open(p_tail_1, "r+b") as old_tail_file:
			old_tail_file.seek(24)
			for i in range(config.NUM_METADATA_COL, self.table.total_columns):
				old_tail_file.seek(curr_offset, 1) # skip over already written stuff
				old_tail_file.write(self.base_page_to_commit[i][0:offset_written])
		old_tail_file.close()


		# STEP 3. take the remaining and write it into the next page
		written_id = self.next_tail_page_id.value(1)
		written_tail_page_path = self.tail_path(written_id) 

		metadata_pointer = self.next_tail_metadata_page_id.value(1)
		self.initialize_metadata_file(metadata_pointer)
		self.initialize_base_tail_page(written_id, metadata_pointer)
		self.tail_offset = PsuedoBuffIntValue(self, written_id, config.byte_position.base_tail.OFFSET)
		self.tail_offset.add_flush_location(metadata_pointer, config.byte_position.metadata.OFFSET)


		with open(self.metadata_path(metadata_pointer), "r+b") as new_metadata_file: # open new metadata file
			new_metadata_file.seek(8)
			for i in range(0, config.NUM_METADATA_COL):
				# the order is swapped because we are adding a new page rather than adding to a page
				new_metadata_file.write(self.tail_page_to_commit[i][offset_written:]) # config.PHYSICAL_PAGE_SIZE - offset_written
				new_metadata_file.seek(offset_written, 1) 
		new_metadata_file.close()

		with open(written_tail_page_path, "r+b") as file: # open new page file
			file.seek(24)
			for i in range(config.NUM_METADATA_COL, len(self.tail_page_to_commit)): # write the data columns
				file.write(self.tail_page_to_commit[i][offset_written:])
				file.seek(offset_written, 1)
		file.close()

		self.tail_page_to_commit = [PhysicalPage()] * self.table.total_columns
		return True


	# should only be "base" or "tail" path_type
	# 
	def write_new_base_page(self) -> bool: # the page MUST be full in order to write. returns true if success

		# check that physical page sizes and offsets are the same
		assert len(
			set(map(lambda physicalPage: physicalPage.size, self.base_page_to_commit))) <= 1
		assert len(
			set(map(lambda physicalPage: physicalPage.offset, self.base_page_to_commit))) <= 1
		
		# STEP 1. get offset for this page. increment offset to the end
		offset_to_write = self.base_page_to_commit[0].offset # amount of bytes to write
		curr_offset = self.base_offset.value()
		# if we can fit all of the remaining physical page data into this page, then only increment
		# by that value. otherwise, set the offset value to config.PHYSICAL_PAGE_SIZE
		# self.base_offset.value(offset_to_write if curr_offset + offset_to_write <= config.PHYSICAL_PAGE_SIZE else config.PHYSICAL_PAGE_SIZE - curr_offset)
		# old_metadata_pointer_buff = PseudoBuffIntTypeValue[MetadataPageID](self, )
		self.base_offset.flush() # flush the old offset (this is ahead of what we will write, but it's okay)


		# STEP 2. write whatever we can into this page, both metadata and base
		p_metadata_1 = self.metadata_path(BaseMetadataPageID(self.next_base_metadata_page_id.value() - 1))
		offset_skip = config.PHYSICAL_PAGE_SIZE - curr_offset
		with open(p_metadata_1, "r+b") as old_metadata_file:
			old_metadata_file.seek(8)
			for i in range(0, config.NUM_METADATA_COL):
				old_metadata_file.write(self.base_page_to_commit[i][0:curr_offset]) 
				old_metadata_file.seek(offset_skip, 1) # skip over already written stuff
		old_metadata_file.close()

		p_base_1 = self.base_path(BasePageID(self.next_base_page_id.value() - 1))
		with open(p_base_1, "r+b") as old_base_file:
			old_base_file.seek(24)
			for i in range(config.NUM_METADATA_COL, self.table.total_columns):
				old_base_file.write(self.base_page_to_commit[i][0:curr_offset])
				old_base_file.seek(offset_skip, 1) # skip over already written stuff
		old_base_file.close()


		# STEP 3. take the remaining and write it into the next page
		written_id = self.next_base_page_id.value(1)
		written_base_page_path = self.base_path(written_id) 

		metadata_pointer = self.next_base_metadata_page_id.value(1)
		self.initialize_metadata_file(metadata_pointer)
		self.initialize_base_tail_page(written_id, metadata_pointer)
		self.base_offset = PsuedoBuffIntValue(self, written_id, config.byte_position.base_tail.OFFSET)
		self.base_offset.add_flush_location(metadata_pointer, config.byte_position.metadata.OFFSET)


		# with open(self.metadata_path(metadata_pointer), "r+b") as new_metadata_file: # open new metadata file
		# 	new_metadata_file.seek(8)
		# 	for i in range(0, config.NUM_METADATA_COL):
		# 		# the order is swapped because we are adding a new page rather than adding to a page
		# 		new_metadata_file.seek(curr_offset, 1) 
		# 		new_metadata_file.write(self.base_page_to_commit[i][curr_offset:]) # config.PHYSICAL_PAGE_SIZE - offset_written
		# new_metadata_file.close()

		# with open(written_base_page_path, "r+b") as file: # open new page file
		# 	file.seek(24)
		# 	for i in range(config.NUM_METADATA_COL, len(self.base_page_to_commit)): # write the data columns
		# 		file.seek(offset_written, 1)
		# 		file.write(self.base_page_to_commit[i][offset_written:])
		# file.close()

		self.base_page_to_commit = []
		for i in range(self.table.total_columns):
			self.base_page_to_commit.append(PhysicalPage(data=bytearray(config.PHYSICAL_PAGE_SIZE), offset=0))
		self.base_offset.value_assign(0)
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
	# the [1] default value is just so that I can overwrite it later with the proper default value; 
	# in other words it is just a placeholder
	# returns None for every column not in projected_columns_index
	def read_projected_cols_of_page(self, page_id: PageID, projected_columns_index: list[Literal[0, 1]] | None = None, projected_metadata_columns_index: list[Literal[0, 1]] | None = None) -> FilePageReadResult | None: 
		projected_columns_index = [1] * self.table.num_columns if projected_columns_index is None else projected_columns_index  # type: ignore
		projected_metadata_columns_index = [1] * config.NUM_METADATA_COL if projected_metadata_columns_index is None else projected_metadata_columns_index # type: ignore
		assert projected_columns_index is not None
		assert projected_metadata_columns_index is not None
		physical_pages: list[PhysicalPage | None] = [None] * self.table.num_columns
		metadata_pages: list[PhysicalPage | None] = [None] * config.NUM_METADATA_COL

		metadata_buff = PBBaseMetadataPageID(self, page_id, config.byte_position.base_tail.METADATA_PTR)
		metadata_path = self.metadata_path(metadata_buff.value())
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
				if projected_columns_index[i] == 1:
					physical_pages[i] = PhysicalPage(data=bytearray(file.read(config.PHYSICAL_PAGE_SIZE)), offset=offset)
					# physical_pages.append(PhysicalPage(data=bytearray(file.read(config.PHYSICAL_PAGE_SIZE)), offset=offset))
				else:
					# physical_pages.append(None)
					file.seek(config.PHYSICAL_PAGE_SIZE, 1) # seek 4096 (or size) bytes forward from current position (the 1 means "from current position")
		file.close()
		page_type: Literal["base", "tail"] = "base"
		if isinstance(page_id, TailPageID):
			page_type = "tail"
		elif not isinstance(page_id, BasePageID):
			raise(Exception("unexpected page_id type that wasn't base or tail page?"))
		return FilePageReadResult(metadata_pages, physical_pages, page_type)

	def read_full_page(self, page_id: PageID) -> FullFilePageReadResult:
		res = self.read_projected_cols_of_page(page_id)
		assert res is not None
		assert len(res.data_physical_pages) == self.table.num_columns
		assert len(res.metadata_physical_pages) == config.NUM_METADATA_COL
		filtered_data = [physical_page for physical_page in res.data_physical_pages if physical_page is not None]
		filtered_metadata = [physical_page for physical_page in res.metadata_physical_pages if physical_page is not None]
		assert len(filtered_data) == self.table.num_columns
		assert len(filtered_metadata) == config.NUM_METADATA_COL
		page_type: Literal["base", "tail"] = "base"
		if isinstance(page_id, TailPageID):
			page_type = "tail"
		elif not isinstance(page_id, BasePageID):
			raise(Exception("unexpected page_id type that wasn't base or tail page?"))
		return FullFilePageReadResult(filtered_metadata, filtered_data, page_type)


	
	def insert_tail_record(self, metadata: WriteSpecifiedMetadata, *columns:int | None) -> int:
		total_cols = self.table.total_columns

		list_columns: list[int | None] = list(columns)
		rid = self.next_tail_rid.value(1)
		list_columns.insert(config.INDIRECTION_COLUMN, metadata.indirection_column)
		list_columns.insert(config.RID_COLUMN, rid)
		list_columns.insert(config.TIMESTAMP_COLUMN, int(time.time()))
		list_columns.insert(config.SCHEMA_ENCODING_COLUMN, metadata.schema_encoding)
		list_columns.insert(config.NULL_COLUMN, metadata.null_column)
		list_columns.insert(config.BASE_RID, rid)
		cols = tuple(list_columns)
		for i in range(len(self.tail_page_to_commit)):
			physical_page = self.tail_page_to_commit[i]	
			if physical_page is not None:
				if not physical_page.has_capacity():
					self.write_new_tail_page()
				else:
					physical_page.insert(cols[i])
			self.tail_offset.value(config.BYTES_PER_INT)
		pg_dir_entry: 'PageDirectoryEntry'
		pg_dir_entry = PageDirectoryEntry(TailPageID(self.next_tail_page_id.value()), TailMetadataPageID(self.next_tail_metadata_page_id.value()), self.tail_offset.value(), "tail")
		self.table.page_directory_buff.value_assign(rid, pg_dir_entry)
		
		return rid
	



	def insert_base_record(self, metadata: WriteSpecifiedMetadata, *columns: int | None) -> int: # returns RID of inserted record
		assert len(
			set(map(lambda physicalPage: physicalPage.size, self.base_page_to_commit))) <= 1
		assert len(
			set(map(lambda physicalPage: physicalPage.offset, self.base_page_to_commit))) <= 1
		null_bitmask = 0
		total_cols = self.table.total_columns
		if metadata.indirection_column == None: # set 1 for null indirection column
			# #print("setting indirection null bit")
			null_bitmask = helper.ith_total_col_shift(total_cols, config.INDIRECTION_COLUMN)
			# null_bitmask = 1 << (total_cols - 1)
		for idx, column in enumerate(columns):
			# #print(f"checking cols for null... {column}")
			if column is None:
				# #print("found a null col")
				null_bitmask = null_bitmask | helper.ith_total_col_shift(len(columns), idx, False) #
				# null_bitmask = null_bitmask | (1 << (len(columns)-idx-1))
			
		# #print(f"inserting null bitmask {bin(null_bitmask)}")
		
		# Transform columns to a list to append the schema encoding and the indirection column
		# #print(columns)
		list_columns: list[int | None] = list(columns)
		rid = self.next_base_rid.value(1)
		list_columns.insert(config.INDIRECTION_COLUMN, metadata.indirection_column)
		list_columns.insert(config.RID_COLUMN, rid)
		list_columns.insert(config.TIMESTAMP_COLUMN, int(time.time()))
		list_columns.insert(config.SCHEMA_ENCODING_COLUMN, metadata.schema_encoding)
		list_columns.insert(config.NULL_COLUMN, null_bitmask)
		list_columns.insert(config.BASE_RID, rid)
		cols = tuple(list_columns)
		for i in range(len(self.base_page_to_commit)):
			physical_page = self.base_page_to_commit[i]	
			if physical_page is not None:
				physical_page.insert(cols[i])
		#print(f"before: {self.base_offset.value()}")
		self.base_offset.value(config.BYTES_PER_INT)
		#print(f"after: {self.base_offset.value()}")
		#print(f"physical_page_offset = {physical_page.offset} ..--.. base_offset = {self.base_offset.value()}")
		assert self.base_page_to_commit[0].offset == self.base_offset.value(), f"physical_page_offset = {physical_page.offset} but base_offset = {self.base_offset.value()}"
		if self.base_offset.value() == config.PHYSICAL_PAGE_SIZE:
			self.write_new_base_page()	
		pg_dir_entry = PageDirectoryEntry(BasePageID(self.next_base_page_id.value()), BaseMetadataPageID(self.next_base_metadata_page_id.value()), self.base_offset.value(), "base")
		self.table.page_directory_buff.value_assign(rid, pg_dir_entry)
		return rid

	def initialize_table_files(self) -> None:
		# initialize catalog file
		catalog_path = self.table_file_path("catalog")
		open(catalog_path, "xb")
		with open(catalog_path, "w+b") as catalog_file:
			helper.write_int(catalog_file, self.table.num_columns)
			helper.write_int(catalog_file, self.table.key_index)
			# initialze IDs (page ids, rid)
			helper.write_int(catalog_file, 2)
			helper.write_int(catalog_file, 2)
			helper.write_int(catalog_file, 2)
			helper.write_int(catalog_file, 2)
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
		self.write_new_base_page()
		self.next_base_page_id.flush()
		self.next_tail_page_id.flush()
		self.next_base_metadata_page_id.flush()
		self.next_tail_metadata_page_id.flush()
		self.next_base_rid.flush()
		self.next_tail_rid.flush()
		self.base_offset.flush()
		self.tail_offset.flush()

	def __del__(self) -> None:
		pass
		# self.flush()
		#print(f"DELETING FILE HANDLER PATH {self.table_path}")


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




	# @property
	# def page_range_str(self) -> str:
	#     return helper.str_each_el(self.page_ranges)

	# @property
	# def page_directory_str(self) -> str:
	#     return str({key: (value.page.high_level_str, value.offset) for (key, value) in
	#                 self.page_directory.items()})  # type: ignore[index]

#     def __str__(self) -> str:
#         return f"""{config.INDENT}TABLE: {self.name}
# {config.INDENT}key index: {self.key_index}
# {config.INDENT}num_columns: {self.num_columns}
# {config.INDENT}page_directory: {self.page_directory_str}
# {config.INDENT}page_ranges: {self.page_range_str}"""

	# {config.INDENT}page_directory_raw: {self.page_directory}
	# def get_record_by_rid(self, rid: int) -> Record:
	#     page_dir_entry = self.page_directory_buff[rid]
	#     return page_dir_entry.page_id.get_nth_record(
	#         page_dir_entry.offset)

	# def _update_record_by_id()
	# def __merge(self):
	#     #print("merge is happening")
	#     pass

	# TODO: uncomment

	
	def bring_base_pages_to_memory(self)-> None:
		#list_base_pages=[]
		for table_name in os.listdir(self.db_path):
			if self.name==table_name:
				table_path=os.path.join(self.db_path,table_name)
				catalog_path = os.path.join(self.db_path,"catalog")
				with open(catalog_path, 'rb') as catalog:
					#get the catalog to create the table 
					table_num_columns= int.from_bytes(catalog.read(8))
					table_key_index= int.from_bytes(catalog.read(8))
					table_pages_per_range = int.from_bytes(catalog.read(8))
					table_last_page_id = int.from_bytes(catalog.read(8))
					table_last_tail_id= int.from_bytes(catalog.read(8))
					table_last_rid= int.from_bytes(catalog.read(8))
				
				for file in os.listdir(table_path):
					if file =="*page*":
						page_id=int(file.split("_")[1]) # take the page id, may not work :( 
		#                 #page= BasePage(table_num_columns, DataIndex(table_key_index))
		#                 #page.id=page_id
		#                 page_path = os.path.join(table_path,page_id)
		#                 with open(page_path, "rb") as page_file:
		#                     metadata_id= int(page_file.read(8))
		#                     offset=  int(page_file.read(8))
		#                     page_range_id=int(page_file.read(8))
		#                     if page_range_id== self.page_range_id:
		#                         metadata_path=os.path.join(table_path,metadata_id)
		#                         with open(metadata_path,"rb") as metadata_file:
		#                             rid=metadata_file.read(offset)
		#                             timestamp=metadata_file.read(offset)
		#                             indirection_column=metadata_file.read(offset)
		#                             schema_encoding=metadata_file.read(offset)
		#                             null_column=metadata_file.read(offset)
		#                         list_physical_pages=[]
		#                         list_physical_pages.append(PhysicalPage(bytearray(rid), offset))
		#                         list_physical_pages.append(PhysicalPage(bytearray(timestamp), offset))
		#                         list_physical_pages.append(PhysicalPage(bytearray(indirection_column), offset))
		#                         list_physical_pages.append(PhysicalPage(bytearray(schema_encoding), offset))
		#                         list_physical_pages.append(PhysicalPage(bytearray(null_column), offset))
		#                         while True:
		#                             physical_page_information=page_file.read(offset)
									
		#                             if not physical_page_information:
		#                                 break
									
		#                             physical_page_data = bytearray(physical_page_information)
		#                             physical_page = PhysicalPage(physical_page_data,offset)
						
						file_page_read_result=self.file_handler.read_full_page(BasePageID(page_id))     

						data = file_page_read_result.data_physical_pages
						metadata = file_page_read_result.metadata_physical_pages
						file_result = FilePageReadResult(metadata, data, 'base')
						
						self.get_updated_base_page(file_result, BasePageID(page_id))

	
	def merge(self):
		list_base_page=self.bring_base_pages_to_memory()
		pass

	def get_updated_base_page(self,file_page_read_result: FilePageReadResult,page_id: BasePageID) -> None:
		object_to_get_tps=PsuedoBuffIntValue(self.file_handler, page_id, config.byte_position.base_tail.TPS)
		#tps=self.get_updated_base_page(file_page_read_result,object_to_get_tps.value())
		tps=object_to_get_tps.value()
		physical_pages=file_page_read_result.data_physical_pages
		metadata=file_page_read_result.metadata_physical_pages
		total_columns=len(physical_pages)+ len(metadata)
		if physical_pages[0] is not None:
			offset=physical_pages[0].offset 
		num_records=offset/8

		for i in range(int(num_records)):
			for j in range(len(physical_pages)): #iterate through all the columns of a record
				not_null_indirection_column=helper.not_null(metadata[config.INDIRECTION_COLUMN])
				indirection_column=not_null_indirection_column.data[8*i : 8*(i+1)]
				not_null_schema_encoding=helper.not_null(metadata[config.SCHEMA_ENCODING_COLUMN])
				schema_encoding=not_null_schema_encoding.data[8*i : 8*(i+1)]
				not_null_null_column=helper.not_null(metadata[config.NULL_COLUMN])
				null_column=not_null_null_column.data[8*i : 8*(i+1)]


				tail_indirection_column=indirection_column
				tail_schema_encoding=schema_encoding
				tail_physical_page=physical_pages
				tail_metadata_page=metadata
				tail_offset=i
				tail_null_column=null_column

				## check tps 
				if int.from_bytes(tail_indirection_column) < tps:
					break
				## check if deleted 
				if int.from_bytes(tail_null_column) & 1 << (total_columns-j)!=0: ## check this 
					break
			
				while int.from_bytes(tail_schema_encoding) & 1 << (total_columns-j)!=0: #column has been updated
				#loop for retreiving information not updated 
					tail_page_directory_entry = self.page_directory_buff[int.from_bytes(tail_indirection_column)]
					tail_offset=tail_page_directory_entry.offset
					tail_page_id = tail_page_directory_entry.page_id  ## tail page id 
					
					tail=self.file_handler.read_full_page(tail_page_id)
					
					tail_physical_page=tail.data_physical_pages
					tail_metadata_page=tail.metadata_physical_pages

					not_none_tail_indirection_column=helper.not_null(tail_metadata_page[config.INDIRECTION_COLUMN])
					tail_indirection_column=not_none_tail_indirection_column.data[8*tail_offset: 8*(tail_offset+1)]
					not_none_tail_schema_encoding=helper.not_null(tail_metadata_page[config.SCHEMA_ENCODING_COLUMN])
					tail_schema_encoding=not_none_tail_schema_encoding.data[8*tail_offset : 8*(tail_offset+1)]
					not_none_tail_null_column=helper.not_null(metadata[config.NULL_COLUMN])
					tail_null_column=not_none_tail_null_column.data[8*tail_offset : 8*(tail_offset+1)]
					
				not_null_physical_pages = helper.not_null(physical_pages[j])
				not_tail_physical_pages = helper.not_null(tail_physical_page[i])
				not_null_physical_pages.data[8*i : 8*(i+1)]=not_tail_physical_pages.data[tail_offset*8:(tail_offset+1)*8]
		#change schema encoding of the updated entry of the base page 
			please_give_us_an_A = helper.not_null(metadata[config.SCHEMA_ENCODING_COLUMN])
			please_give_us_an_A.data[i*8:8*(i+1)]=0
		
		final_physical_pages=metadata+physical_pages
		#create new base page file with the updated information
		self.file_handler.write_new_base_page()
		object_to_get_tps._value = int.from_bytes(indirection_column)
		object_to_get_tps.flush()




class BufferedRecord:
	def __del__(self) -> None:
		self.bufferpool.change_pin_count(self.buff_indices, -1)
	# def __getattribute__(self, attr: str): # type: ignore[no-untyped-def]
	# 	if attr == "_contents":
	# 		raise(Exception("do NOT get the contents directly"))
	# 	super().__getattribute__(attr)
	# value can actually have Any type here
	# def __setattr__(self, name: str, value) -> None: # type: ignore[no-untyped-def]
	# 	if name == "_contents" and self.initialized:
	# 		raise(Exception("buffered copies are read only"))
	# 	super().__setattr__(name, value)
	def __init__(self, bufferpool: Bufferpool, table: Table, buff_indices: List[BufferpoolIndex], record_offset: int, record_id: int, projected_columns_index: List[Literal[0, 1]]):
		# self.initialized = False
		self.bufferpool = bufferpool
		self.buff_indices = buff_indices # frame indices of base pages (including metadata and data)
		self.table = table
		self.record_offset = record_offset
		self.record_id = record_id
		self.projected_columns_index = projected_columns_index
		bufferpool.change_pin_count(buff_indices, +1) # increment pin counts of relevant bufferpool frames
		# self.initialized = True

	def add_buff_idx(self, buff_idx: BufferpoolIndex) -> None:
		self.buff_indices.append(buff_idx)
		self.bufferpool[buff_idx].pin_count += 1 # will be decremented later because it was addded to base_buff_indices

	def unpin_buff_indices(self, buff_indices: List[BufferpoolIndex]) -> None:
		for buff_idx in buff_indices:
			self.bufferpool[buff_idx].pin_count -= 1
			self.buff_indices.remove(buff_idx)

	def get_value(self) -> Record:
		metadata_cols: List[PhysicalPage] = []
		data_cols: List[PhysicalPage] = []
		for buff_idx in self.buff_indices:
			col_idx = self.bufferpool[buff_idx].physical_page_index
			assert col_idx is not None, "column held by BufferedRecord was None!"
			physical_page = self.bufferpool[buff_idx].physical_page
			assert physical_page is not None, "physical_page held by BufferedRecord was None!"
			if col_idx < config.NUM_METADATA_COL: # this is metadata column
				metadata_cols.append(physical_page)
			else:
				data_cols.append(physical_page)

		all_cols = metadata_cols + data_cols

		def get_no_none_check(col_idx: RawIndex, record_offset: int) -> int:
			return helper.unpack_data(all_cols[col_idx].data, record_offset)
		def get_check_for_none(col_idx: RawIndex, record_offset: int) -> int | None:
			# val = all_cols[col_idx][record_offset]
			# val = int.from_bytes(all_cols[col_idx].data[record_offset:record_offset+config.BYTES_PER_INT], byteorder="big")
			val = get_no_none_check(col_idx, record_offset)
			# # #print("getting checking null")
			if val == 0:
				# breaking an abstraction barrier for convenience right now. TODO: fix?
				thing = helper.unpack_data(all_cols[config.NULL_COLUMN].data, record_offset)
				# thing = helper.unpack_col(self, config.NULL_COLUMN, record_offset / config.BYTES_PER_INT)
				# thing = struct.unpack(config.PACKING_FORMAT_STR, self.physical_pages[config.NULL_COLUMN].data[(record_idx * 8):(record_idx * 8)+8])[0]
				# is_none = (self.physical_pages[config.NULL_COLUMN].data[(record_idx * 8):(record_idx * 8)+8] == b'x01')

				# is_none = ( thing >> ( self.num_columns + config.NUM_METADATA_COL - col_idx - 1 ) ) & 1
				is_none = helper.ith_bit(thing, self.table.total_columns, col_idx)
				if is_none == 1:
					return None
			return val
			

		indirection_column, rid, schema_encoding, timestamp, key_col, null_col, base_rid = \
			get_check_for_none(config.INDIRECTION_COLUMN, self.record_offset), \
			get_check_for_none(config.RID_COLUMN, self.record_offset), \
			get_no_none_check(config.SCHEMA_ENCODING_COLUMN, self.record_offset), \
			get_no_none_check(config.TIMESTAMP_COLUMN, self.record_offset), \
			get_check_for_none(helper.data_to_raw_idx(self.table.key_index), self.record_offset), \
			get_check_for_none(config.NULL_COLUMN, self.record_offset), \
			get_no_none_check(config.BASE_RID, self.record_offset), \

		# this is just outside the metadata column range. thus it should reveal the actual page type of base or tail (both base and tail pages will have "metadata" type pages in the bufferpool)
		# page_type = self.bufferpool[self.buff_indices[config.NUM_METADATA_COL]].page_type
		page_type = self.bufferpool.get_page_type(self.buff_indices[config.NUM_METADATA_COL])
		is_base_page = False
		if page_type == "base":
			is_base_page = True
		elif page_type == "metadata":
			raise(Exception("should not be getting metadata page type in get_record."))

		return Record(FullMetadata(rid, timestamp, indirection_column,schema_encoding, null_col, base_rid), is_base_page, *[helper.unpack_data(data_col.data, self.record_offset) for data_col in data_cols])
				

# class BufferpoolList(list):
# 	def __getitem__(self, key: BufferpoolIndex):
# 		super().__getitem__(key)

class Bufferpool:
	# buffered_physical_pages: list[Buffered[PhysicalPage]] = []
	# page_to_commit: typing.Annotated[list[PhysicalPage], self.num_raw_columns] = []

	# buffered_physical_pages: dict[int, Buffered[PhysicalPage]] = {}
	TProjected_Columns = List[Literal[0, 1]]
	def __init__(self, path: str) -> None: 
		# self.buffered_metadata: Annotated[list[list[PhysicalPage], config.BUFFERPOOL_SIZE]]  = [[]]
		# self.buffered_metadata: Annotated[list[list[PhysicalPage]], config.BUFFERPOOL_SIZE] = [[]]

		# self.file_handlers = {table.name: FileHandler(table) for table in self.tables} # create FileHandlers for each table
		# self.table_for_physical_pages: Annotated[list[str | None], config.BUFFERPOOL_SIZE] = [None] * config.BUFFERPOOL_SIZE

		# self.pin_counts: Annotated[list[int], config.BUFFERPOOL_SIZE] = [0] * config.BUFFERPOOL_SIZE
		# self.buffered_physical_pages: Annotated[list[PhysicalPage | None], config.BUFFERPOOL_SIZE] = [None] * config.BUFFERPOOL_SIZE
		# self.dirty_bits: Annotated[list[bool], config.BUFFERPOOL_SIZE] = [False] * config.BUFFERPOOL_SIZE
		# self.ids_of_physical_pages : Annotated[list[PageID | None], config.BUFFERPOOL_SIZE] = [None] * config.BUFFERPOOL_SIZE # the id of the PAGE that the physical page belongs to
		# self.index_of_physical_page_in_the_page : list[RawIndex | None] = [None] * config.BUFFERPOOL_SIZE # This tells you if its the first, second, third, etc physical page of the page
		# self.page_types: Annotated[list[str | None], config.BUFFERPOOL_SIZE] = [None] * config.BUFFERPOOL_SIZE
		# self.tables: Annotated[List[Table | None], config.BUFFERPOOL_SIZE] = [None] * config.BUFFERPOOL_SIZE

		# self.client_records: List[BufferedRecord] = [] # a list of all BufferedRecords provided by this class to clients

		self.entries: Annotated[List[BufferpoolEntry | None], config.BUFFERPOOL_SIZE] = [None] * config.BUFFERPOOL_SIZE
		self.path = path
		self.curr_clock_hand = 0
	
	def get_item(self, key: int) -> BufferpoolEntry | None:
		entry = self.entries[key]
		return entry


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
			self[idx].pin_count += change

	
	
	def insert_tail_record(self, table: Table, metadata: WriteSpecifiedMetadata, *columns: int | None) -> int: # Returns the RID of the inserted record
		return table.file_handler.insert_tail_record(metadata, *columns)
	

	def insert_base_record(self, table: Table, record_type: Literal["base", "tail"], metadata: WriteSpecifiedMetadata, *columns: int) -> int: # returns RID of inserted record
		# table_list = list(filter(lambda table: table.name == table_name, self.tables))
		# assert len(table_list) == 1
		# table = table_list[0]
		return table.file_handler.insert_base_record(metadata, *columns)

	# returns whether the requested portion of record is in the bufferpool, 
	# and the bufferpool indices of any column of the record found (regardless of whether it was)
	# fully found or not
	def is_record_in_bufferpool(self, table: Table, rid : int, projected_columns_index: list[Literal[0] | Literal[1]]) -> BufferpoolSearchResult: ## this will be called inside the bufferpool functions
		# table: 'Table' = next(table for table in self.tables if table.name == table_name)
		requested_columns: list[DataIndex] = [DataIndex(i) for i, binary_item in enumerate(projected_columns_index) if binary_item == 1]
		
		# a = table.page_directory_buff.value_get()
		# for key in a.keys():
		# 	if key == record_id:
		# 		page_directory_entry = table.page_directory_buff[key]
		# 		break 
		page_dir = table.page_directory_buff.value_get()
		if not rid in page_dir:
			return BufferpoolSearchResult(False, [], [], None)
		page_directory_entry = table.page_directory_buff[rid]

		record_page_id=page_directory_entry.page_id

		number_of_columns = sum(projected_columns_index)
		num_of_columns_found = 0
		# the ith element of this array is:
			# None, if not requested;
			# -1, if requested but not found;
			# the ith data column, if found and requested.
		data_buff_indices: list[None | BufferpoolSearchResult.TNOT_FOUND | BufferpoolIndex] = [None] * table.num_columns
		for col in requested_columns:
			data_buff_indices[col] = -1
		# after this point, data_columns_found array is NOT_FOUND if requested and None if not requested.

		metadata_buff_indices: List[BufferpoolIndex | BufferpoolSearchResult.TNOT_FOUND] = [-1] * config.NUM_METADATA_COL
		#column_list = [None] * len(projected_columns_index)

		for i in [BufferpoolIndex(_) for _ in range(config.BUFFERPOOL_SIZE)]: # search the entire bufferpool for columns
			if self.maybe_get_entry(i) is None:
				continue
			if self[i].physical_page_id == record_page_id:
				raw_idx = self[i].physical_page_index
				assert raw_idx is not None, "non None in ids_of_physical_pages but None in index_of_physical_page_in_page?"
				data_idx = raw_idx.toDataIndex()
				if data_idx in requested_columns: # is a data column we requested?
					#column_list[i] = self.buffered_physical_pages[i].data
					# num_of_columns_found += 1
					# data_buff_indices.append(i)
					# raw_idx = self[i].physical_page_index
					# assert raw_idx is not None
					# data_idx = raw_idx.toDataIndex()
					data_buff_indices[data_idx] = i
				elif raw_idx in range(config.NUM_METADATA_COL):
					metadata_buff_indices[data_idx] = i
				else: 
					continue

		found = (len([True for idx in data_buff_indices if idx == -1]) == 0) and (len([True for idx in metadata_buff_indices if idx == -1]) == 0)
		return BufferpoolSearchResult(found, data_buff_indices, metadata_buff_indices, page_directory_entry.offset)


	def bring_from_disk(self, table: Table, record_id: int, projected_columns_index: list[Literal[0] | Literal[1]], record_type: Literal["base", "tail"]) -> bool: # returns true if success
		# https://stackoverflow.com/questions/2361426/get-the-first-item-from-an-iterable-that-matches-a-condition
		# table: 'Table' = next(table for table in self.tables if table.name == table_name)
		requested_columns: list[int] = [i for i, binary_item in enumerate(projected_columns_index) if binary_item == 1]

		t = self.is_record_in_bufferpool(table, record_id, projected_columns_index)
		found, data_buff_indices, metadata_buff_indices = t.found, t.data_buff_indices, t.metadata_buff_indices
		
		# if not found:
			# find all indices not found. evict that many slots, and save the indices. 
			# also save the data and metadata indices that need retreival
			# retrieve those and put them in bufferpool
		for idx in data_buff_indices:
			if idx == -1:
				self.evict_physical_page_clock()


		if not record_id in table.page_directory_buff.value_get():
			return False	
		
		page_directory_entry = table.page_directory_buff[record_id]
		record_page_id = page_directory_entry.page_id
		
		#list_columns = []
		t_ = table.file_handler.read_projected_cols_of_page(record_page_id, projected_columns_index)
		if t_ is None:
			return False
		metadata_physical_pages, data_physical_pages = t_.metadata_physical_pages, t_.data_physical_pages
		all_physical_pages = metadata_physical_pages + data_physical_pages
		assert(len(all_physical_pages)) == table.total_columns, "read_page outputted the wrong length"
		# evict all slots necessary to hold the page and its metadata
		buff_indices = self.evict_n_slots(len([True for item in projected_columns_index if item == 1])) 
		if buff_indices is None:
			return False
		for i in [RawIndex(_) for _ in range(len(all_physical_pages))]:
			physical_page = all_physical_pages[i]
			if physical_page is not None:
				new_buff_idx = buff_indices[i]
				page_type = page_directory_entry.page_type
				page_id = page_directory_entry.page_id
				bufferpool_page_type = self.get_page_type(new_buff_idx)

					
				self.change_bufferpool_entry(BufferpoolEntry(0, physical_page, False, Bufferpool.make_page_id(page_id, bufferpool_page_type), i, page_type, table), new_buff_idx)
		return True
			# self.pin_counts[new_buff_idx] = 0 # initialize to
	

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
		return rid_column_marked & null_column_marked
				
	
	def update_nth_record(self, page_id : PageID, offset: int, col_idx: int, new_value: int) -> bool:
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
			return None # deleted record.
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
			curr_schema_encoding = curr_record.get_value().metadata.schema_encoding
			while helper.ith_bit(curr_schema_encoding, table.num_columns, col_idx, False) == 0b0: # while not found
				assert curr_record is not None, "a record with a non-None RID was not found"
				curr_rid = curr_record.get_value().metadata.indirection_column
				assert curr_rid is not None
				curr_record = self.get_record(table, curr_rid, proj_col)
				assert curr_record is not None
				curr_schema_encoding = curr_record.get_value().metadata.schema_encoding
			desired_col = curr_record.get_value()[col_idx]
		return desired_col 
	
	def get_version_col(self, table: Table, record: Record, col_idx: DataIndex, relative_version: int) -> int | None:
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
	def get_version_record(self, table: Table, record_id: int, projected_columns_index: list[Literal[0] | Literal[1]], relative_version: int) -> Record | None:
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
	def get_updated_record(self, table: Table, record_id: int, projected_columns_index: list[Literal[0] | Literal[1]]) -> Record | None:
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
		return updated_record


	# TODO: remove type ignore
	# TODO: get updated value with schema encoding (maybe not this function)
	# TODO: specialize for tail records to only put the non-null columns in bufferpool
	def get_record(self, table: Table, record_id: int, projected_columns_index: list[Literal[0] | Literal[1]]) -> BufferedRecord | None:
		# table: 'Table' = next(table for table in self.tables if table.name == table_name)
		requested_columns: list[DataIndex] = [DataIndex(i) for i, binary_item in enumerate(projected_columns_index) if binary_item == 1]
		t = self.is_record_in_bufferpool(table, record_id, projected_columns_index)
		found, data_buff_indices, metadata_buff_indices = t.found, t.data_buff_indices, t.metadata_buff_indices
		assert len(data_buff_indices) == table.num_columns
		assert len(metadata_buff_indices) == config.NUM_METADATA_COL

		# None means not requested
		data_physical_pages: List[PhysicalPage | None] = []
		metadata_physical_pages: List[PhysicalPage | None] = []

		data_cols_to_get: List[DataIndex] = []
		metadata_cols_to_get: List[int] = [] # should be from 0 to config.NUM_METADATA_COL - 1

		# the main difference here is that a certain column MAY be requested, 
		# but all columns of metadata are requested.
		# as a reminder, -1 for a buff_idx here means not found, None means not requested.
		for j, buff_idx in enumerate(data_buff_indices):
			if buff_idx == -1 and j in requested_columns:
				data_cols_to_get.append(DataIndex(j))
			elif buff_idx == None:
				data_physical_pages.append(None)
			elif isinstance(buff_idx, BufferpoolIndex):
				data_physical_pages.append(self[buff_idx].physical_page)
				self[buff_idx].pin_count += 1 
			else:
				raise(Exception("unexpected datatype for buff_idx inside the is_record_in_bufferpool output"))

		for j, buff_idx in enumerate(metadata_buff_indices):
			if buff_idx == -1:
				metadata_cols_to_get.append(DataIndex(j))
			else:
				physical_page = self[buff_idx].physical_page
				assert physical_page is not None
				metadata_physical_pages.append(physical_page)
				self[buff_idx].pin_count += 1 


		# a key line:
		# evict just enough to get the remaining columns, but don't evict what we already have.
		# "what we already have" has been pinned
		evicted_buff_idx: List[BufferpoolIndex] | None = self.evict_n_slots(len(metadata_cols_to_get + data_cols_to_get))
		if evicted_buff_idx is None:
			return None

		page_dir = table.page_directory_buff.value_get()
		if not record_id in page_dir:
			return None
		page_directory_entry = table.page_directory_buff[record_id]

		# NOTE: this is the new proj_cols, to get only whatever we don't have already
		proj_metadata_cols: List[Literal[0, 1]] = [0] * config.NUM_METADATA_COL 
		for i in range(len(metadata_cols_to_get)):
			proj_metadata_cols[metadata_cols_to_get[i]] = 1

		proj_data_cols: List[Literal[0, 1]] = [0] * config.NUM_METADATA_COL
		for i in range(len(data_cols_to_get)):
			proj_data_cols[data_cols_to_get[i]] = 1

		t_ = table.file_handler.read_projected_cols_of_page(page_directory_entry.page_id, proj_data_cols, proj_metadata_cols)
		assert t_ is not None
		metadata_physical_pages, data_physical_pages = t_.metadata_physical_pages, t_.data_physical_pages
		all_physical_pages = metadata_physical_pages + data_physical_pages

		for i, buff_idx in enumerate(evicted_buff_idx):
			physical_page_ = all_physical_pages[i]
			if physical_page_ is not None:
				self[buff_idx] = BufferpoolEntry(0, physical_page_, False, page_directory_entry.page_id, RawIndex(i), t_.page_type, table)



		t_2 = self.is_record_in_bufferpool(table, record_id, projected_columns_index)
		found, data_buff_indices, metadata_buff_indices, record_offset = t_2.found, t_2.data_buff_indices, t_2.metadata_buff_indices, t_2.record_offset
		assert found, "record not found after bringing it into bufferpool"
		assert record_offset is not None
		filtered_data_buff_indices =  [idx for idx in data_buff_indices if (idx != -1) and (isinstance(idx, BufferpoolIndex)) ]
		filtered_metadata_buff_indices =  [idx for idx in metadata_buff_indices if (idx != -1) and (isinstance(idx, BufferpoolIndex)) ]
		assert len(filtered_data_buff_indices) == len(data_buff_indices)
		assert len(filtered_metadata_buff_indices) == len(metadata_buff_indices)

		r = BufferedRecord(self, table, filtered_metadata_buff_indices + filtered_data_buff_indices, record_offset, record_id, projected_columns_index)
		return r
		

	# returns buffer index of evicted physical page. 
	# does not evict anything in the `save` array.
	def evict_physical_page_clock(self) -> BufferpoolIndex | None: 
		start_hand = self.curr_clock_hand % config.BUFFERPOOL_SIZE
		def curr_hand() -> BufferpoolIndex:
			return BufferpoolIndex(self.curr_clock_hand % config.BUFFERPOOL_SIZE)
		self.curr_clock_hand += 1
		while curr_hand() != start_hand:
			i = curr_hand()
			if self.maybe_get_entry(i) is None:
				return i
			if self[i].pin_count == 0:
				if self[i].dirty_bit == 1: 
					self.write_to_disk(self[i].table, i)
				self.remove_from_bufferpool(i) # remove from the buffer without writing in disk
				return i
			self.curr_clock_hand += 1
		return None

	# NOTE: just pin the indices you want to keep rather than populating the save array... 
	def evict_n_slots(self, n: int) -> List[BufferpoolIndex] | None: # returns buffer indices freed, or None if not all slots could be evicted
		evicted_buff_idx: list[BufferpoolIndex] = []
		for _ in range(n):
			evicted = self.evict_physical_page_clock()
			if evicted is None:
				return None
			evicted_buff_idx.append(evicted)
		return evicted_buff_idx


	def remove_from_bufferpool(self,index: BufferpoolIndex) -> None:
		self[index] = None
		# self.change_bufferpool_entry(BufferpoolEntry(0, None, False, None, None, None, None), index)
		# self.pin_counts[index] = 0
		# self.dirty_bits[index] = False
		# self.buffered_physical_pages[index] = None
		# self.ids_of_physical_pages[index] = None
		# self.index_of_physical_page_in_the_page[index] = None

	def write_to_disk(self, table: Table, index: BufferpoolIndex) -> None:
		# table_name = self.table_names[index]
		# table: 'Table' = next(table for table in self.tables if table.name == table_name)
		page_id = self[index].physical_page_id
		physical_page_index = self[index].physical_page_index
		assert page_id is not None
		assert physical_page_index is not None
		physical_page = self[physical_page_index].physical_page
		assert physical_page is not None
		path = os.path.join(self.path, table.file_handler.page_id_to_path(page_id))
		with open(path, 'wb') as file:
			file.seek(physical_page_index.toDataIndex() * config.PHYSICAL_PAGE_SIZE)
			if self[index].physical_page != None:
				file.write(physical_page.data)

