from __future__ import annotations
import time
import typing
import os
from lstore.table import PageDirectoryEntry
from lstore.ColumnIndex import DataIndex
from lstore.base_tail_page import BasePage
from lstore.config import Metadata, config
from lstore.helper import helper
from lstore.page import Page
from lstore.record_physical_page import PhysicalPage, Record
from typing import Literal, TypeVar, Generic, Annotated

from lstore.table import Table

class PageID(int):
	class Catalog:
		pass
	pass

class BasePageID(PageID):
	pass

class TailPageID(PageID):
	pass

class MetadataPageID(PageID):
	pass

T = TypeVar('T')
class Buffered(Generic[T]):
	
	def __del__(self) -> None:
		self.bufferpool.change_pin_count(self.buff_indices, -1)

	# value can actually have Any type here
	def __setattr__(self, name: str, value) -> None: # type: ignore[no-untyped-def]
		if name == "contents" and self.initialized:
			raise(Exception("buffered copies are read only"))
		super().__setattr__(name, value)

	def __init__(self, bufferpool: 'Bufferpool', buff_indices: list[int], contents: T):
		self.initialized = False
		self.bufferpool = bufferpool
		self.bufferpool.change_pin_count(buff_indices, +1)
		self.buff_indices = buff_indices
		self.contents = contents
		self.initialized = True
		# self.contents: T = contents


class Bufferpool:
	# buffered_physical_pages: list[Buffered[PhysicalPage]] = []
	# page_to_commit: typing.Annotated[list[PhysicalPage], self.num_raw_columns] = []

	# buffered_physical_pages: dict[int, Buffered[PhysicalPage]] = {}
	pin_counts: list[int] = []
	# TODO: flush catalog information, like last_base_page_id, to disk before closing
	def __init__(self, path, tables: list[Table]) -> None: 
		self.pin_counts: Annotated[list[int], config.BUFFERPOOL_SIZE] = [0] * config.BUFFERPOOL_SIZE
		self.buffered_physical_pages: Annotated[list[PhysicalPage | None], config.BUFFERPOOL_SIZE] = [None] * config.BUFFERPOOL_SIZE
		self.tables = tables
		# self.file_handlers = {table.name: FileHandler(table) for table in self.tables} # create FileHandlers for each table
		# self.table_for_physical_pages: Annotated[list[str | None], config.BUFFERPOOL_SIZE] = [None] * config.BUFFERPOOL_SIZE
		self.dirty_bits: Annotated[list[bool], config.BUFFERPOOL_SIZE] = [False] * config.BUFFERPOOL_SIZE
		self.ids_of_physical_pages = []
		self.index_of_physical_page_in_the_page= []
		self.path = path


	def change_pin_count(self, buff_indices: list[int], change: int) -> None:
		for idx in buff_indices:
			self.pin_counts[idx] += change




	def insert_record(self, table_name: str, metadata: Metadata, *columns: int) -> int: # returns RID of inserted record
		table_list = list(filter(lambda table: table.name == table_name, self.tables))
		assert len(table_list) == 1
		table = table_list[0]
		return table.file_handler.insert_record(metadata, *columns)


		# if full:
		# 	self.file_handler.write_page(self.page_to_commit, self.last_base_page_id)
		# 	self.last_base_page_id += 1

	"""
	def get_record_by_rid(self, rid: int) -> Buffered[Record]:
		pass
	def get_page_by_pageid(self, page_id: int) -> Buffered[Page]:
		pass
	
	"""


	def is_record_in_bufferpool(self, table_name : str, record_id : int, projected_columns_index: list[Literal[0] | Literal[1]]): ## this will be called inside the bufferpool functions
		table = None
		for curr_table in self.tables:
			if curr_table.name == table_name:
				table = curr_table
		
		for key in table.page_directory.keys:
			if key== record_id:
				page_directory_entry = table.page_directory[key]
				break 
		record_page=page_directory_entry.page


		number_of_columns = sum(projected_columns_index)
		num_of_columns_found = 0
		for i in range(len(self.ids_of_physical_pages)):
			if (self.ids_of_physical_pages[i] == record_page.id) and (projected_columns_index[i] == 1):
				num_of_columns_found += 1

		return num_of_columns_found == number_of_columns

	def get_record(self, record: rid):
		pass


	def evict_physcical_page_clock(self) -> None:
		for i in range(len(self.buffered_physical_pages)):
			if self.pin_counts[i] == 0:
				if self.dirty_bits[i]==1: #remove from the buffer without writing in disk
					self.write_to_disk(i)
				self.remove_dirty_from_buffer(i)


	def remove_dirty_from_buffer(self,index):
		self.pin_counts.remove(index)
		self.dirty_bits.remove(index)
		self.buffered_physical_pages.remove(index)
		self.ids_of_physical_pages.remove(index)
		self.index_of_physical_page_in_the_page.remove(index)


	def write_to_disk(self, index: int):

		for 



		with open(self.path + "/", )


			

class BufferedValue():
	def __init__(self, file_handler: FileHandler, page_sub_path: PageID | Literal["catalog"], byte_position: int):
		page_path = file_handler.page_id_to_path(page_sub_path) if isinstance(page_sub_path, PageID) else file_handler.catalog_path
		with open(page_path) as file:
			file.seek(byte_position)
			self._value = int(file.read(8)) # assume buffered value is 8 bytes
		self.page_path = page_path
		self.file_handler = file_handler
		self.byte_position = byte_position
	def flush(self) -> None:
		FileHandler.write_position(self.page_path, self.byte_position, self._value)


	def value(self, increment: int=0) -> int:
		if increment != 0:
			self._value += increment 
		return self._value

	def __del__(self) -> None: # flush when this value is deleted
		self.flush()
	
class FileHandler:
	def __init__(self, table: 'Table') -> None:
		# self.last_base_page_id = self.get_last_base_page_id()
		self.last_base_page_id = BufferedValue(self, "catalog", config.byte_position.CATALOG_LAST_BASE_ID)
		self.last_tail_page_id = BufferedValue(self, "catalog", config.byte_position.CATALOG_LAST_TAIL_ID)
		self.last_metadata_page_id = BufferedValue(self, "catalog", config.byte_position.CATALOG_LAST_METADATA_ID)
		self.last_rid = BufferedValue(self, "catalog", config.byte_position.CATALOG_LAST_RID)
		# TODO: populate the offset byte with 0 when creating a new page
		self.offset = BufferedValue(self, BasePageID(self.last_base_page_id.value() - 1), config.byte_position.BASE_OFFSET) # the current offset is based on the last written page
		self.table = table
		self.page_to_commit: Annotated[list[PhysicalPage], self.table.total_columns] = self.read_base_page(self.last_base_page_id.value()) # could be empty PhysicalPages, to start
		# check that physical page sizes and offsets are the same
		assert len(
			set(map(lambda physicalPage: physicalPage.size, self.page_to_commit))) <= 1
		assert len(
			set(map(lambda physicalPage: physicalPage.offset, self.page_to_commit))) <= 1
		self.table_path = os.path.join(config.PATH, self.table.name)
	def base_path(self, base_page_id: int) -> str:
		return os.path.join(self.table_path, f"base_{base_page_id}")
		# return os.path.join(config.PATH, self.table.name, f"base_{base_page_id}")
	def tail_path(self, tail_page_id: int) -> str:
		return os.path.join(self.table_path, f"tail_{tail_page_id}")
		# return os.path.join(config.PATH, self.table.name, f"tail_{tail_page_id}")
	def metadata_path(self, metadata_page_id: int) -> str:
		return os.path.join(self.table_path, f"metadata_{metadata_page_id}")

	@property
	def catalog_path(self) -> str:
		return os.path.join(self.table_path, "catalog")
		# return os.path.join(config.PATH, self.table.name, "catalog")

	# def get_last_ids(self) -> tuple[int, int, int]: # writing boolean specifies whether this id will be written to by the user.
	# 	with open(self.catalog_path, "r") as file:
	# 		return (int(file.read(8)), int(file.read(8)), int(file.read(8)))

	def page_id_to_path(self, page_id: PageID) -> str:
		path: str = ""
		if isinstance(page_id, BasePageID):
			path = self.base_path(page_id)
		elif isinstance(page_id, TailPageID):
			path = self.tail_path(page_id)
		elif isinstance(page_id, MetadataPageID):
			path = self.metadata_path(page_id)
		else:
			raise(Exception(f"page_id had unexpected type of {type(page_id)}"))
		return path

	@staticmethod
	def write_position(page_path: str, byte_position: int, value: int) -> bool:
		with open(page_path, "wb") as file:
			file.seek(byte_position)
			file.write(value.to_bytes(config.BYTES_PER_INT, byteorder="big"))
		return True

	def write_new_base_page(self, physical_pages: list[PhysicalPage]) -> bool: # the page MUST be full in order to write. returns true if success
		bpath = self.base_path(self.last_base_page_id.value(1))

		# check that physical page sizes and offsets are the same
		assert len(
			set(map(lambda physicalPage: physicalPage.size, physical_pages))) <= 1
		assert len(
			set(map(lambda physicalPage: physicalPage.offset, physical_pages))) <= 1

		metadata_pointer = self.last_metadata_page_id.value(1)

		with open(bpath, "wb") as file:
			file.write(metadata_pointer.to_bytes(config.BYTES_PER_INT, byteorder="big"))
			file.write((16).to_bytes(8, byteorder="big")) # offset 16 is the first byte offset where data can go
			for physical_page in physical_pages:
				file.write(physical_page.data)
		# self.page_to_commit.clear()
		self.page_to_commit = self.read_base_page(self.last_base_page_id.value())
		return True

	def read_base_page(self, base_page_id: int) -> list[PhysicalPage]: # reads the full base page written to disk
		physical_pages: list[PhysicalPage] = []
		with open(self.base_path(base_page_id), "rb") as file: 
			for _ in range(self.table.total_columns):
				physical_pages.append(PhysicalPage(data=bytearray(file.read(config.PHYSICAL_PAGE_SIZE)), offset=config.PHYSICAL_PAGE_SIZE))
		return physical_pages

	def insert_record(self, metadata: Metadata, *columns: int | None) -> int: # returns RID of inserted record
		null_bitmask = 0
		total_cols = self.table.total_columns
		if metadata.indirection_column == None: # set 1 for null indirection column
			# print("setting indirection null bit")
			null_bitmask = helper.ith_total_col_shift(total_cols, config.INDIRECTION_COLUMN)
			# null_bitmask = 1 << (total_cols - 1)
		for idx, column in enumerate(columns):
			# print(f"checking cols for null... {column}")
			if column is None:
				# print("found a null col")
				null_bitmask = null_bitmask | helper.ith_total_col_shift(len(columns), idx, False) 
				# null_bitmask = null_bitmask | (1 << (len(columns)-idx-1))
			
		# print(f"inserting null bitmask {bin(null_bitmask)}")
		
		# Transform columns to a list to append the schema encoding and the indirection column
		# print(columns)
		list_columns: list[int | None] = list(columns)
		rid = self.last_rid.value(1)
		list_columns.insert(config.INDIRECTION_COLUMN, metadata.indirection_column)
		list_columns.insert(config.RID_COLUMN, rid)
		list_columns.insert(config.TIMESTAMP_COLUMN, int(time.time()))
		list_columns.insert(config.SCHEMA_ENCODING_COLUMN, metadata.schema_encoding)
		list_columns.insert(config.NULL_COLUMN, null_bitmask)
		cols = tuple(list_columns)
		for i in range(len(self.page_to_commit)):
			self.page_to_commit[i].insert(cols[i])
			self.offset.value(config.BYTES_PER_INT)
		if self.offset.value() == config.PHYSICAL_PAGE_SIZE:
			self.write_new_base_page(self.page_to_commit)	
		return rid

	

