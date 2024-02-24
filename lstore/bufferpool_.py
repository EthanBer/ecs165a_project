import typing
import os
from lstore.ColumnIndex import DataIndex
from lstore.base_tail_page import BasePage
from lstore.config import Metadata, config
from lstore.helper import helper
from lstore.page import Page
from lstore.record_physical_page import PhysicalPage, Record
from typing import TypeVar, Generic, Annotated
from __future__ import annotations

from lstore.table import Table

class PageID(int):
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
	def __init__(self, tables: list[Table]) -> None: 
		self.pin_counts: Annotated[list[int], config.BUFFERPOOL_SIZE] = [0] * config.BUFFERPOOL_SIZE
		self.buffered_physical_pages: Annotated[list[PhysicalPage | None], config.BUFFERPOOL_SIZE] = [None] * config.BUFFERPOOL_SIZE
		self.tables = tables
		self.file_handlers = {table.name: FileHandler(table) for table in self.tables} # create FileHandlers for each table
		self.table_for_physical_pages: Annotated[list[str | None], config.BUFFERPOOL_SIZE] = [None] * config.BUFFERPOOL_SIZE
		self.dirty_bits: Annotated[list[bool], config.BUFFERPOOL_SIZE] = [False] * config.BUFFERPOOL_SIZE
		pass
	def change_pin_count(self, buff_indices: list[int], change: int) -> None:
		for idx in buff_indices:
			self.pin_counts[idx] += change
	def insert_record(self, table_name: str, metadata: Metadata, *columns: int) -> int: # returns RID of inserted record
		self.file_handlers[table_name].insert_record(metadata, columns)

		if full:
			self.file_handler.write_page(self.page_to_commit, self.last_base_page_id)
			self.last_base_page_id += 1

	"""
	def get_record_by_rid(self, rid: int) -> Buffered[Record]:
		pass
	def get_page_by_pageid(self, page_id: int) -> Buffered[Page]:
		pass
	
	"""

class BufferedValue(Generic[T]):
	def __init__(self, initial_value: T, file_handler: FileHandler, page_id: PageID, byte_position: int):
		self.value = initial_value
		self.page_id = page_id
		self.file_handler = file_handler
		self.byte_position = byte_position
	def flush(self):
		self.
	


class FileHandler:
	def __init__(self, table: Table) -> None:
		self.last_base_page_id = self.get_last_base_page_id()
		self.table = table
		self.page_to_commit: Annotated[list[PhysicalPage], self.table.total_columns] = self.read_base_page(self.last_base_page_id) # could be empty PhysicalPages, to start
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

	def get_last_base_page_id(self) -> int: # writing boolean specifies whether this id will be written to by the user.
		with open(self.catalog_path, "r") as file:
			return int(file.read(8))

	def write_position(self, page_id: PageID, byte_position: int, value: int):
		path: str = ""
		if isinstance(self.page_id, BasePageID):
			path = self.file_handler.base_path(self.page_id)
		elif isinstance(self.page_id, TailPageID):
			path = self.file_handler.tail_path(self.page_id)
		elif isinstance(self.page_id, MetadataPageID):
			path = self.file_handler.metadata_path(self.page_id)
		else:
			raise(Exception(f"page_id had unexpected type of {type(page_id)}"))
		with open(path, "w") as file:
			file.seek(byte_position)
			file.write(value)

	def write_new_page(self, physical_pages: list[PhysicalPage], base_page_id: int) -> bool: # the page MUST be full in order to write. returns true if success
		bpath = self.base_path(base_page_id)

		# check that physical page sizes and offsets are the same
		assert len(
			set(map(lambda physicalPage: physicalPage.size, physical_pages))) <= 1
		assert len(
			set(map(lambda physicalPage: physicalPage.offset, physical_pages))) <= 1

		metadata_pointer = 
		offset = physical_pages[0].offset

		with open(bpath, "wb") as file:
			file.write()
			for physical_page in physical_pages:
				file.write(physical_page.data)
		return True
	def read_base_page(self, base_page_id: int) -> list[PhysicalPage]: # reads the full base page written to disk
		physical_pages: list[PhysicalPage] = []
		with open(self.base_path(base_page_id), "rb") as file: 
			for _ in range(self.table.total_columns):
				physical_pages.append(PhysicalPage(data=bytearray(file.read(config.PHYSICAL_PAGE_SIZE)), offset=config.PHYSICAL_PAGE_SIZE))
		return physical_pages

	def insert_record(self, metadata: Metadata, *columns: int) -> bool:
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
		list_columns = list(columns)
		list_columns.insert(config.INDIRECTION_COLUMN, metadata.indirection_column)
		list_columns.insert(config.RID_COLUMN, metadata.rid)
		list_columns.insert(config.TIMESTAMP_COLUMN, metadata.timestamp)
		list_columns.insert(config.SCHEMA_ENCODING_COLUMN, metadata.schema_encoding)
		list_columns.insert(config.NULL_COLUMN, null_bitmask)
		columns = tuple(list_columns)
		for i, physical_page in enumerate(self.page_to_commit):
			physical_page.insert(columns[i])
		return True

	

