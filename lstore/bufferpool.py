from __future__ import annotations
from collections import namedtuple
import pickle
import time
import typing
import glob
import os
from lstore.table import PageDirectoryEntry
from lstore.ColumnIndex import DataIndex, RawIndex
from lstore.base_tail_page import BasePage
from lstore.config import FullMetadata, WriteSpecifiedMetadata, config
from lstore.helper import helper
from lstore.record_physical_page import PhysicalPage, Record
from typing import Any, List, Literal, Tuple, Type, TypeVar, Generic, Annotated

from lstore.table import Table

class PageID(int):
	pass

class BasePageID(PageID):
	pass

class TailPageID(PageID):
	pass

class MetadataPageID(PageID):
	pass

class BufferpoolIndex(int):
	pass

class BufferedRecord:
	def __del__(self) -> None:
		self.bufferpool.change_pin_count(self.base_buff_indices, -1)
	def __getattribute__(self, attr: str): # type: ignore[no-untyped-def]
		if attr == "_contents":
			raise(Exception("do NOT get the contents directly"))
		super().__getattribute__(attr)
	# value can actually have Any type here
	def __setattr__(self, name: str, value) -> None: # type: ignore[no-untyped-def]
		if name == "_contents" and self.initialized:
			raise(Exception("buffered copies are read only"))
		super().__setattr__(name, value)
	def __init__(self, bufferpool: 'Bufferpool', buff_indices: List[BufferpoolIndex], table_name: str, record_offset: int, record_id: int, projected_columns_index: List[Literal[0, 1]]):
		self.initialized = False
		self.bufferpool = bufferpool
		self.buff_indices = buff_indices # frame indices of base pages (including metadata and data)
		self.bufferpool.change_pin_count(self.buff_indices, +1) # increment pin counts of relevant bufferpool frames
		# self.tail_buff_indices = tail_buff_indices # frame indices of tail pages (including metadata and data)
		self.table: Table = next(table for table in self.bufferpool.tables if table.name == table_name)
		self.record_offset = record_offset
		self.record_id = record_id
		self.projected_columns_index = projected_columns_index
		self.initialized = True

	def add_buff_idx(self, buff_idx: BufferpoolIndex) -> None:
		self.buff_indices.append(buff_idx)
		self.bufferpool.pin_counts[buff_idx] += 1 # will be decremented later because it was addded to base_buff_indices

	def unpin_buff_indices(self, buff_indices: List[BufferpoolIndex]) -> None:
		for buff_idx in buff_indices:
			self.bufferpool.pin_counts[buff_idx] -= 1
			self.buff_indices.remove(buff_idx)

	def get_value(self) -> Record:
		metadata_cols: List[PhysicalPage] = []
		data_cols: List[PhysicalPage] = []
		for buff_idx in self.base_buff_indices:
			col_idx = self.bufferpool.index_of_physical_page_in_the_page[buff_idx]
			if col_idx < config.NUM_METADATA_COL: # this is metadata column
				metadata_cols.append(self.bufferpool.buffered_physical_pages[buff_idx])
			else:
				data_cols.append(self.bufferpool.buffered_physical_pages[buff_idx])

		all_cols = metadata_cols + data_cols

		def get_no_none_check(col_idx: RawIndex, record_offset: int) -> int:
			return helper.unpack_data(all_cols[col_idx].data, record_offset)
		def get_check_for_none(col_idx: RawIndex, record_offset: int) -> int | None:
			# val = all_cols[col_idx][record_offset]
			# val = int.from_bytes(all_cols[col_idx].data[record_offset:record_offset+config.BYTES_PER_INT], byteorder="big")
			val = get_no_none_check(col_idx, record_offset)
			# # print("getting checking null")
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
			

		indirection_column, rid, schema_encoding, timestamp, key_col, null_col = \
			get_check_for_none(config.INDIRECTION_COLUMN, self.record_offset), \
			get_check_for_none(config.RID_COLUMN, self.record_offset), \
			get_no_none_check(config.SCHEMA_ENCODING_COLUMN, self.record_offset), \
			get_no_none_check(config.TIMESTAMP_COLUMN, self.record_offset), \
			get_check_for_none(helper.data_to_raw_idx(self.table.key_index), self.record_offset), \
			get_check_for_none(config.NULL_COLUMN, self.record_offset)

		# this is just outside the metadata column range. thus it should reveal the actual page type of base or tail (both base and tail pages will have "metadata" type pages in the bufferpool)
		page_type = self.bufferpool.page_types[self.buff_indices[config.NUM_METADATA_COL]] 
		is_base_page = False
		if page_type == "base":
			is_base_page = True
		elif page_type == "metadata":
			raise(Exception("should not be getting metadata page type in get_record."))

		return Record(FullMetadata(indirection_column, timestamp, indirection_column,schema_encoding, null_col), is_base_page, *[helper.unpack_data(data_col.data, self.record_offset) for data_col in data_cols])
				
class BufferpoolSearchResult:
	TNOT_FOUND = Literal[-1]
	def __init__(self, found: bool, data_buff_indices: List[None | TNOT_FOUND | BufferpoolIndex], metadata_buff_indices: List[TNOT_FOUND | BufferpoolIndex], record_offset: int | None):
		self.found = found
		self.data_buff_indices = data_buff_indices
		self.metadata_buff_indices = metadata_buff_indices
		self.record_offset = record_offset

class FilePageReadResult:
	def __init__(self, metadata_physical_pages: list[PhysicalPage | None], data_physical_pages: list[PhysicalPage | None], page_type: Literal["base", "tail"]):
		self.metadata_physical_pages = metadata_physical_pages
		self.data_physical_pages = data_physical_pages
		self.page_type = page_type

class FullFilePageReadResult:
	def __init__(self, metadata_physical_pages: list[PhysicalPage], data_physical_pages: list[PhysicalPage], page_type: Literal["base", "tail"]):
		self.metadata_physical_pages = metadata_physical_pages
		self.data_physical_pages = data_physical_pages
		self.page_type = page_type

class BufferpoolEntry:
	def __init__(self, pin_count: int, physical_page: PhysicalPage | None, dirty_bit: bool, physical_page_id: PageID | None, physical_page_index: RawIndex | None, page_type: Literal["base", "tail", "metadata"] | None, table_name: str | None):
		# if pin_count != None: # None for pin_count will not change pin_count
		self.pin_count = pin_count 
		self.physical_page = physical_page
		self.dirty_bit = dirty_bit
		self.physical_page_id = physical_page_id
		self.physical_page_index = physical_page_index
		self.page_type = page_type
		self.table_name = table_name

# class BufferpoolList(list):
# 	def __getitem__(self, key: BufferpoolIndex):
# 		super().__getitem__(key)

class Bufferpool:
	# buffered_physical_pages: list[Buffered[PhysicalPage]] = []
	# page_to_commit: typing.Annotated[list[PhysicalPage], self.num_raw_columns] = []

	# buffered_physical_pages: dict[int, Buffered[PhysicalPage]] = {}
	TProjected_Columns = List[Literal[0, 1]]
	# TODO: flush catalog information, like last_base_page_id, to disk before closing
	def __init__(self, path: str, tables: list[Table]) -> None: 
		# self.buffered_metadata: Annotated[list[list[PhysicalPage], config.BUFFERPOOL_SIZE]]  = [[]]
		# self.buffered_metadata: Annotated[list[list[PhysicalPage]], config.BUFFERPOOL_SIZE] = [[]]

		# self.file_handlers = {table.name: FileHandler(table) for table in self.tables} # create FileHandlers for each table
		# self.table_for_physical_pages: Annotated[list[str | None], config.BUFFERPOOL_SIZE] = [None] * config.BUFFERPOOL_SIZE
		self.pin_counts: Annotated[list[int], config.BUFFERPOOL_SIZE] = [0] * config.BUFFERPOOL_SIZE
		self.buffered_physical_pages: Annotated[list[PhysicalPage | None], config.BUFFERPOOL_SIZE] = [None] * config.BUFFERPOOL_SIZE
		self.dirty_bits: Annotated[list[bool], config.BUFFERPOOL_SIZE] = [False] * config.BUFFERPOOL_SIZE
		self.ids_of_physical_pages : Annotated[list[PageID | None], config.BUFFERPOOL_SIZE] = [None] * config.BUFFERPOOL_SIZE # the id of the PAGE that the physical page belongs to
		self.index_of_physical_page_in_the_page : list[RawIndex | None] = [None] * config.BUFFERPOOL_SIZE # This tells you if its the first, second, third, etc physical page of the page
		self.page_types: Annotated[list[Literal["base", "tail", "metadata"] | None], config.BUFFERPOOL_SIZE] = [None] * config.BUFFERPOOL_SIZE
		self.table_names: Annotated[List[str | None], config.BUFFERPOOL_SIZE] = [None] * config.BUFFERPOOL_SIZE
		self.client_records: List[BufferedRecord] = [] # a list of all BufferedRecords provided by this class to clients
		self.tables = tables
		self.path = path
		self.curr_clock_hand = 0

	def close_bufferpool(self) -> None:
		for i in [BufferpoolIndex(_) for _ in range(len(self.buffered_physical_pages))]:
			if self.dirty_bits[i]==True:
				self.write_to_disk(i)
			self.buffered_physical_pages[i]=None
			self.dirty_bits[i]
			bufferpool_entry=BufferpoolEntry(0, None, False,  None,  None,  None,  None)
			self.change_bufferpool_entry(bufferpool_entry,BufferpoolIndex(i))
		

	def change_bufferpool_entry(self, entry: BufferpoolEntry, buff_idx: BufferpoolIndex) -> None:
		self.pin_counts[buff_idx] = entry.pin_count
		self.buffered_physical_pages[buff_idx] = entry.physical_page
		self.dirty_bits[buff_idx] = entry.dirty_bit
		self.ids_of_physical_pages[buff_idx] = entry.physical_page_id
		self.index_of_physical_page_in_the_page[buff_idx] = entry.physical_page_index
		self.page_types[buff_idx] = entry.page_type
		self.table_names[buff_idx] = entry.table_name


	def change_pin_count(self, buff_indices: list[BufferpoolIndex], change: int) -> None:
		for idx in buff_indices:
			self.pin_counts[idx] += change


	def insert_record(self, table_name: str, record_type: Literal["base", "tail"], metadata: WriteSpecifiedMetadata, *columns: int) -> int: # returns RID of inserted record
		table_list = list(filter(lambda table: table.name == table_name, self.tables))
		assert len(table_list) == 1
		table = table_list[0]
		return table.file_handler.insert_record(record_type, metadata, *columns)

	# returns whether the requested portion of record is in the bufferpool, 
	# and the bufferpool indices of any column of the record found (regardless of whether it was)
	# fully found or not
	def is_record_in_bufferpool(self, table_name: str, rid : int, projected_columns_index: list[Literal[0] | Literal[1]]) -> BufferpoolSearchResult: ## this will be called inside the bufferpool functions
		table: Table = next(table for table in self.tables if table.name == table_name)
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
			if self.ids_of_physical_pages[i] == record_page_id:
				raw_idx = self.index_of_physical_page_in_the_page[i] 	
				assert raw_idx is not None, "non None in ids_of_physical_pages but None in index_of_physical_page_in_page?"
				data_idx = raw_idx.toDataIndex()
				if data_idx in requested_columns: # is a data column we requested?
					#column_list[i] = self.buffered_physical_pages[i].data
					# num_of_columns_found += 1
					data_buff_indices.append(i)
					raw_idx = self.index_of_physical_page_in_the_page[i]
					assert raw_idx is not None
					data_idx = raw_idx.toDataIndex()
					data_buff_indices[data_idx] = i
				elif raw_idx in range(config.NUM_METADATA_COL):
					metadata_buff_indices.append(i)
				else: 
					continue

		found = (len([True for idx in data_buff_indices if idx == -1]) > 0) or (len([True for idx in metadata_buff_indices if idx == -1]) > 0)
		return BufferpoolSearchResult(found, data_buff_indices, metadata_buff_indices, page_directory_entry.offset)


	def bring_from_disk(self, table_name : str, record_id: int, projected_columns_index: list[Literal[0] | Literal[1]], record_type: Literal["base", "tail"]) -> bool: # returns true if success
		# https://stackoverflow.com/questions/2361426/get-the-first-item-from-an-iterable-that-matches-a-condition
		table: Table = next(table for table in self.tables if table.name == table_name)
		requested_columns: list[int] = [i for i, binary_item in enumerate(projected_columns_index) if binary_item == 1]

		t = self.is_record_in_bufferpool(table_name, record_id, projected_columns_index)
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
				page_type = page_directory_entry.page_type if i >= config.NUM_METADATA_COL else "metadata" # metadata columns are at the beginning
				page_id = BasePageID(record_page_id) if page_type == "base" else TailPageID(record_page_id) if page_type == "tail" else MetadataPageID(record_page_id)
				# okay technically speaking,
				# this typing is now redundant since page_type exists...
				# keeping it here bc too lazy to refactor
				self.change_bufferpool_entry(BufferpoolEntry(0, physical_page, False, page_id, i, page_type, table_name), new_buff_idx)
		return True
			# self.pin_counts[new_buff_idx] = 0 # initialize to


	def get_updated_col(self, table_name: str, record: Record, col_idx: DataIndex) -> int | None:
		if record.metadata.rid == None:
			return None # deleted record.
		table: Table = next(table for table in self.tables if table.name == table_name)
		desired_col: int | None = record[col_idx]
		schema_encoding = record.metadata.schema_encoding
		if helper.ith_bit(schema_encoding, table.num_columns, col_idx, False) == 0b1:
			curr_rid = record.metadata.indirection_column
			assert curr_rid is not None, "record rid wasn't none, so none of the indirections should be none either"
			proj_col: List[Literal[0, 1]] = [0] * table.num_columns
			proj_col[col_idx] = 1 # only get the desired column
			curr_record = self.get_record(table_name, curr_rid, proj_col)
			assert curr_record is not None, "a record with a non-None RID was not found"
			curr_schema_encoding = curr_record.get_value().metadata.schema_encoding
			while helper.ith_bit(curr_schema_encoding, table.num_columns, col_idx, False) == 0b0: # while not found
				assert curr_record is not None, "a record with a non-None RID was not found"
				curr_rid = curr_record.get_value().metadata.indirection_column
				assert curr_rid is not None
				curr_record = self.get_record(table_name, curr_rid, proj_col)
				assert curr_record is not None
				curr_schema_encoding = curr_record.get_value().metadata.schema_encoding
			desired_col = curr_record.get_value()[col_idx]
		return desired_col 


	# THIS FUNCTION RECEIVES ONLY **BASE** RECORDS
	def get_updated_record(self, table_name: str, record_id: int, projected_columns_index: list[Literal[0] | Literal[1]]) -> Record | None:
		# table: Table = next(table for table in self.tables if table.name == table_name)

		# If there are multiple writers we probably need a lock here so the indirection column is not modified after we get it
		buffered_record = self.get_record(table_name, record_id, projected_columns_index)
		if buffered_record is None:
			return None
		columns: List[int | None] = []
		for i in range(len(projected_columns_index)):
			if projected_columns_index[i] == 1:
				columns.append(self.get_updated_col(table_name, buffered_record.get_value(), DataIndex(i)))
			else:
				columns.append(None)
		updated_record = Record(buffered_record.get_value().metadata, True, *columns)
		return updated_record


	# TODO: remove type ignore
	# TODO: get updated value with schema encoding (maybe not this function)
	# TODO: specialize for tail records to only put the non-null columns in bufferpool
	def get_record(self, table_name: str, record_id: int, projected_columns_index: list[Literal[0] | Literal[1]]) -> BufferedRecord | None:
		table: Table = next(table for table in self.tables if table.name == table_name)
		requested_columns: list[int] = [i for i, binary_item in enumerate(projected_columns_index) if binary_item == 1]
		t = self.is_record_in_bufferpool(table_name, record_id, projected_columns_index)
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
			if buff_idx == -1:
				data_cols_to_get.append(DataIndex(j))
			elif buff_idx == None:
				data_physical_pages.append(None)
			elif isinstance(buff_idx, BufferpoolIndex):
				data_physical_pages.append(self.buffered_physical_pages[buff_idx])
				self.pin_counts[buff_idx] += 1 
			else:
				raise(Exception("unexpected datatype for buff_idx inside the is_record_in_bufferpool output"))

		for j, buff_idx in enumerate(metadata_buff_indices):
			if buff_idx == -1:
				metadata_cols_to_get.append(DataIndex(j))
			else:
				physical_page = self.buffered_physical_pages[buff_idx]
				assert physical_page is not None
				metadata_physical_pages.append(physical_page)
				self.pin_counts[buff_idx] += 1 


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
			self.change_bufferpool_entry(BufferpoolEntry(0, all_physical_pages[i], False, page_directory_entry.page_id, RawIndex(i), t_.page_type, table_name), buff_idx)



		t_2 = self.is_record_in_bufferpool(table_name, record_id, projected_columns_index)
		found, data_buff_indices, metadata_buff_indices, record_offset = t_2.found, t_2.data_buff_indices, t_2.metadata_buff_indices, t_2.record_offset
		assert found, "record not found after bringing it into bufferpool"
		assert record_offset is not None
		filtered_data_buff_indices =  [idx for idx in data_buff_indices if (idx is not None) and (idx != -1) and (isinstance(idx, BufferpoolIndex)) ]
		filtered_metadata_buff_indices =  [idx for idx in metadata_buff_indices if (idx != -1) and (isinstance(idx, BufferpoolIndex)) ]
		assert len(filtered_data_buff_indices) == len(data_buff_indices)
		assert len(filtered_metadata_buff_indices) == len(metadata_buff_indices)

		r = BufferedRecord(self, filtered_metadata_buff_indices + filtered_data_buff_indices, table_name, record_offset, record_id, projected_columns_index)
		return r
		

	# returns buffer index of evicted physical page. 
	# does not evict anything in the `save` array.
	def evict_physical_page_clock(self, save: List[BufferpoolIndex] = []) -> BufferpoolIndex | None: 
		start_hand = self.curr_clock_hand % config.BUFFERPOOL_SIZE
		def curr_hand() -> BufferpoolIndex:
			return BufferpoolIndex(self.curr_clock_hand % config.BUFFERPOOL_SIZE)
		while curr_hand() != start_hand:
			i = curr_hand()
			if not i in save:
				if self.pin_counts[i] == 0:
					if self.dirty_bits[i] == 1: 
						self.write_to_disk(i)
					self.remove_from_bufferpool(i) # remove from the buffer without writing in disk
					return i
			self.curr_clock_hand += 1
		return None

	# NOTE: just pin the indices you want to keep rather than populating the save array... 
	def evict_n_slots(self, n: int, save: List[BufferpoolIndex] = []) -> List[BufferpoolIndex] | None: # returns buffer indices freed, or None if not all slots could be evicted
		evicted_buff_idx: list[BufferpoolIndex] = []
		for _ in range(n):
			evicted = self.evict_physical_page_clock(save)
			if evicted is None:
				return None
			evicted_buff_idx.append(evicted)
		return evicted_buff_idx


	def remove_from_bufferpool(self,index: BufferpoolIndex) -> None:
		self.change_bufferpool_entry(BufferpoolEntry(0, None, False, None, None, None, None), index)
		# self.pin_counts[index] = 0
		# self.dirty_bits[index] = False
		# self.buffered_physical_pages[index] = None
		# self.ids_of_physical_pages[index] = None
		# self.index_of_physical_page_in_the_page[index] = None


	def write_to_disk(self, index: BufferpoolIndex) -> None:
		table_name = self.table_names[index]
		table: Table = next(table for table in self.tables if table.name == table_name)
		page_id = self.ids_of_physical_pages[index]
		physical_page_index = self.index_of_physical_page_in_the_page[index]
		assert page_id is not None
		assert physical_page_index is not None
		physical_page = self.buffered_physical_pages[physical_page_index]
		assert physical_page is not None
		path = os.path.join(self.path, table.file_handler.page_id_to_path(page_id))
		with open(path, 'wb') as file:
			file.seek(physical_page_index.toDataIndex() * config.PHYSICAL_PAGE_SIZE)
			if self.buffered_physical_pages[index] != None:
				file.write(physical_page.data)


# this class is called "PseudoBuff" because it kind of works like the BufferedValue
# but without an actual Bufferpool (hence "pseudo")
class PsuedoBuffIntValue():
	def __init__(self, file_handler: FileHandler, page_sub_path: PageID | Literal["catalog"], byte_position: int) -> None:
		self.page_sub_path = page_sub_path
		self.page_path = file_handler.page_path(page_sub_path)
		self.file_handler = file_handler
		self.byte_position = byte_position
		self._value = file_handler.read_int_value(page_sub_path, byte_position)
		# self._value = file_handler.read_value(page_sub_path, byte_position, "int")
	def flush(self) -> None:
		FileHandler.write_position(self.page_path, self.byte_position, self._value)
	def value(self, increment: int=0) -> int:
		if increment != 0:
			self._value += increment 
		return self._value
	def __del__(self) -> None: # flush when this value is deleted
		self.flush()

class PseudoBuffBaseIDValue(PsuedoBuffIntValue):
	def value(self, increment: int = 0) -> BasePageID:
		super().value()
		return BasePageID(self._value)


U = TypeVar('U')
V = TypeVar('V')
class PseudoBuffDictValue(Generic[U, V]):
	def __init__(self, file_handler: FileHandler, page_sub_path: Literal["page_directory", "indices"]):
		self.page_sub_path = page_sub_path
		self.page_path = file_handler.page_path(page_sub_path)
		self.file_handler = file_handler
		self._value = file_handler.read_dict_value(page_sub_path)
		# self._value = file_handler.read_value(page_sub_path, byte_position, "int")
	def flush(self) -> None:
		with open(self.page_path, "wb") as handle:
			pickle.dump(self._value, handle)
	def value_get(self) -> dict[U, V]:
		return self._value
	def __getitem__(self, key: U) -> V:
		return self._value[key]
	def value_assign(self, new_key: U, new_value: V) -> dict[U, V]:
		self._value[new_key] = new_value
		return self._value
	def __del__(self) -> None: # flush when this value is deleted
		self.flush()

class FileHandler:
	def __init__(self, table: 'Table') -> None:
		# self.last_base_page_id = self.get_last_base_page_id()

		# NOTE: these next_*_id variables represent the *next* id to be written, not necessarily the last one. the last 
		# written id is the next_*_id variable minus 1
		self.next_base_page_id = PseudoBuffBaseIDValue(self, "catalog", config.byte_position.CATALOG_LAST_BASE_ID)
		self.next_tail_page_id = PsuedoBuffIntValue(self, "catalog", config.byte_position.CATALOG_LAST_TAIL_ID)
		self.next_metadata_page_id = PsuedoBuffIntValue(self, "catalog", config.byte_position.CATALOG_LAST_METADATA_ID)
		self.next_rid = PsuedoBuffIntValue(self, "catalog", config.byte_position.CATALOG_LAST_RID)
		# TODO: populate the offset byte with 0 when creating a new page
		self.offset = PsuedoBuffIntValue(self, BasePageID(self.next_base_page_id.value() - 1), config.byte_position.OFFSET) # the current offset is based on the last written page
		self.table = table
		t_base = self.read_projected_cols_of_page(BasePageID(self.next_base_page_id.value() - 1)) # could be empty PhysicalPages, to start. but the page files should still exist, even when they are empty
		if t_base is None:
			raise(Exception("the base_page_id just before the next_page_id must have a folder."))
		t_tail = self.read_projected_cols_of_page(TailPageID(self.next_base_page_id.value() - 1))
		if t_tail is None:
			raise(Exception("the tail_page_id just before the next_page_id must have a folder."))
		
		self.base_page_to_commit: Annotated[list[PhysicalPage | None], self.table.total_columns] = t_base.metadata_physical_pages + t_base.data_physical_pages # could be empty PhysicalPages, to start. concatenating the metadata and physical 
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

	# this calculated property gives the path for a "table file". 
	# Table files are files which apply to the entire table. These files are, as of now, 
	# "catalog", "page_directory.pickle", and "indices.pickle". The page directory and indices
	# files are only specified by their names (even though they will be persisted separately with pickle)
	def table_file_path(self, file_name: Literal["catalog", "page_directory", "indices"]) -> str:
		path = os.path.join(self.table_path, file_name)
		if file_name == "page_directory" or file_name == "indices":
			path += ".pickle" # these files have the .pickle extension
		return path

	@staticmethod
	def table_file_path_static(self, file_name: Literal["catalog", "page_directory", "indices"], table_name: str) -> str:
		path = os.path.join(os.path.join(), file_name)
		if file_name == "page_directory" or file_name == "indices":
			path += ".pickle" # these files have the .pickle extension
		return path


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

	def write_new_page(self, physical_pages: list[PhysicalPage], path_type: Literal["base", "tail"]) -> bool: # the page MUST be full in order to write. returns true if success
		written_id = self.next_base_page_id.value(1)
		path = self.base_path(written_id) if path_type == "base" else self.base_path(self.next_tail_page_id.value(1))

		# check that physical page sizes and offsets are the same
		assert len(
			set(map(lambda physicalPage: physicalPage.size, physical_pages))) <= 1
		assert len(
			set(map(lambda physicalPage: physicalPage.offset, physical_pages))) <= 1

		metadata_pointer = self.next_metadata_page_id.value(1)
		with open(self.metadata_path(metadata_pointer), "wb") as file: # open metadata file
			for i in range(config.NUM_METADATA_COL):
				file.write(physical_pages[i].data) # write the metadata columns

		with open(path, "wb") as file: # open page file
			file.write(metadata_pointer.to_bytes(config.BYTES_PER_INT, byteorder="big"))
			file.write((16).to_bytes(8, byteorder="big")) # offset 16 is the first byte offset where data can go
			for i in range(config.NUM_METADATA_COL, len(physical_pages)): # write the data columns
				file.write(physical_pages[i].data)

		# t = self.read_page(BasePageID(self.next_base_page_id.value()))
		# assert t
		self.page_to_commit = [PhysicalPage()] * self.table.total_columns
		return True

	def read_value_page_directory(self) -> dict[int, PageDirectoryEntry]:
		page_path = self.page_path("page_directory")
		with open(page_path, "rb") as handle:
			ret: dict[int, PageDirectoryEntry] = pickle.load(handle) # this is not typesafe at all.... ohwell
			return ret

	def read_int_value(self, page_sub_path: PageID | Literal["catalog"], byte_position: int) -> int:
		page_path = self.page_path(page_sub_path)
		with open(page_path) as file:
			assert byte_position is not None
			file.seek(byte_position)
			return int(file.read(8)) # assume buffered value is 8 bytes
		
	def read_dict_value(self, page_sub_path: Literal["page_directory", "indices"]) -> dict:
		with open(page_sub_path, "rb") as handle:
			return pickle.load(handle)


	# returns the full page path, given a particular pageID OR 
	# the special catalog/page_directory files
	def page_path(self, page_sub_path: PageID | Literal["catalog", "page_directory", "indices"]) -> str:
		if isinstance(page_sub_path, PageID):
			return self.page_id_to_path(page_sub_path)
		elif page_sub_path == "catalog" or page_sub_path == "page_directory":
			return self.table_file_path(page_sub_path)
		else:
			raise(Exception(f"unexpected page_sub_path {page_sub_path}"))


	# reads the full base page written to disk
	# the [1] default value is just so that I can overwrite it later with the proper default value; 
	# in other words it is just a placeholder
	# returns None for every column not in projected_columns_index
	def read_projected_cols_of_page(self, page_id: PageID, projected_columns_index: list[Literal[0, 1]] | None = None, projected_metadata_columns_index: list[Literal[0, 1]] | None = None) -> FilePageReadResult | None: 
		projected_columns_index = [1] * self.table.num_columns if projected_columns_index is None else projected_columns_index
		projected_metadata_columns_index = [1] * config.NUM_METADATA_COL if projected_metadata_columns_index is None else projected_metadata_columns_index
		assert projected_columns_index is not None
		assert projected_metadata_columns_index is not None
		physical_pages: list[PhysicalPage | None] = [None] * self.table.num_columns
		metadata_pages: list[PhysicalPage | None] = [None] * config.NUM_METADATA_COL
		metadata_path = self.metadata_path(PsuedoBuffIntValue(self, page_id, config.byte_position.METADATA_PTR).value())
		path = self.page_id_to_path(page_id)
		if not os.path.isfile(metadata_path) or not os.path.isfile(path):
			return None

		offset = self.read_int_value(page_id, config.byte_position.OFFSET)
		# read selected metadata
		with open(metadata_path, "rb") as metadata_file:
			for i in range(config.NUM_METADATA_COL):
				metadata_pages[i] = PhysicalPage(data=bytearray(metadata_file.read(config.PHYSICAL_PAGE_SIZE)), offset=offset)

		# read selected data
		with open(path, "rb") as file: 
			for i in range(self.table.num_columns):
				if projected_columns_index[i] == 1:
					physical_pages[i] = PhysicalPage(data=bytearray(file.read(config.PHYSICAL_PAGE_SIZE)), offset=offset)
					# physical_pages.append(PhysicalPage(data=bytearray(file.read(config.PHYSICAL_PAGE_SIZE)), offset=offset))
				else:
					# physical_pages.append(None)
					file.seek(config.PHYSICAL_PAGE_SIZE, 1) # seek 4096 (or size) bytes forward from current position (the 1 means "from current position")
		page_type: Literal["base", "tail"] = "base"
		if isinstance(page_id, TailPageID):
			page_type = "tail"
		elif not isinstance(page_id, BasePageID):
			raise(Exception("unexpected page_id type that wasn't base or tail page?"))
		return FilePageReadResult(metadata_pages, physical_pages, page_type)

	def read_full_page(self, page_id: PageID) -> FullFilePageReadResult | None:
		res = self.read_projected_cols_of_page(page_id)
		if res is None:
			return None
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



	def insert_record(self, path_type: Literal["base", "tail"], metadata: WriteSpecifiedMetadata, *columns: int | None) -> int: # returns RID of inserted record
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
		rid = self.next_rid.value(1)
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
			self.write_new_page(self.page_to_commit, path_type)	
		pg_dir_entry: PageDirectoryEntry
		if path_type == "base":
			pg_dir_entry = PageDirectoryEntry(BasePageID(self.next_base_page_id.value()), MetadataPageID(self.next_metadata_page_id.value()), self.offset.value(), "base")
		elif path_type == "tail":
			pg_dir_entry = PageDirectoryEntry(TailPageID(self.next_base_page_id.value()), MetadataPageID(self.next_metadata_page_id.value()), self.offset.value(), "tail")
		self.table.page_directory_buff.value_assign(rid, pg_dir_entry)
		return rid
