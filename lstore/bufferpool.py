from __future__ import annotations
from collections import namedtuple
import pickle
import time
import typing
import glob
import os
from lstore.file_handler import FileHandler, Table
from lstore.file_result_types import BufferpoolEntry, BufferpoolIndex, BufferpoolSearchResult
from lstore.page_directory_entry import BasePageID, MetadataPageID, PageDirectoryEntry, PageID, TailPageID
from lstore.ColumnIndex import DataIndex, RawIndex
# from lstore.base_tail_page import BasePage
from lstore.config import FullMetadata, WriteSpecifiedMetadata, config
from lstore.helper import helper
from lstore.record_physical_page import PhysicalPage, Record
from typing import Any, List, Literal, Tuple, Type, TypeVar, Generic, Annotated




class BufferedRecord:
	def __del__(self) -> None:
		self.bufferpool.change_pin_count(self.buff_indices, -1)
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
		self.table: 'Table' = next(table for table in self.bufferpool.tables if table.name == table_name)
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
		for buff_idx in self.buff_indices:
			col_idx = self.bufferpool.index_of_physical_page_in_the_page[buff_idx]
			assert col_idx is not None, "column held by BufferedRecord was None!"
			physical_page = self.bufferpool.buffered_physical_pages[buff_idx]
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
			

		indirection_column, rid, schema_encoding, timestamp, key_col, null_col, base_rid = \
			get_check_for_none(config.INDIRECTION_COLUMN, self.record_offset), \
			get_check_for_none(config.RID_COLUMN, self.record_offset), \
			get_no_none_check(config.SCHEMA_ENCODING_COLUMN, self.record_offset), \
			get_no_none_check(config.TIMESTAMP_COLUMN, self.record_offset), \
			get_check_for_none(helper.data_to_raw_idx(self.table.key_index), self.record_offset), \
			get_check_for_none(config.NULL_COLUMN, self.record_offset), \
			get_no_none_check(config.BASE_RID, self.record_offset), \

		# this is just outside the metadata column range. thus it should reveal the actual page type of base or tail (both base and tail pages will have "metadata" type pages in the bufferpool)
		page_type = self.bufferpool.page_types[self.buff_indices[config.NUM_METADATA_COL]] 
		is_base_page = False
		if page_type == "base":
			is_base_page = True
		elif page_type == "metadata":
			raise(Exception("should not be getting metadata page type in get_record."))

		return Record(FullMetadata(indirection_column, timestamp, indirection_column,schema_encoding, null_col, base_rid), is_base_page, *[helper.unpack_data(data_col.data, self.record_offset) for data_col in data_cols])
				

# class BufferpoolList(list):
# 	def __getitem__(self, key: BufferpoolIndex):
# 		super().__getitem__(key)

class Bufferpool:
	# buffered_physical_pages: list[Buffered[PhysicalPage]] = []
	# page_to_commit: typing.Annotated[list[PhysicalPage], self.num_raw_columns] = []

	# buffered_physical_pages: dict[int, Buffered[PhysicalPage]] = {}
	TProjected_Columns = List[Literal[0, 1]]
	# TODO: flush catalog information, like last_base_page_id, to disk before closing
	def __init__(self, path: str, tables: list['Table']) -> None: 
		# self.buffered_metadata: Annotated[list[list[PhysicalPage], config.BUFFERPOOL_SIZE]]  = [[]]
		# self.buffered_metadata: Annotated[list[list[PhysicalPage]], config.BUFFERPOOL_SIZE] = [[]]

		# self.file_handlers = {table.name: FileHandler(table) for table in self.tables} # create FileHandlers for each table
		# self.table_for_physical_pages: Annotated[list[str | None], config.BUFFERPOOL_SIZE] = [None] * config.BUFFERPOOL_SIZE
		self.pin_counts: Annotated[list[int], config.BUFFERPOOL_SIZE] = [0] * config.BUFFERPOOL_SIZE
		self.buffered_physical_pages: Annotated[list[PhysicalPage | None], config.BUFFERPOOL_SIZE] = [None] * config.BUFFERPOOL_SIZE
		self.dirty_bits: Annotated[list[bool], config.BUFFERPOOL_SIZE] = [False] * config.BUFFERPOOL_SIZE
		self.ids_of_physical_pages : Annotated[list[PageID | None], config.BUFFERPOOL_SIZE] = [None] * config.BUFFERPOOL_SIZE # the id of the PAGE that the physical page belongs to
		self.index_of_physical_page_in_the_page : list[RawIndex | None] = [None] * config.BUFFERPOOL_SIZE # This tells you if its the first, second, third, etc physical page of the page
		self.page_types: Annotated[list[str | None], config.BUFFERPOOL_SIZE] = [None] * config.BUFFERPOOL_SIZE
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
		table: 'Table' = next(table for table in self.tables if table.name == table_name)
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
		table: 'Table' = next(table for table in self.tables if table.name == table_name)
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
		table: 'Table' = next(table for table in self.tables if table.name == table_name)
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

	def get_version_col(self, table_name: str, record: Record, col_idx: DataIndex, relative_version: int) -> int | None:
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
			record = self.get_record(table_name, curr_rid, proj_col)
			curr_record = record
			assert curr_record is not None, "a record with a non-None RID was not found"
			curr_schema_encoding = curr_record.get_value().metadata.schema_encoding
			counter = 0
			overversioned = False
			assert curr_record is not None, "a record with a non-None RID was not found"
			curr_rid = curr_record.get_value().metadata.indirection_column
			while counter > relative_version or helper.ith_bit(curr_schema_encoding, table.num_columns, col_idx, False) == 0b0: # while not found
				assert curr_rid is not None
				temp = self.get_record(table_name, curr_rid, proj_col)
				if temp is None:
					overversioned = True
					break
				assert curr_record is not None
				curr_rid = temp.get_value().metadata.indirection_column
				curr_record = self.get_record(table_name, curr_rid, proj_col)
				curr_schema_encoding = curr_record.get_value().metadata.schema_encoding
				counter -= 1
			if overversioned is True:
				curr_record = record
			desired_col = curr_record.get_value()[col_idx]
		return desired_col
	def get_version_record(self, table_name: str, record_id: int, projected_columns_index: list[Literal[0] | Literal[1]], relative_version: int) -> Record | None:
		# table: Table = next(table for table in self.tables if table.name == table_name)

		# If there are multiple writers we probably need a lock here so the indirection column is not modified after we get it
		buffered_record = self.get_record(table_name, record_id, projected_columns_index)
		if buffered_record is None:
			return None
		columns: List[int | None] = []
		for i in range(len(projected_columns_index)):
			if projected_columns_index[i] == 1:
				columns.append(self.get_version_col(table_name, buffered_record.get_value(), DataIndex(i), relative_version))
			else:
				columns.append(None)
		version_record = Record(buffered_record.get_value().metadata, True, *columns)
		return version_record

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
		table: 'Table' = next(table for table in self.tables if table.name == table_name)
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
		table: 'Table' = next(table for table in self.tables if table.name == table_name)
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

