from typing import List, Literal



from lstore.record_physical_page import PhysicalPage
from lstore.ColumnIndex import RawIndex

from lstore.page_directory_entry import PageID
class BufferpoolIndex(int):
	pass
class BufferpoolSearchResult:
	TNOT_FOUND = Literal[-1]
	def __init__(self, found: bool, data_buff_indices: List[None | TNOT_FOUND | BufferpoolIndex], metadata_buff_indices: List[TNOT_FOUND | BufferpoolIndex], record_offset: int | None):
		self.found = found
		self.data_buff_indices = data_buff_indices
		self.metadata_buff_indices = metadata_buff_indices
		self.record_offset = record_offset

class FilePageReadResult:
	def __init__(self, metadata_physical_pages: list[PhysicalPage | None], data_physical_pages: list[PhysicalPage | None], page_type: str): # should be only "base" or "tail" page type
		self.metadata_physical_pages = metadata_physical_pages
		self.data_physical_pages = data_physical_pages
		self.page_type = page_type

class FullFilePageReadResult:
	def __init__(self, metadata_physical_pages: list[PhysicalPage], data_physical_pages: list[PhysicalPage], page_type: str): # should be only "base" or "tail" page type
		self.metadata_physical_pages = metadata_physical_pages
		self.data_physical_pages = data_physical_pages
		self.page_type = page_type

class BufferpoolEntry:
	def __init__(self, pin_count: int, physical_page: PhysicalPage | None, dirty_bit: bool, physical_page_id: PageID | None, physical_page_index: RawIndex | None, page_type: str | None, table_name: str | None):
		# if pin_count != None: # None for pin_count will not change pin_count
		self.pin_count = pin_count 
		self.physical_page = physical_page
		self.dirty_bit = dirty_bit
		self.physical_page_id = physical_page_id
		self.physical_page_index = physical_page_index
		if page_type == "base" or page_type == "tail" or page_type == "metadata" or page_type is None:
			self.page_type = page_type
		else:
			raise(Exception("invalid page_type value passed to BufferpoolEntry"))
		self.table_name = table_name