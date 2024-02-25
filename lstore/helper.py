from __future__ import annotations
import struct
import typing
from lstore.ColumnIndex import DataIndex, RawIndex
from lstore.config import config


if typing.TYPE_CHECKING:
	from lstore.page import Page
class helper:
	@staticmethod
	def str_each_el(arr: list, delim: str="") -> str:
		return delim.join([str(el) for el in arr])

	@staticmethod
	def data_to_raw_idx(idx: DataIndex) -> RawIndex:
		return RawIndex(idx + config.NUM_METADATA_COL)

	@staticmethod
	def unpack_data(data: bytearray, record_idx: int) -> int: # idx is the byte index of desired data
		# # print(f"data{data}", record_idx)
		return struct.unpack(config.PACKING_FORMAT_STR, data[record_idx*8:record_idx*8+8])[0]
	
	@staticmethod
	def unpack_col(page: 'Page', col_idx: RawIndex, record_idx: int) -> int: # col_idx is the index of the desired physical page in which the data is stored
		return helper.unpack_data(page.physical_pages[col_idx].data, record_idx)

	@staticmethod
	def ith_total_col_shift(total_cols: int, col_idx: RawIndex | int, total_cols_is_total_table_cols: bool = True) -> int: # returns the bit vector shifted to the indicated col idx
		# check that col_idx is actually a RawIndex
		assert True if not total_cols_is_total_table_cols else isinstance(col_idx, RawIndex), f"if the number of total columns is the number of table columns, the col_idx should be a RawIndex. instead it was a {type(col_idx)}."
		return 0b1 << (total_cols - col_idx - 1)

	@staticmethod
	def ith_bit(bit_vector: int, total_cols: int, col_idx: RawIndex | int, total_cols_is_total_table_cols: bool = True) -> int: # returns the bit vector shifted to the indicated col idx
		# check that col_idx is actually a RawIndex
		assert True if not total_cols_is_total_table_cols else isinstance(col_idx, RawIndex), f"if the number of total columns is the number of table columns, the col_idx should be a RawIndex. instead it was a {type(col_idx)}."
		return ( bit_vector >> ( total_cols - col_idx - 1 ) ) & 0b1
		# s = ""
		# for el in arr:
		# 	s += el.__str__()
		# return s

	# helper function to type cast list 
	# https://www.geeksforgeeks.org/python-type-casting-whole-list-and-matrix/
	@staticmethod
	def cast_list(test_list, data_type):
		return list(map(data_type, test_list))
     