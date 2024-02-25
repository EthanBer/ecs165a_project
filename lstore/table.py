from time import time
from typing import Literal, TypedDict
from lstore.bufferpool_ import PseudoBuffDictValue, BufferedValue, FileHandler, MetadataPageID, PageID
from lstore.helper import helper
from lstore.base_tail_page import BasePage
from lstore.config import config
from lstore.ColumnIndex import DataIndex, RawIndex

from lstore.page import Page
from lstore.page_range import PageRange
from lstore.record_physical_page import Record
# from lstore.ColumnIndex import RawIndex, DataIndex

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
SCHEMA_ENCODING_COLUMN = 2


class PageDirectoryEntry:
    def __init__(self, page_id: PageID, metadata_page_id: MetadataPageID, offset: int, page_type: Literal["base", "tail"]):
        self.page_id = page_id
        self.metadata_page_id = metadata_page_id
        self.offset = offset
        self.page_type = page_type

    # @property
    # def high_level_str(self) -> str:
    #     return f"({self.page.high_level_str}, {self.offset})"

    def __str__(self) -> str:
        return f"({self.page_id}, {self.offset})"

PageDirectory = dict[int, PageDirectoryEntry]
class Table:
    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def __init__(self, name: str, num_columns: int, key_index: DataIndex, pages_per_range: int):
        self.name: str = name
        self.key_index = DataIndex(key_index)
        self.num_columns: int = num_columns # data columns only
        self.total_columns = self.num_columns + config.NUM_METADATA_COL # inclding metadata
        self.file_handler = FileHandler(self)
        self.page_directory_buff = PseudoBuffDictValue[int, PageDirectoryEntry](self.file_handler, "page_directory")
        # self.last_rid = 1
        self.pages_per_range = pages_per_range

        # ## second milestone
        # self.last_physical_page_id=None
        # self.last_tail_id=None  
        # ####
        
        
        # Page Directory:
        # {Rid: (Page, offset)}
        from lstore.index import Index
        self.index = Index(self)

        self.page_ranges: list[PageRange] = []
        self.page_ranges.append(PageRange(self.num_columns, self.key_index, self.pages_per_range))
        # create a B-tree index object for the key index (hard-coded for M1)
        self.index.create_index(self.key_index)
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
    #     print("merge is happening")
    #     pass
