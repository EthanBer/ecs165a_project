from time import time
from typing import TypedDict

from lstore.page import Page
from lstore.page_range import PageRange
from lstore.record_physical_page import Record
# from lstore.ColumnIndex import RawIndex, DataIndex

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
SCHEMA_ENCODING_COLUMN = 2


class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def __init__(self, name: str, num_columns: int, key: int):
        self.name: str = name
        self.key: int = key
        self.num_columns: int = num_columns
        class PageDirectoryEntry(TypedDict):
            page: Page
            offset: int
        self.page_directory: dict[int, PageDirectoryEntry] = {}
        # Page Directory:
        # {Rid: (Page, offset)}
        from lstore.index import Index
        self.index = Index(self)

        self.page_ranges: list[PageRange] = []
        self.page_ranges.append(PageRange(self.num_columns, key))

    def get_record_by_rid(self, rid: int) -> Record:
        page_dir_entry = self.page_directory[rid]
        return page_dir_entry["page"].get_nth_record(
                page_dir_entry["offset"])
    # def __merge(self):
    #     print("merge is happening")
    #     pass
