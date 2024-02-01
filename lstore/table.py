from lstore.index import Index
from time import time
from typing import TypedDict

from lstore.page import PageRange, Page


INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, key, indirection_column, schema_encoding, columns):
        global RID_COLUMN

        self.rid = RID_COLUMN
        self.key = key
        self.schema_encoding = schema_encoding
        self.indirection_column = indirection_column
        self.columns = columns
        RID_COLUMN += 1

    def __getitem__(self, key: int) -> int:
        # this syntax is used in the increment() function of query.py, so this operator should be implemented
        return self.columns[key]


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
        self.index = Index(self)

        self.page_ranges: list[PageRange] = []
        self.page_ranges.append(PageRange(self.num_columns))

    def __merge(self):
        print("merge is happening")
        pass
