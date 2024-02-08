from typing import NewType
from lstore.record_physical_page import Record, PhysicalPage

from lstore.table import Record
from lstore.config import config
from lstore.ColumnIndex import RawIndex, DataIndex

global last_rid
last_rid = 0

class PageRange:
    def __init__(self, num_columns: int):
        self.num_records = 0
        self.physical_pages: list[PhysicalPage] = []
        # self.page_range = page_range
        self.num_columns = num_columns
        for _ in range(self.num_columns):
            page = PhysicalPage()
            self.physical_pages.append(page)

        # checking that all page sizes are the same:
        # https://stackoverflow.com/questions/3844801/check-if-all-elements-in-a-list-are-identical
        assert len(
            set(map(lambda physicalPage: physicalPage.size, self.physical_pages))) <= 1
        self.physical_page_size: int = self.physical_pages[0].size
        # assert len(self.physical_pages) == num_columns

    def has_capacity(self, n: int=1) -> bool:
        # if self.num_records
        # checks if we have capacity for n more records
        return (self.num_records * self.num_columns * 64) <= self.physical_page_size - (self.num_columns * 64 * n)


    # Returns -1 if there is no capacity in the page
    def insert(self, schema_encoding: int, indirection_column: int, key:int, *columns: int) -> int:
        # NOTE: should follow same format as records, should return RID of successful record
        record = Record(indirection_column, last_rid, schema_encoding, key, *columns)

        # Transform columns to a list to append the schema encoding and the indirection column
        print(columns)
        list_columns = list(columns)
        list_columns.insert(config.INDIRECTION_COLUMN, indirection_column)
        # list_columns.insert(config.TIMESTAMP_COLUMN, timestamp)    uncomment for next milestone
        list_columns.insert(config.RID_COLUMN, last_rid)
        list_columns.insert(config.SCHEMA_ENCODING_COLUMN, schema_encoding)
        list_columns.insert(config.KEY_COLUMN, key)
        columns = tuple(list_columns)

        if (self.has_capacity == False):
            return -1

        for i in range(len(columns)):
            self.physical_pages[i].insert(columns[i])

        self.num_records += 1
        last_rid += 1

        return record.rid

    # def update(self):
    #     pass


    def get_nth_record(self, record_idx: DataIndex) -> Record:
        # get record at idx n of this page
        if record_idx == -1:
            return self.physical_pages[-1].data[-8:]
        
        top_idx = record_idx // self.physical_page_size
        bottom_idx = record_idx % self.physical_page_size

        indirection_column = self.physical_pages[config.INDIRECTION_COLUMN].__get_nth_record__(record_idx)
        rid = self.physical_pages[config.RID_COLUMN].__get_nth_record__(record_idx)
        schema_encoding = self.physical_pages[config.SCHEMA_ENCODING_COLUMN].__get_nth_record__(record_idx)
        key = self.physical_pages[config.KEY_COLUMN].__get_nth_record__(record_idx)

        columns = []
        for i in range(4, 4 + self.num_columns):
            columns.append(self.physical_pages[i].__get_nth_record__(record_idx))
        
        return Record(indirection_column, rid, schema_encoding, key, *columns)






class PhysicalPage:

    def __init__(self) -> None:
        # self.size = 8192
        self.size = 4096
        self.data = bytearray(self.size)
        self.offset = 0

    def insert(self, value: int) -> None:
        # Pack the 64-bit integer into bytes (using 'Q' format for unsigned long long)
        packed_data = struct.pack('Q', value)
        # Append the packed bytes to the bytearray
        self.data[:len(packed_data)] = packed_data
        self.offset += 64


    def __get_nth_record__(self, record_idx: int) -> int:
        if record_idx == -1:
            return int.from_bytes(self.data[-8:], 'big')    # endianess = 'big'
    
        record_data=self.data[8*record_idx:8*record_idx+8]
        return int.from_bytes(record_data, 'big')

