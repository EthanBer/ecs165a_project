
import random
from lstore.record_physical_page import Record, PhysicalPage, DataIndex, RawIndex
from lstore.config import config

global last_rid
last_rid = 0


class Page:
    def __init__(self, num_columns: int, key_index : DataIndex):

        self.key_index = key_index
        self.id = random.randrange(1, int(1e10))

        self.num_records = 0
        self.physical_pages: list[PhysicalPage] = []

        # self.page_range = page_range
        self.num_columns = num_columns
        for _ in range(self.num_columns + config.NUM_METADATA_COL):
            page = PhysicalPage()
            self.physical_pages.append(page)

        # checking that all page sizes are the same:
        # https://stackoverflow.com/questions/3844801/check-if-all-elements-in-a-list-are-identical
        assert len(
            set(map(lambda physicalPage: physicalPage.size, self.physical_pages))) <= 1
        self.physical_page_size: int = self.physical_pages[0].size
        # assert len(self.physical_pages) == num_columns

    # def __str__(self) -> str:
        
    #     for physical_page in self.physical_pages:
            
    @property
    def high_level_str(self) -> str:
        return f"Page id {self.id} starting with RID{self.physical_pages[config.RID_COLUMN].__get_nth_record__(0)}"

    def __str__(self) -> str:
        newline = "\n"
        return f"""
{4 * config.INDENT}{self.high_level_str}; key_index:{self.key_index}; num_records:{self.num_records}; num_columns:{self.num_columns}
{5 * config.INDENT}physical_pages:{config.str_each_el(self.physical_pages, newline + (5 * config.INDENT) + (15 * " "))}"""

    def has_capacity(self, n: int=1) -> bool:
        # if self.num_records
        # checks if we have capacity for n more records
        return (self.num_records * self.num_columns * 64) <= self.physical_page_size - (self.num_columns * 64 * n)


    # Returns -1 if there is no capacity in the page
    def insert(self, timestamp: int, schema_encoding: int, indirection_column: int, key:int, *columns: int | None) -> int:
        # NOTE: should follow same format as records, should return RID of successful record
        last_rid = 0
        record = Record(indirection_column, last_rid, schema_encoding, key, *columns)

        # Transform columns to a list to append the schema encoding and the indirection column
        print(columns)
        list_columns = list(columns)
        list_columns.insert(config.INDIRECTION_COLUMN, indirection_column)
        list_columns.insert(config.TIMESTAMP_COLUMN, timestamp)
        list_columns.insert(config.RID_COLUMN, last_rid)
        list_columns.insert(config.SCHEMA_ENCODING_COLUMN, schema_encoding)
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
        


    def get_nth_record(self, record_idx: int) -> Record:
        # get record at idx n of this page
        indirection_column = self.physical_pages[config.INDIRECTION_COLUMN].__get_nth_record__(record_idx)
        rid = self.physical_pages[config.RID_COLUMN].__get_nth_record__(record_idx)
        schema_encoding = self.physical_pages[config.SCHEMA_ENCODING_COLUMN].__get_nth_record__(record_idx)
        timestamp = self.physical_pages[config.TIMESTAMP_COLUMN].__get_nth_record__(record_idx)
        key_col = self.physical_pages[self.key_index].__get_nth_record__(record_idx)
        
        columns = []
        for i in range(config.NUM_METADATA_COL, config.NUM_METADATA_COL + self.num_columns):
            columns.append(self.physical_pages[i].__get_nth_record__(record_idx))
        
        return Record(indirection_column, rid, schema_encoding, self.key_index, *columns)
