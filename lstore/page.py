
from lstore.record_physical_page import Record, PhysicalPage, DataIndex, RawIndex
from lstore.config import config

global last_rid
last_rid = 0


class Page:
    def __init__(self, num_columns: int, key : DataIndex):

        self.key = key

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

    def has_capacity(self, n: int=1) -> bool:
        # if self.num_records
        # checks if we have capacity for n more records
        return (self.num_records * self.num_columns * 64) <= self.physical_page_size - (self.num_columns * 64 * n)


    # Returns -1 if there is no capacity in the page
    def insert(self, timestamp: int, schema_encoding: int, indirection_column: int, key:int, *columns: int) -> int:
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
        key_col = self.physical_pages[self.key].__get_nth_record__(record_idx)
        
        columns = []
        for i in range(config.NUM_METADATA_COL, config.NUM_METADATA_COL + self.num_columns):
            columns.append(self.physical_pages[i].__get_nth_record__(record_idx))
        
        return Record(indirection_column, rid, schema_encoding, self.key, *columns)
