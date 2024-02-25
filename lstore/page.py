import random
import struct
from lstore.record_physical_page import Record, PhysicalPage
from lstore.ColumnIndex import DataIndex, RawIndex
from lstore.config import config, WriteSpecifiedMetadata
from lstore.helper import helper


class Page:
    def __init__(self, num_columns: int, key_index : DataIndex):
        self.key_index = key_index
        self.id = config.ID_COUNT 
        config.ID_COUNT += 1
        self.debugging_id = random.randint(0, 10^6)

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

        # # Create a file for the page
        # open(config.PATH + "\\" + str(self.id), 'wb')
        
                

   
    
    @property
    def high_level_str(self) -> str:
        return f"Page id {self.debugging_id} starting with RID{self.physical_pages[config.RID_COLUMN].__get_nth_record__(0)}"

    def __str__(self) -> str:
        newline = "\n"
        return f"""
{4 * config.INDENT}{self.high_level_str}; key_index:{self.key_index}; num_records:{self.num_records}; num_columns:{self.num_columns}
{5 * config.INDENT}physical_pages:{helper.str_each_el(self.physical_pages, newline + (5 * config.INDENT) + (15 * " "))}"""

    def has_capacity(self, n: int=1) -> bool:
        # if self.num_records
        # checks if we have capacity for n more records
        return (self.num_records * self.num_columns * 64) <= self.physical_page_size - (self.num_columns * 64 * n)


    # Returns -1 if there is no capacity in the page
    def insert(self, metadata: WriteSpecifiedMetadata, *columns: int | None) -> int:
        # NOTE: should follow same format as records, should return RID of successful record

        null_bitmask = 0
        total_cols = len(columns) + config.NUM_METADATA_COL
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
        list_columns = list(columns)
        list_columns.insert(config.INDIRECTION_COLUMN, metadata.indirection_column)
        list_columns.insert(config.RID_COLUMN, metadata.rid)
        list_columns.insert(config.TIMESTAMP_COLUMN, metadata.timestamp)
        list_columns.insert(config.SCHEMA_ENCODING_COLUMN, metadata.schema_encoding)
        list_columns.insert(config.NULL_COLUMN, null_bitmask)
        columns = tuple(list_columns)
        # print("COLUMNS with metadata")
        # print(columns)

        if (self.has_capacity == False):
            return -1

        for i in range(len(columns)):
            # print(f"inserting raw value {columns[i]}")
            self.physical_pages[i].insert(columns[i])
        # print(f"end record")

        self.num_records += 1
        # config.last_rid += 1
        
        return metadata.rid

    # def update(self):
    #     pass

    def get_nth_record(self, record_idx: int) -> Record:
        # get record at idx n of this page

        def get_check_for_none(col_idx: RawIndex, record_idx: int) -> int | None:
            val = self.physical_pages[col_idx].__get_nth_record__(record_idx)
            # # print("getting checking null")
            if val == 0:
                # breaking an abstraction barrier for convenience right now. TODO: fix?
                thing = helper.unpack_col(self, config.NULL_COLUMN, record_idx)
                # thing = struct.unpack(config.PACKING_FORMAT_STR, self.physical_pages[config.NULL_COLUMN].data[(record_idx * 8):(record_idx * 8)+8])[0]
                # is_none = (self.physical_pages[config.NULL_COLUMN].data[(record_idx * 8):(record_idx * 8)+8] == b'x01')

                # is_none = ( thing >> ( self.num_columns + config.NUM_METADATA_COL - col_idx - 1 ) ) & 1
                is_none = helper.ith_bit(thing, self.num_columns + config.NUM_METADATA_COL, col_idx)
                if is_none == 1:
                    # # print("set some value to None")
                    val = None
            return val

        indirection_column, rid, schema_encoding, timestamp, key_col, null_col = \
            get_check_for_none(config.INDIRECTION_COLUMN, record_idx), \
            get_check_for_none(config.RID_COLUMN, record_idx), \
            get_check_for_none(config.SCHEMA_ENCODING_COLUMN, record_idx), \
            get_check_for_none(config.TIMESTAMP_COLUMN, record_idx), \
            get_check_for_none(helper.data_to_raw_idx(self.key_index), record_idx), \
            get_check_for_none(config.NULL_COLUMN, record_idx)
        
        if rid is None or timestamp is None or schema_encoding is None or null_col is None:
            raise(Exception("rid or timestamp or schema_encoding or null_col was None when reading"))
        
        columns = []
        for i in range(config.NUM_METADATA_COL, config.NUM_METADATA_COL + self.num_columns):
            columns.append(self.physical_pages[i].__get_nth_record__(record_idx))
        
        from lstore.base_tail_page import BasePage
        return Record(WriteSpecifiedMetadata(indirection_column, rid, timestamp, schema_encoding, null_col), key_col, isinstance(self, BasePage), *columns)

    def update_nth_record(self, record_idx: int, update_col_idx: RawIndex, new_val: int) -> bool:
        self.physical_pages[update_col_idx].data[(record_idx * 8):(record_idx * 8)+8] = struct.pack(config.PACKING_FORMAT_STR, new_val)
        assert self.physical_pages[update_col_idx].__get_nth_record__(record_idx) == new_val, "update nth record failed"
        return self.physical_pages[update_col_idx].__get_nth_record__(record_idx) == new_val
