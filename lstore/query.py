from typing import Literal
from lstore.ColumnIndex import DataIndex, RawIndex
from lstore.file_handler import Table
from lstore.page_directory_entry import PageDirectoryEntry
from lstore.pseudo_buff_dict_value import Record
from lstore.index import Index
from lstore.base_tail_page import BasePage, Page, TailPage
import time
from lstore.config import WriteSpecifiedMetadata, config
from lstore.helper import helper
import struct


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """

    def __init__(self, table: Table):
        self.table = table
        self.db_bpool = table.db_bufferpool # The bufferpool is the same for every table, because there is only one
        pass
    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """

    def delete(self, primary_key: int) -> bool:
        """
        projected_columns_index = [1] * self.table.num_columns
        records = self.select(primary_key, self.table.key_index, projected_columns_index)
        assert len(records) == 1, "only one record should be returned with primary key"
        record = records[0]

        if (len(records) == 0):
            return False

        tmp = self.table.page_directory[record.rid]
        page = tmp.page
        offset = tmp.offset

        bitmask = 1 << (self.table.num_columns - config.RID_COLUMN)
        packed_data = struct.pack('>Q', bitmask)
        # Append the packed bytes to the bytearray
        page.physical_pages[config.NULL_COLUMN].data[offset*8:offset*8+8] = packed_data
        indirection_column = struct.unpack('>Q', page.physical_pages[config.INDIRECTION_COLUMN].data[offset*8:offset*8+8])[0]

        while indirection_column != 0:
            tmp = self.table.page_directory[indirection_column]
            page = tmp.page
            offset = tmp.offset

            packed_data = struct.pack('>Q', bitmask)
            # Append the packed bytes to the bytearray
            page.physical_pages[config.NULL_COLUMN].data[offset*8:offset*8+8] = packed_data
            indirection_column = struct.unpack('>Q', page.physical_pages[config.INDIRECTION_COLUMN].data[offset*8:offset*8+8])[0]
        """
        return self.update(primary_key, *([None] * self.table.num_columns), delete=True)
        # bitmask = 1 << (self.table.num_columns - config.RID_COLUMN - 1)
        # page_dir_entry = self.table.page_directory[record.rid]
        # page = page_dir_entry.page
        # offset = page_dir_entry.offset
        # page.physical_pages[config.NULL_COLUMN].data[offset*8:offset*8+8] = packed_data
        # indirection_column = struct.unpack('>Q', page.physical_pages[config.INDIRECTION_COLUMN].data[offset*8:offset*8+8])[0]
        # packed_data = struct.pack('>Q', bitmask)

        # return

    def insert_tail(self, indirection_column: int, schema_encoding: int,
                    *columns: int | None) -> int:  # returns RID if successful
        return -1
        # else:self.
        #     page = self.table.page_ranges[-1].tail_pages[-1]
        #     page_range = self.table.page_ranges[-1]

        # if not self.table.page_ranges[-1].base_pages[-1].has_capacity:
        #     self.table.page_ranges.append(PageRange(self.table.num_columns, self.table.key_index))
        #     rid = self.table.page_ranges[-1].base_pages[-1].insert(Metadata(None, self.table.last_rid, timestamp, schema_encoding), *columns)
        # rid = self.table.page_ranges[-1].base_pages[-1].insert(Metadata(None, self.table.last_rid, timestamp, schema_encoding), *columns)

        # if rid == -1:
        # pass

    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """

    def insert(self, *columns: int | None) -> bool:
        schema_encoding = 0b0
        timestamp = int(time.time())

        rid = self.table.file_handler.insert_record("base", WriteSpecifiedMetadata(None, 0b0, None), *columns)
        # print(f"rid: {rid}")

        # if rid == -1:
        #     self.table.page_ranges.append(PageRange(self.table.num_columns, self.table.key_index))
        #     rid = self.table.page_ranges[-1].base_pages[-1].insert(Metadata(None, self.table.last_rid, timestamp, schema_encoding), *columns)

        # TODO: update ALL columns with an index
        self.table.index.update_index(self.table.key_index,
                                      columns[self.table.key_index],
                                      rid)

        return True

    # gets the most up-to-date column value for a record.
    def get_updated_col(self, record: Record, col_idx: DataIndex) -> int | None:
        return -1
        
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """

    def select(self, search_key: int, search_key_index: DataIndex,
               projected_columns_index: list[Literal[0] | Literal[1]], use_idx: bool = False) -> list[Record] | Literal[-1]:
        return -1
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key: int, search_key_index: DataIndex, projected_columns_index: list[Literal[0, 1]],
                       relative_version: int) -> list[Record] | Literal[False]:
        return False 

    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """

    def update(self, primary_key: int, *columns: int | None, **kwargs: bool) -> bool:
        return False

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """

    def sum(self, start_range: int, end_range: int, aggregate_column_index: DataIndex) -> int | bool:
        return False

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """

    def sum_version(self, start_range: int, end_range: int, aggregate_column_index: DataIndex,
                    relative_version: int) -> int | bool:
        s = None

        valid_numbers: list[int] = []
        for key in range(start_range, end_range + 1):
            projected_cols: list[Literal[0, 1]] = [0] * (self.table.num_columns - 0)
            projected_cols[aggregate_column_index] = 1
            use_idx = True
            if aggregate_column_index == 1:
                use_idx = False
            select_query = self.select_version(key, self.table.key_index, projected_cols, relative_version)
            if select_query == False or len(select_query) == 0: continue
            assert len(select_query) == 1, "expected one for primary key but got " + str(len(select_query))
            record = select_query[0]
            if record.base_record == False:
                continue
            num = record[aggregate_column_index]
            assert num is not None
            valid_numbers.append(num)
            # valid_records.append(self.select(key, aggregate_column_index, projected_cols)[0])
        if len(valid_numbers) == 0:
            return False
        else: return sum(valid_numbers)


    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """

    def increment(self, key: int, column: DataIndex) -> bool:
        r = self.select(key, self.table.key_index, [1] * self.table.num_columns)[0] # type: ignore
        if r is not False:
            updated_columns: list[int | None] = [None] * self.table.num_columns
            to_add: int = 0
            rec_col = r[column]
            if rec_col is not None:
                to_add = rec_col
            updated_columns[column] = to_add + 1
            u = self.update(key, False, *updated_columns)
            return u
        return False