from lstore.ColumnIndex import DataIndex
from lstore.table import PageDirectoryEntry, Table, Record
from lstore.index import Index
from lstore.page import Page
from lstore.base_tail_page import BasePage, TailPage
import time
from lstore.config import Metadata, config
import struct
from lstore.page_range import PageRange

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """

    def __init__(self, table: Table):
        self.table = table
        pass

    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """

    def delete(self, primary_key: int) -> bool:
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

        return True
        

    def insert_tail(self, page_range: PageRange, indirection_column: int, schema_encoding: int, *columns: int | None) -> int: # returns RID if successful
        tail_page = page_range.tail_pages[-1]
        timestamp = int(time.time())

        if not tail_page.has_capacity(): # the last page of the tail page range can't handle another record
            tail_page = TailPage(self.table.num_columns, self.table.key_index)
            page_range.tail_pages.append(tail_page)

        rid = tail_page.insert(Metadata(indirection_column, self.table.last_rid, timestamp, schema_encoding), *columns)

        if rid == -1:
            raise(Exception("insert tail failed"))
            # return False

        page_directory_entry = PageDirectoryEntry(page_range, tail_page, tail_page.num_records - 1)
        self.table.page_directory[rid] = page_directory_entry
        self.table.last_rid += 1
        return rid

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
        schema_encoding = 0
        timestamp = int(time.time())

        page: BasePage | None = None
        page_range: PageRange | None = None
        if not self.table.page_ranges[-1].base_pages[-1].has_capacity(): # the last page of the page range is full
            if not self.table.page_ranges[-1].has_capacity(): # the page range can't handle another page, so make a new range. this range implicitly makes a new page as well
                self.table.page_ranges.append(PageRange(self.table.num_columns, self.table.key_index, self.table.pages_per_range))
            else: # the last page of the range was full, but the page range can accomodate another page, so make one
                page = BasePage(self.table.num_columns, self.table.key_index)
                page_range = self.table.page_ranges[-1]
                self.table.page_ranges[-1].base_pages.append(page)
        else: # the last page of the range can fit another record. don't do any new allocation.
            page = self.table.page_ranges[-1].base_pages[-1]
            page_range = self.table.page_ranges[-1]

        if page is None or page_range is None:
            return False

        rid = self.table.page_ranges[-1].base_pages[-1].insert(Metadata(None, self.table.last_rid, timestamp, schema_encoding), *columns)
        
        # if rid == -1:
        #     self.table.page_ranges.append(PageRange(self.table.num_columns, self.table.key_index))
        #     rid = self.table.page_ranges[-1].base_pages[-1].insert(Metadata(None, self.table.last_rid, timestamp, schema_encoding), *columns)

        if rid == -1:
            raise(Exception("insert failed"))

        page_directory_entry=PageDirectoryEntry(page_range, page, page.num_records - 1)
        self.table.page_directory[rid] = page_directory_entry
        self.table.last_rid += 1

        #self.table.index.update_index()
        return True

    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """

    def select(self, search_key: int, search_key_index: int | DataIndex, projected_columns_index: list[int | DataIndex]) -> list[Record]:
        search_key_index = DataIndex(search_key_index)
        projected_columns_index = [DataIndex(idx) for idx in projected_columns_index]

        ret: list[Record] = []
        for rid in range(1, self.table.last_rid):
            rec = self.table.get_record_by_rid(rid)
            col_list = list(rec.columns)
            if rec.columns[search_key_index] == search_key:
                for i, col in enumerate(col_list):
                    if projected_columns_index[i] == 0:
                        col_list[i] = None 
                        # rec.columns[i] = None  # filter out that column from the projection
                rec.columns = tuple(col_list)
                ret.append(rec)
        # self.table.page_ranges.
        return ret 

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

    # def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
    #     true_index = search_key_index + 4
    #     pass

    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """

    def update(self, primary_key: int, *columns: int) -> bool:
        assert len(
            columns) == self.table.num_columns, "len(columns) must be equal to number of columns in table"
        if len(columns) != self.table.num_columns:
            return False
        primary_key_matches = self.select(primary_key, self.table.key_index, [1] * len(columns))
        if len(primary_key_matches) == 0:
            return False
        assert len(primary_key_matches) == 1 # primary key results in ONE select result
        # select indirection and rid columns
        base_record = primary_key_matches[0]
        base_page_dir_entry = self.table.page_directory[base_record.rid]
        page_range = base_page_dir_entry.page_range
        # self.insert_tail(tail_page, )
        
        tail_1_values: list[int | None] = []
        tail_1_indirection: int = 0
        tail_1_schema_encoding: int = 0
        tail_indirection: int
        first_update = False

        if base_record.indirection_column is None:  # this record hasn't been updated before
            first_update = True
            # will set the key to none, wherever it is
            tail_1_values = [None] * len(columns)
            tail_1_indirection = base_record.rid
            # the first bit is a flag, specifying whether this tail record is a snapshot of original base page values or an updated value
            tail_1_schema_encoding = 1 << self.table.num_columns # ..for now. we also need to take into account which columns were updated
        else:
            tail_indirection = base_record.indirection_column
            # new_record_values.append([])
            # ... put tail record
        tail_schema_encoding = 0  # ..for now. we need to take into account updated columns
        # if not tail_page.has_capacity(2 if first_update else 1):
        #     tail_page = TailPage(page_range, page_range.num_columns)
        #     page_range.append_tail_page(tail_page)

        updated_columns: list[int | None] = [None] * self.table.num_columns
        for i, column in enumerate(columns):
            if column is not None:
                updated_columns[i] = column
                schema_shift = 1 << (self.table.num_columns - i - 1)
                if first_update:
                    tail_1_values[i] = base_record.columns[i]
                    # tail_1_schema_encoding |= schema_shift
                tail_schema_encoding |= schema_shift

        if first_update:
            tail_indirection = self.insert_tail(page_range, tail_1_indirection, tail_1_schema_encoding, *tail_1_values)
        else:
            if not isinstance(base_record.indirection_column, int): return False
            tail_indirection = base_record.indirection_column
            # if last_update_rid:
            #     last_update_page_dir_entry = self.table.page_directory[last_update_rid]
            # se:
            #     assert False, "brh"

            prev_schema_encoding = self.table.get_record_by_rid(tail_indirection).schema_encoding

            # prev_schema_encoding = last_update_page_dir_entry["page"].get_nth_record(
            #     last_update_page_dir_entry["offset"]).schema_encoding
            tail_schema_encoding |= prev_schema_encoding # if first_update, these two should be the same. but if not then it might change


        base_indirection = self.insert_tail(page_range, tail_indirection, tail_schema_encoding, *updated_columns)
        success = base_page_dir_entry.page.update_nth_record(base_page_dir_entry.offset, config.INDIRECTION_COLUMN, base_indirection)
        assert success, "update not successful"

        return success # True if update was successful

        # tail_1_indirection = rid if first_update else last_update_rid

        # if first update,
        # indirection of first tail = base record being updated
        # schema encoding of first tail = 1 + whatever columns were updated (1 is the snapshot flag)
        # key of first tail = null (does not directly participate in search queries)
        # otherwise,
        # indirection of tail = indirection of base column (should be non-null)
        # schema encoding of tail = whatever columns were updated in this tail record bitwiseOR schema encoding of tail record with RID = indirection of base column
        # key of tail = null (does not directly participate in search queries)

        # check if a tail page is allocated:
        # if allocated, and it has enough space for (first time updating ? 2 : 1) tail records, great!
        # if one of the above not true, allocate a new tail page. this should be added
        # we now have the tail page that we can append to

        # append (first update ? 2 : 1) tail records to given tail page

        # for i, column in enumerate(columns):
        #   if column is not None:
        #       updated_columns[i] = column
        # tail_page.insert(updated_columns)

        # indirection of tail = base record being updated (self.select(primary_key, self.table.key, [0, 1] + ([0] * len(columns) - 2))[0][RID_COLUMN])
        # schema encoding of tail = (schema encoding with updated columns for this tail) OR (schema encoding of previous tail record)
        # key of tail = null (does not directly participate in search queries)
        # get only indirectoin and base columns
        # for column in columns:
        #     if column is not None:

        # indirection of base = RID of tail

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        s = 0
        for rid in range(start_range, end_range + 1):
            s = self.table.get_record_by_rid(rid).columns[aggregate_column_index]
        if s:
            return s
        return 0

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """

    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        pass

    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """

    def increment(self, key, column):
        r = self.select(key, self.table.key_index, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
