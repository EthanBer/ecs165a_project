from typing import Literal
from lstore.bufferpool import PsuedoBuffIntValue, Table
from lstore.ColumnIndex import DataIndex, RawIndex
from lstore.page_directory_entry import BaseRID, PageDirectoryEntry, BasePageID
#from lstore.pseudo_buff_dict_value import Record
from lstore.record_physical_page import Record
from lstore.index import Index
#from lstore.base_tail_page import BasePage, Page, TailPage
import time
from lstore.config import WriteSpecifiedBaseMetadata, WriteSpecifiedTailMetadata, config
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
        self.db_bpool = table.db_bpool  # The bufferpool is the same for every table, because there is only one

    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """

    def delete(self, primary_key: int) -> bool:

        projected_columns_index : list[Literal[0, 1]] = [1] * self.table.num_columns
        records = self.select(primary_key, self.table.key_index, projected_columns_index)
        assert len(records) == 1, "only one record should be returned with primary key"
        record = records[0]
        
        bitmask = 0b1 << (self.table.num_columns - config.RID_COLUMN)

        rid = record.metadata.rid
        indirection_column = record.metadata.indirection_column

        # If record was already deleted
        if rid is None:
            return False
        
        # The record doesn't have tail records
        if indirection_column is None:
            assert isinstance(rid, BaseRID)
            new_null_column = record.metadata.null_column | bitmask
            self.db_bpool.update_col_record_inplace(self.table, rid, config.NULL_COLUMN, new_null_column)
            return True
        
        # Call update to create new tail record with everything None
        self.update(primary_key, [None]*self.table.num_columns) # type: ignore


        # after update we need to select again because the indirection column of the record has been updated
        records = self.select(primary_key, self.table.key_index, projected_columns_index)
        assert len(records) == 1, "only one record should be returned with primary key"
        record = records[0]

        indirection_column = record.metadata.indirection_column
        assert indirection_column is not None
        tmp = self.table.page_directory_buff[indirection_column] # This is the last tail page entry
        
        while tmp.page_type != "base":
            new_null_column = bitmask | record.metadata.null_column
            self.table.db_bpool.update_col_record_inplace(self.table, indirection_column, config.NULL_COLUMN, new_null_column)

            buf_record = self.db_bpool.get_record(self.table, indirection_column, [1]*self.table.num_columns)
            assert buf_record is not None
            record = buf_record.get_value()
            assert record is not None
            indirection_column = record.metadata.indirection_column
            assert indirection_column is not None
            tmp = self.table.page_directory_buff[indirection_column]

        return True


    """
    def insert_tail(self, indirection_column: int, schema_encoding: int,
                    *columns: int | None) -> int:  # returns RID if successful
        tail_page = page_range.tail_pages[-1]
        timestamp = int(time.time())

        if not tail_page.has_capacity():  # the last page of the tail page range can't handle another record
            tail_page = TailPage(self.table.num_columns, self.table.key_index)
            page_range.tail_pages.append(tail_page)

        key_null_bitmask = self.table.ith_total_col_shift(
            self.table.key_index.toRawIndex())  # this value for the null column makes the key column null

        rid = tail_page.insert(
            WriteSpecifiedMetadata(indirection_column, self.table.last_rid, timestamp, schema_encoding,
                                   key_null_bitmask), *columns)

        if rid == -1:
            raise (Exception("insert tail failed"))
            # return False

        page_directory_entry = PageDirectoryEntry(page_range, tail_page, tail_page.num_records - 1)
        self.table.page_directory_buff[rid] = page_directory_entry
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


    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns: int | None) -> bool:
        schema_encoding = 0b0
        timestamp = int(time.time())

        # the null column in this Metadata object won't be used by the page insert.
        # #print(f"trying to insert")
        # #print(f"trying to insert {columns}")
        rid = self.table.file_handler.insert_base_record(WriteSpecifiedBaseMetadata(None, 0b0, None),
                                                                     *columns)
        # print(f"trying to insert rid = {rid}")
        if rid == 511:
            print(f"rid 511 was {columns}")
        if rid == 512:
            print(f"rid 512 was {columns}")
        if rid == 513:
            print(f"rid 513 was {columns}")
        # #print(f"rid: {rid}")

        # if rid == -1:
        #     self.table.page_ranges.append(PageRange(self.table.num_columns, self.table.key_index))
        #     rid = self.table.page_ranges[-1].base_pages[-1].insert(Metadata(None, self.table.last_rid, timestamp, schema_encoding), *columns)


        self.table.index.update_index(self.table.key_index,
                                      columns[self.table.key_index],
                                      rid)

        return True


    # gets the most up-to-date column value for a record.
    def get_updated_col(self, record: Record, col_idx: DataIndex) -> int | None:
        return self.db_bpool.get_updated_col(self.table, record, DataIndex(col_idx))

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
               projected_columns_index: list[Literal[0] | Literal[1]], use_idx: bool = False) -> list[Record]:
        # search_key_index = DataIndex(search_key_index)
        # projected_columns_index = [DataIndex(idx) for idx in projected_columns_index]

        ret: list[Record] = []
        # if search_key is the key_index, then the rid is located by the index
        # otherwise, locate the rid manually
        # ...get the updated value for this rid
        valid_records: list[Record] = []
        rid: BaseRID
        if search_key_index == self.table.key_index:  # search_key_index == self.table.key_index
            rid = BaseRID(self.table.index.locate(search_key_index, search_key)) #type: ignore
            if rid is not None:
                record = self.db_bpool.get_updated_record(self.table, rid, [1] * self.table.num_columns)
                assert record is not None, "record was none even though rid was not none"
                valid_records.append(record)
                # #print(f"record cols was {valid_records[0].columns}")
        else:
            last_base_rid_buff = self.table.file_handler.next_base_rid
            for rid in [BaseRID(_) for _ in range(1, last_base_rid_buff.value())]:
            # for rid in helper.cast_list(range(1, last_base_rid_buff.value()), BaseRID()):
                record = self.db_bpool.get_updated_record(self.table, rid, [1] * self.table.num_columns)
                assert record is not None
                assert record.metadata.rid == rid
                assert record.metadata.rid is not None
                dir_entry = self.table.page_directory_buff[record.metadata.rid]
                if dir_entry.page_type != "base":
                    continue
                search_key_col = self.db_bpool.get_updated_col(self.table, record, DataIndex(search_key_index))
                # print(f"considering rid {rid}, its col was {search_key_col}. looking for {search_key}")
                if search_key_col == search_key:
                    valid_records.append(record)
            # last_base_rid_buff.flush()
        
        for record in valid_records:
            col_list = list(record.columns)
            # #print(f"col_list was {col_list}")
            schema_encoding = record.metadata.schema_encoding  # I think this is fine for this milestone because there is no concurrency
            for i in range(len(col_list)):
                if projected_columns_index[i] == 0:
                    col_list[i] = None
                else:
                    if record.metadata.rid == None:
                        continue
                    else:
                        # col_list[i] = self.get_updated_col(record, DataIndex(i))
                        col_list[i] = self.db_bpool.get_updated_col(self.table, record, DataIndex(i))
            record.columns = tuple(col_list)
            ret.append(record)

            # for i, column_value, data_index_indicator in enumerate(zip(rec.columns, projected_columns_index)):
            #     if data_index_indicator == 1:
            #         rec.columns[i] = column_value
            # return [rec]
            # else:
            #     return None
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
    # def select_version(self, search_key: int, search_key_index: DataIndex, projected_columns_index: list[Literal[0, 1]],
    #                    relative_version: int) -> list[Record] | Literal[False]:
    #     # search_key_index = DataIndex(search_key_index)
    #     # projected_columns_index = [DataIndex(idx) for idx in projected_columns_index]
    #     #  #print(self.table.page_directory.keys())
    #     ret: list[Record] = []
    #     # if search_key is the key_index, then the rid is located by the index
    #     # otherwise, locate the rid manually
    #     # ...get the updated value for this rid
    #     valid_records: list[Record] = []
        
    #     if search_key_index == self.table.key_index:
    #         # #print(search_key_index)
    #         # #print(search_key)
    #         rid = self.table.index.locate(search_key_index, search_key)
    #         if rid is not None:
    #             # raise(Exception("select should not be called on a key that doesn't exist"))
    #             valid_records.append(
    #                 helper.not_null(self.db_bpool.get_version_record(self.table, rid, [1] * self.table.num_columns, relative_version)))
    #     else:
    #         last_base_rid = PsuedoBuffIntValue(self.table.file_handler, "catalog", config.byte_position.catalog.LAST_BASE_RID)
    #         for rid in range(1, last_base_rid.value()):
    #             rec = helper.not_null(self.db_bpool.get_version_record(self.table, rid, [1] * self.table.num_columns, relative_version))
    #             assert rec.metadata.rid is not None
    #             dir_entry = self.table.page_directory_buff[rec.metadata.rid]
    #             if dir_entry.page_type != "base":
    #                 continue
    #             search_key_col = self.db_bpool.get_version_col(self.table, rec, search_key_index, relative_version)
    #             if search_key_col == search_key:
    #                 valid_records.append(rec)
    #             #     valid_records.append(rid)
        
    #     for record in valid_records:
    #         col_list = list(record.columns)
    #         schema_encoding = record.metadata.schema_encoding
    #         for i in range(len(col_list)):
    #             if projected_columns_index[i] == 0:
    #                 col_list[i] = None
    #             else:
    #                 if record.metadata.rid == None:
    #                     continue
    #                 else:
    #                     if helper.ith_bit(schema_encoding, self.table.num_columns, i,
    #                                       False) == 0b1:  # check if the column has been updated.
    #                         # #print("detected on schema encoding bit")
    #                         assert record.metadata.indirection_column is not None, "inconsistent state: schema_encoding bit on but indirection was None"
    #                         curr_rid = record.metadata.indirection_column
    #                         record1 = helper.not_null(self.db_bpool.get_updated_record(self.table, curr_rid, "tail", [
    #                             1] * self.table.num_columns))
    #                         curr_schema_encoding = record1.metadata.schema_encoding
    #                         counter = 0
    #                         overversioned = False
    #                         while counter > relative_version or helper.ith_bit(curr_schema_encoding,
    #                                                                            self.table.num_columns, i, False) == 0b0:
    #                             temp = self.db_bpool.get_updated_record(self.table, curr_rid, [1] * self.table.num_columns)
    #                             if temp is None:
    #                                 overversioned = True
    #                                 break
    #                             assert temp.metadata.indirection_column is not None, "looped back to base record? indirection_column == None"
    #                             curr_rid = temp.metadata.indirection_column
    #                             tmp_record = helper.not_null(self.db_bpool.get_updated_record(self.table, curr_rid, [
    #                                 1] * self.table.num_columns))
    #                             curr_schema_encoding = tmp_record.metadata.schema_encoding
    #                             counter -= 1
    #                             # curr_rid, curr_schema_encoding = temp.indirection_column, temp.schema_encoding
    #                             # curr_indirection = temp
    #                         if overversioned is True:
    #                             curr_rid = record.metadata.indirection_column
    #                         tmp = helper.not_null(self.db_bpool.get_updated_record(self.table, curr_rid, [1] * self.table.num_columns))
    #                         col_list[i] = tmp.columns[i]
    #                 # rec.columns[i] = None  # filter out that column from the projection
    #         if record.metadata.rid is None:
    #             continue
    #         curr_rid = helper.not_null(record.metadata.rid)
    #         record.columns = helper.not_null(self.db_bpool.get_version_record(self.table, curr_rid,[1] * self.table.num_columns,relative_version)).columns
    #         ret.append(record)

    #         # for i, column_value, data_index_indicator in enumerate(zip(rec.columns, projected_columns_index)):
    #         #     if data_index_indicator == 1:
    #         #         rec.columns[i] = column_value
    #         # return [rec]
    #         # else:
    #         #     return None
    #     # self.table.page_ranges.
    #     return ret



    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """    
    # TODO finish
    """
    """
    def update(self, primary_key: int, *columns: int | None, **kwargs: bool) -> bool:
        delete = kwargs.get("delete")
        if delete is None:
            delete = False
        
        assert len(
            columns) == self.table.num_columns, f"len(columns) must be equal to number of columns in table; argument had length {len(columns)} but expected {self.table.num_columns} length, cols was {columns}"
        if len(columns) != self.table.num_columns:
            return False
        primary_key_matches = self.select(primary_key, self.table.key_index, [1] * len(columns))
       
        assert len(
            primary_key_matches) == 1, f"only one primary key match for select, len was {len(primary_key_matches)}"

        if len(primary_key_matches) != 1:
            return False
        
        # select indirection and rid columns
        base_record = primary_key_matches[0]
        base_page_dir_entry = self.table.page_directory_buff[helper.not_null(base_record.metadata.rid)]
        #page_range = base_page_dir_entry.page_range
        # self.insert_tail(tail_page, )

        tail_1_values: list[int | None] = [None] * len(columns)
        tail_1_indirection: int = helper.not_null(base_record.metadata.rid) if base_record.metadata.indirection_column is None else base_record.metadata.indirection_column
        tail_1_schema_encoding = 0b1 << self.table.num_columns  # ..for now. we also need to take into account which columns were updated
        tail_schema_encoding = 0b0  # ..for now. we need to take into account updated columns
        tail_indirection: int = 0
        column_first_update: bool = False

        updated_columns: list[int | None] = [None] * self.table.num_columns
        if updated_columns == list(columns):  # all Nones were passed in, do nothing.
            return True
        # #print(f"columns passed were {columns}")
        for i, column in enumerate(columns):  # you can't change a value to None, unfortuntately.
            if column is not None:
                updated_columns[i] = column
                schema_shift = helper.ith_total_col_shift(self.table.num_columns, i, False)
                # schema_shift = 1 << (self.table.num_columns - i - 1)
                if helper.ith_bit(base_record.metadata.schema_encoding, self.table.num_columns, i, False) == 0b0:
                    tail_1_values[i] = base_record.columns[i]
                    if not column_first_update:
                        column_first_update = True
                    # tail_1_schema_encoding |= schema_shift
                tail_schema_encoding |= schema_shift
        if not delete:
            # if base_record.indirection_column is not None:
            #     tail_indirection = base_record.indirection_column
            # new_record_values.append([])
            # ... put tail record
            # if not tail_page.has_capacity(2 if first_update else 1):
            #     tail_page = TailPage(page_range, page_range.num_columns)
            #     page_range.append_tail_page(tail_page)
            
            # Create the null bitmask      
            null_bitmask = 0
            for value in columns:
                if value is None:
                    null_bitmask |= 1
                null_bitmask <<= 1
            # Right shift by one to remove the extra shift from the last iteration
            null_bitmask >>= 1

            if column_first_update:
                assert base_record.metadata.rid is not None
                tail_1_metadata = WriteSpecifiedTailMetadata(tail_1_indirection, tail_1_schema_encoding, null_bitmask, base_record.metadata.rid)
                tail_indirection = self.db_bpool.insert_tail_record(self.table, tail_1_metadata, *tail_1_values)
                
            else:
                # if not column_first_update, at least one bit in the schema encoding had to be a 1
                # in that case, we know the record should an indirection column, since it was updated
                assert isinstance(base_record.metadata.indirection_column,
                                  int), f"inconsistent state: expected base record's indirection column to be an integer but instead got {type(base_record.metadata.indirection_column)}. columns was {columns}"
                tail_indirection = base_record.metadata.indirection_column
            
                # if last_update_rid:
                #     last_update_page_dir_entry = self.table.page_directory[last_update_rid]
                # se:
                #     assert False, "brh"

                # prev_schema_encoding = self.table.get_record_by_rid(tail_indirection).schema_encoding

                # prev_schema_encoding = last_update_page_dir_entry["page"].get_nth_record(
                #     last_update_page_dir_entry["offset"]).schema_encoding
                # tail_schema_encoding |= prev_schema_encoding  # if first_update, these two should be the same. but if not then it might change

        else:
            tail_schema_encoding = 0b0
            tail_indirection = base_record.metadata.base_rid

            # curr = tail_indirection
            bitmask = self.table.ith_total_col_shift(config.RID_COLUMN)
            # bitmask = 1 << (self.table.total_columns - config.RID_COLUMN - 1) # this will go into the NULL_COLUMN; ie we are setting the RID to null
            # packed_data = struct.pack('>Q', bitmask)
            # # Append the packed bytes to the bytearray
            # page.physical_pages[config.NULL_COLUMN].data[offset*8:offset*8+8] = packed_data
            # indirection_column = struct.unpack('>Q', page.physical_pages[config.INDIRECTION_COLUMN].data[offset*8:offset*8+8])[0]
            tmp_indirection_col: int | None = base_record.metadata.indirection_column

            if tmp_indirection_col is not None:
                while True:
                    if tmp_indirection_col is None:
                        break

                    base_dir_entry = self.table.page_directory_buff[tmp_indirection_col]
                    page_id = base_dir_entry.page_id
                    offset = base_dir_entry.offset

                    # packed_data = struct.pack(config.PACKING_FORMAT_STR, bitmask)
                    # Append the packed bytes to the bytearray

                    self.db_bpool.delete_nth_record(self.table, page_id, offset)# the other bits in the null column no longer matter because they are deleted
                    #page.update_nth_record(offset, config.RID_COLUMN, 0b0)  # set the RID to null 

                    if base_dir_entry.page_type == "base":
                        break
                            

                    current_buffer_record=self.db_bpool.get_record(self.table,tmp_indirection_col,[1]*self.table.num_columns)
                    assert current_buffer_record is not None
                    current_record=current_buffer_record.get_value()
                    tmp_indirection_col=current_record.metadata.indirection_column
            
            
                    # page.physical_pages[config.NULL_COLUMN].data[offset*8:offset*8+8] = packed_data
                    #tmp_indirection_col=helper.unpack_data(page.physical_pages[config.INDIRECTION_COLUMN].data, offset)

                    #tmp_indirection_col = helper.unpack_col(page, config.INDIRECTION_COLUMN, offset)
                    # tmp_indirection_col = struct.unpack(config.PACKING_FORMAT_STR, page.physical_pages[config.INDIRECTION_COLUMN].data[offset*8:offset*8+8])[0]
                    
            else: #deleting base record
                assert base_record.metadata.rid is not None
                base_dir_entry = self.table.page_directory_buff[base_record.metadata.rid]
                self.db_bpool.delete_nth_record(self.table, BasePageID(base_record.metadata.base_rid), base_dir_entry.offset)# the other bits in the null column no longer matter because they are deleted
                #base_dir_entry.page_id.update_nth_record(base_dir_entry.offset, config.NULL_COLUMN, bitmask)
        
        #base_indirection = self.insert_tail(page_range, tail_indirection, tail_schema_encoding, *updated_columns)
        base_metadata = WriteSpecifiedMetadata(tail_indirection, tail_schema_encoding, null_bitmask)
        base_indirection = self.db_bpool.insert_tail_record(self.table, base_metadata, *updated_columns)
        
        #success = base_page_dir_entry.page_id.update_nth_record(base_page_dir_entry.offset, config.INDIRECTION_COLUMN,
        #                                                        base_indirection)
        
        success = self.db_bpool.update_nth_record(base_dir_entry.page_id, base_dir_entry.offset, config.INDIRECTION_COLUMN, base_indirection)
        assert success, "update not successful"

        #base_indirection = self.insert_tail(page_range, tail_indirection, tail_schema_encoding, *updated_columns)
        assert base_record.metadata.rid is not None
        tail_metadata = WriteSpecifiedTailMetadata(tail_indirection, tail_schema_encoding, null_bitmask, base_record.metadata.rid)
        base_indirection = self.db_bpool.insert_tail_record(self.table, tail_metadata, *updated_columns)
        
        
        # success = self.db_bpool.update_nth_record(base_dir_entry.page_id, base_dir_entry.offset, config.INDIRECTION_COLUMN, base_indirection)
        success = self.db_bpool.update_col_record_inplace(self.table, BaseRID(base_record.metadata.rid), config.INDIRECTION_COLUMN, base_indirection)
        
        
        assert success, "update not successful"
        base_schema_encoding = base_record.metadata.schema_encoding | tail_schema_encoding
        #success = base_page_dir_entry.page_id.update_nth_record(base_page_dir_entry.offset,
        #                                                        config.SCHEMA_ENCODING_COLUMN,
        #                                                        base_schema_encoding)
        
        success = self.db_bpool.update_col_record_inplace(self.table, BaseRID(base_record.metadata.rid), config.SCHEMA_ENCODING_COLUMN, base_schema_encoding)
        assert success, "update not successful"

        r = self.db_bpool.get_record(self.table, BaseRID(base_record.metadata.rid), [1] * self.table.num_columns)
        
        return success

    

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """

    def sum(self, start_range: int, end_range: int, aggregate_column_index: DataIndex) -> int | bool:
        s = None
        # #print("hi")
        # valid_records: list[Record] = []
        valid_numbers: list[int] = []

        # rids = self.table.index.locate_range(start_range, end_range, aggregate_column_index)
        # for rid in rids:
        #     valid_records.append(self.table.get_record_by_rid(rid))

        for key in range(start_range, end_range + 1):
            projected_cols: list[Literal[0, 1]] = [0] * self.table.num_columns
            projected_cols[aggregate_column_index] = 1
            use_idx = True
            if aggregate_column_index == 1:
                use_idx = False
            select_query = self.select(key, self.table.key_index, projected_cols)
            if len(select_query) == 0: continue
            assert len(select_query) == 1, "expected one for primary key"
            num = select_query[0][aggregate_column_index]
            assert num is not None
            valid_numbers.append(num)
            # valid_records.append(self.select(key, aggregate_column_index, projected_cols)[0])
        if len(valid_numbers) == 0:
            return False
        else:
            return sum(valid_numbers)

        # for record in valid_records:
        #     if s is None:
        #         s = 0
        #     to_add = record[aggregate_column_index]
        #     assert to_add is not None, "the thing to add was None inside sum"
        #     s += to_add
        # if s is None:
        #     return False
        # #print("byte")
        # for rid in rids:
        #     record = self.table.get_record_by_rid(rid)
        #     s += record[aggregate_column_index]
        # schema_encoding = record.schema_encoding
        # if helper.ith_bit(schema_encoding, self.table.num_columns, aggregate_column_index,
        #                   False) == 0b1:

        #     assert record.indirection_column is not None
        #     curr: int = record.indirection_column
        #     while self.table.get_record_by_rid(curr).columns[aggregate_column_index] == 0:
        #         temp = self.table.get_record_by_rid(curr).indirection_column
        #         curr = temp
        #     s += self.table.get_record_by_rid(record.indirection_column)[aggregate_column_index]
        return s


    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """

    # def sum_version(self, start_range: int, end_range: int, aggregate_column_index: DataIndex,
    #                 relative_version: int) -> int | bool:
    #     s = None

    #     valid_numbers: list[int] = []
    #     for key in range(start_range, end_range + 1):
    #         projected_cols: list[Literal[0, 1]] = [0] * (self.table.num_columns - 0)
    #         projected_cols[aggregate_column_index] = 1
    #         use_idx = True
    #         if aggregate_column_index == 1:
    #             use_idx = False
    #         select_query = self.select_version(key, self.table.key_index, projected_cols, relative_version)
    #         if select_query == False or len(select_query) == 0: continue
    #         assert len(select_query) == 1, "expected one for primary key but got " + str(len(select_query))
    #         record = select_query[0]
    #         assert record.metadata.rid is not None
    #         dir_entry = self.table.page_directory_buff[record.metadata.rid]
    #         if dir_entry.page_type != "base":
    #             continue
    #         num = record[aggregate_column_index]
    #         assert num is not None
    #         valid_numbers.append(num)
    #         # valid_records.append(self.select(key, aggregate_column_index, projected_cols)[0])
    #     if len(valid_numbers) == 0:
    #         return False
    #     else:
    #         return sum(valid_numbers)


    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key: int, column: DataIndex) -> bool:
        r = self.select(key, self.table.key_index, [1] * self.table.num_columns)[0]
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