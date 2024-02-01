from lstore.table import Table, Record
from lstore.index import Index
from lstore.page import Page
from lstore.base_tail_page import TailPage


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

    def delete(self, primary_key):
        pass

    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """

    def insert(self, *columns: int):
        schema_encoding = '0' * self.table.num_columns
        # should make a new RID
        # make a new record (Record class)
        # Page Directory:
        # {Rid: (Page, offset)}

        if len(self.table.page_directory) == 0:
            page = Page(self.table.page_ranges[0], self.table.num_columns)
            self.table.page_ranges[0].base_pages.append(page)

        elif not self.table.page_ranges[-1].base_pages[-1].has_capacity():
            page = Page(self.table.page_ranges[-1], self.table.num_columns)
            self.table.page_ranges[-1].base_pages.append(page)

        rid = self.table.page_ranges[-1].base_pages[-1].insert(
            schema_encoding, -1, *columns)
        self.table.page_directory[rid] = [page, page.num_records]

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

    def select(self, search_key: int, search_key_index: int, projected_columns_index: list[int]) -> list[Record]:
        return []
        pass

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

    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        true_index = search_key_index + 4
        pass

    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """

    def update(self, primary_key: int, *columns: int) -> bool:
        assert len(
            columns) == self.table.num_columns, "len(columns) must be equal to number of columsn in table"
        if len(columns) != self.table.num_columns:
            return False
        INDIRECTION_COLUMN = 0
        RID_COLUMN = 1
        primary_key_matches = self.select(primary_key, self.table.key, [
                                          1, 1] + ([0] * (len(columns) - 2)))
        if len(primary_key_matches) == 0:
            return False
        assert len(primary_key_matches) == 1
        # select indirection and rid columns
        base_record = primary_key_matches[0]
        page_range = self.table.page_directory[base_record.rid][0].page_range
        tail_page = page_range.tail_pages[-1]
        first_update = False
        tail_1_values: list[int | None] = []
        tail_1_indirection: int = 0
        tail_1_schema_encoding: int = 0
        last_update_rid = None
        # the first bit is a flag, specifying whether this tail record is a snapshot of original base page values or an updated value

        if base_record.indirection_column == None:  # this record hasn't been updated before
            first_update = True
            # will set the key to none, wherever it is
            tail_1_values = [None] * len(columns)
            tail_1_indirection = base_record.rid
            # ..for now. we also need to take into account which columns were updated
            tail_1_schema_encoding = 1 << self.table.num_columns
        else:
            last_update_rid = base_record.indirection_column
            # new_record_values.append([])
            # ... put tail record
        tail_schema_encoding = 0  # ..for now. we need to take into account updated columns
        if not tail_page.has_capacity(2 if first_update else 1):
            tail_page = TailPage(page_range, page_range.num_columns)
            page_range.append_tail_page(tail_page)

        updated_columns: list[int | None] = [None] * self.table.num_columns
        for i, column in enumerate(reversed(columns)):
            if column is not None:
                updated_columns[i] = column
                if first_update:
                    tail_1_values[i] = column
                    tail_1_schema_encoding |= (1 << i)
                tail_schema_encoding |= (1 << i)

        if first_update:
            tail_indirection = tail_page.insert(
                tail_1_indirection, tail_1_schema_encoding, tail_1_values)
        else:
            tail_indirection = base_record.rid
            if last_update_rid:
                last_update_page_dir_entry = self.table.page_directory[last_update_rid]
            else:
                assert False, "brh"
            prev_schema_encoding = last_update_page_dir_entry[0].get_nth_record(
                last_update_page_dir_entry[1]).schema_encoding
            tail_schema_encoding |= prev_schema_encoding

        tail_page.insert(tail_indirection,
                         tail_schema_encoding, updated_columns)
        return True

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
        pass

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        pass

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
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
