from typing import NewType
from lstore.record_physical_page import Record, PhysicalPage

DataPositionIndex = NewType('DataPositionIndex', int)
RawPositionIndex = NewType('RawPositionIndex', int)

class Page:
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

    def insert(self, schema_encoding: int, indirection_column: int, *columns: int) -> int:
        # NOTE: should follow same format as records, should return RID of successful record

        print("hi")
        record = Record(columns[0], indirection_column,
                        schema_encoding, *columns[1:])

        # Transform columns to a list to append the schema encoding and the indirection column
        print(columns)
        list_columns = list(columns)
        list_columns.append(schema_encoding)
        list_columns.append(0)
        columns = tuple([indirection_column, ])
        columns = tuple(list_columns)

        if (self.has_capacity == False):
            return -1

        for i in range(len(columns)):
            self.physical_pages[i].insert(columns[i])

        self.num_records += 1

        return record.rid

    # def update(self):
    #     pass

    # def get_nth_record(self, record:)
    # def get_nth_record(self, record_idx: int) -> Record:
    #     pass
    #     pass
        # # get record at idx n of this page
        # if record_idx == -1:
        #     return self.physical_pages[-1].data[-4:]
        #     #return Record()
        # top_idx = record_idx // self.physical_page_size
        # bottom_idx = record_idx % self.physical_page_size
        # return Record(self.table.key)
        # return self.physical_pages[top_idx].__get_nth_record__(bottom_idx)


