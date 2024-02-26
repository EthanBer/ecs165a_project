
from lstore.record_physical_page import Record, PhysicalPage

import random
import struct
from lstore.record_physical_page import Record, PhysicalPage
from lstore.ColumnIndex import DataIndex, RawIndex
from lstore.config import config, WriteSpecifiedMetadata
from lstore.helper import helper


class Page:
    def __init__(self, num_columns: int, key_index : DataIndex):
        self.key_index = key_index
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

class TailPage(Page):
    def __str__(self) -> str:
        return Page.__str__(self)
    pass

class BasePage(Page):
    def __str__(self) -> str:
        return Page.__str__(self)

    # def __str__(self) -> str:
    #     return "OFJKJEF"
    pass

