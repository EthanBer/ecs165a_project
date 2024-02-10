from lstore.ColumnIndex import DataIndex
from lstore.base_tail_page import BasePage, TailPage

class PageRange:
    def __init__(self, num_columns: int, key_col: int):
        self.base_pages: list[BasePage] = []
        self.tail_pages: list[TailPage] = []
        self.num_columns = num_columns
        self.base_pages.append(BasePage(self.num_columns, key_col))
        self.tail_pages.append(TailPage(self.num_columns, key_col))

    def append_tail_page(self, tail_page: TailPage) -> None:
        self.tail_pages.append(tail_page)