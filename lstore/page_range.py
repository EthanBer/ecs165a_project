from lstore.ColumnIndex import DataIndex
from lstore.base_tail_page import BasePage, TailPage
from lstore.config import config

class PageRange:
    def __init__(self, num_columns: int, key_col: DataIndex):
        self.base_pages: list[BasePage] = []
        self.tail_pages: list[TailPage] = []
        self.num_columns = num_columns
        self.base_pages.append(BasePage(self.num_columns, key_col))
        self.tail_pages.append(TailPage(self.num_columns, key_col))

    def append_tail_page(self, tail_page: TailPage) -> None:
        self.tail_pages.append(tail_page)

    def __str__(self) -> str:
        return f"""
{2 * config.INDENT}PageRange:
{3 * config.INDENT}base_pages:{config.str_each_el(self.base_pages)}
{3 * config.INDENT}tail_pages:{config.str_each_el(self.tail_pages)}"""
