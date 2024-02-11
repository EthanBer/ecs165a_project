from lstore.ColumnIndex import DataIndex
from lstore.base_tail_page import BasePage, TailPage
from lstore.config import config

class PageRange:
    def __init__(self, num_columns: int, key_col: DataIndex, pages_per_range: int):
        assert pages_per_range >= 1, "pages per range must be >= 1"
        self.base_pages: list[BasePage] = []
        self.tail_pages: list[TailPage] = []
        self.num_columns = num_columns
        self.base_pages.append(BasePage(self.num_columns, key_col))
        self.tail_pages.append(TailPage(self.num_columns, key_col))
        self.pages_per_range = pages_per_range

    def append_tail_page(self, tail_page: TailPage) -> None:
        self.tail_pages.append(tail_page)

    # the number of tail pages per base page is unbounded, but there are only a 
    # certain amount of base pages per page range.
    def has_capacity(self) -> bool:
        return len(self.base_pages) <= (self.pages_per_range - 1)

    def __str__(self) -> str:
        return f"""
{2 * config.INDENT}PageRange:
{3 * config.INDENT}base_pages:{config.str_each_el(self.base_pages)}
{3 * config.INDENT}tail_pages:{config.str_each_el(self.tail_pages)}"""
