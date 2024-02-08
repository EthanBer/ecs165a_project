from lstore.base_tail_page import BasePage, TailPageç

class PageRange:
    def __init__(self, num_columns: int):
        self.base_pages: list[BasePage] = []
        self.tail_pages: list[TailPage] = []
        self.num_columns = num_columns
        self.base_pages.append(BasePage(self.num_columns))
        self.tail_pages.append(TailPage(self.num_columns))

    def append_tail_page(self, tail_page: TailPage) -> None:
        self.tail_pages.append(tail_page)