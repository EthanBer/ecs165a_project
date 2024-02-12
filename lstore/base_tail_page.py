
from lstore.page import Page, PhysicalPage
from lstore.record_physical_page import Record


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

