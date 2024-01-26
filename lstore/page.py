
# class BasePage(Page):
#     ...

class PageRange:
    self.pages = []
    self.tail_pages: TailPage[]

class Page:

    def __init__(self):
        self.num_records = 0
        self.physical_pages: Page[]
        # len(self.pages) == num_columns


    def has_capacity(self):
        pass

    def write(self, value):
        self.num_records += 1
        pass

    def insert(self, *columns):
        for i, page in enumerate(pages):
            page.insert(columns[i])
    
    def update():

    def get_nth_record(self, n):
        # get nth record of this page
        pass

class PhysicalPage:
    ..
    self.size = 8192
    self.data = bytearray(self.size) 
    self.offset = 0
    def insert(self, column):
        offset += 64
        data[offset] = column
    def update()
