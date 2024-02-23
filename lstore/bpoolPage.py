from lstore.page import Page


class bpoolPage:

    def __init__(self, page: Page):
        self.page = page
        self.pin_count = 1
        self.dirty = False

    def pin(self):
        self.pin_count += 1
    
    def make_dirty(self):
        self.dirty = True

    def __del__(self):
        self.pin_count -= 1