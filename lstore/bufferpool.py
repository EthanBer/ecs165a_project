from lstore.page import Page
from lstore.config import config

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



class Bufferpool:
    #PATH = ".\Pages" # This should be the path to a folder where the files with the pages are going to be stored
    
    def __init__(self):
        self.max_size = 100
        self.bufferpool = {} # {page.id: bpoolPage}

        #self.dirty_pages = []


    def evict_dirty_page(self, page : bpoolPage) -> bool:
        # Here we can use struct to write the pages as bytearray (probably the best idea), in that case we need to use 'wb' in open
        with open(config.PATH + "\\" + str(page.id), 'wb') as file:
            for physical_page in Page.physical_pages:
                # we need to make sure that the page id and physical_page id are unique
                try:
                    file.write(physical_page.data)
                except:
                    return False
        return True
    

    def add_page(self, page: Page):

        if len(self.bufferpool) == self.size:
            self.evict_page_LRU
        
        bpage = bpoolPage(page)
        self.bufferpool.append(bpage)

    
    def pin_page(self, page: Page):
        for pg in pages
        


    """
    def pin_page(self, page: Page) -> bool:
        if (page in self.bpool) and (page not in self.pinned_pages):
            self.pinned_pages.append(page)
        else:
            return False
        return True
    
    def unpin_page(self, page: Page) -> bool:
        if (page in self.bpool) and (page in self.pinned_pages):
            self.pinned_pages.remove(page)
        else:
            return False
        return True
    
    def make_page_dirty(self, page: Page) -> bool:
        if (page in self.bpool) and (page not in self.dirty_pages):
            self.dirty_pages.append(page)
        else:
            return False
        return True
    
    """

    # This function implements the LRU
    def evict_page_LRU(self):
        # Remember that it has to be the LRU page that isnt pinned

        
