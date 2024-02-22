from lstore.page import Page


class Bufferpool:
    PATH = "" # This should be the path to a folder where the files with the pages are going to be stored

    def __init__(self):
        self.size = 100
        self.bpool = []
        self.pinned_pages = []
        self.dirty_pages = []

    
    def evict_to_disk(self, page : Page) -> bool:
        # Here we can use struct to write the pages as bytearray (probably the best idea), in that case we need to use 'wb' in open

        for physical_page in Page.physical_pages:
            # we need to make sure that the page id and physical_page id are unique
            with open(self.PATH + "\\" + page.id + physical_page.id, 'w') as file:
                # bytearray stuff 
                
                try:
                    file.write(physical_page.data)
                except:
                    return False
        return True
    
    
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
    

    # This function implements the LRU
    def evict_page_LRU(self):
        # Remember that it has to be the LRU page that isnt pinned
        pass

    
    