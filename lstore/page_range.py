from lstore.ColumnIndex import DataIndex
from lstore.base_tail_page import BasePage, TailPage
from lstore.config import config
from lstore.helper import helper
import threading
import os 
class PageRange:
    def __init__(self, num_columns: int, key_col: DataIndex, pages_per_range: int):
        assert pages_per_range >= 1, "pages per range must be >= 1"
        self.base_pages: list[BasePage] = []
        self.tail_pages: list[TailPage] = []
        self.num_columns = num_columns
        self.base_pages.append(BasePage(self.num_columns, key_col))
        self.tail_pages.append(TailPage(self.num_columns, key_col))
        self.pages_per_range = pages_per_range
        #milestone 2 
        self.updates_count=0
        self.path= None 
        self.table_name= None 



    def append_tail_page(self, tail_page: TailPage) -> None:
        self.tail_pages.append(tail_page)
    def bring_base_pages_to_memory(self)->list[BasePage]:
        list_base_pages=[]
        try:
            os.mkdir(self.path)
        except:  #the db is empty so there are no tables to load to bufferpool
            pass
        for table_name in os.listdir(self.path):
            if self.table_name==table_name:
                table_path=os.path.join(self.path,table_name)
                catalog_path = os.path.join(self.path,"catalog")
                with open(catalog_path, 'rb') as catalog:
                    #get the catalog to create the table 
                    table_num_columns= int.from_bytes(catalog.readline())
                    table_key_index= int.from_bytes(catalog.readline())
                    table_pages_per_range = int.from_bytes(catalog.readline())
                    table_last_page_id = int.from_bytes(catalog.readline())
                    table_last_tail_id= int.from_bytes(catalog.readline())
                    table_last_rid= int.from_bytes(catalog.readline())
                for file in os.listdir(table_path):
                    if file =="base*":
                        page_id=int(file.split("_")[1]) # take the page id, may not work :( 
                        page= BasePage(table_num_columns, DataIndex(table_key_index))
                        page.id=page_id

                        page_path = os.path.join(table_path,page_id)
                        with open(page_path, "rb") as page_file:
                            metadata_id= int(page_file.read(8))
                            metadata_path=os.path.join(table_path,metadata_id)
                            with open(metadata_path, "rb") as metadata_file:
                                metadata_file.read(8)
                                

        


        return list_base_pages

    def merge(self):
        



        pass

    def increase_update_count(self):
        self.updates_count=+1
        if self.updates_count>config.UPDATES_BEFORE_MERGE:
            merge_thread = threading.Thread(self.merge(), daemon=True)
            merge_thread.start()
        self.updates_count=0 
    
    # the number of tail pages per base page is unbounded, but there are only a 
    # certain amount of base pages per page range.
    def has_capacity(self) -> bool:
        return len(self.base_pages) <= (self.pages_per_range - 1)

    def __str__(self) -> str:
        return f"""
{2 * config.INDENT}PageRange:
{3 * config.INDENT}base_pages:{helper.str_each_el(self.base_pages)}
{3 * config.INDENT}tail_pages:{helper.str_each_el(self.tail_pages)}"""
