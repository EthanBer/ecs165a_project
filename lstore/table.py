from time import time
from typing import Literal, TypedDict
from lstore.bufferpool import BufferedDictValue, BufferedValue, FileHandler, MetadataPageID, PageID, PseudoBuffDictValue
from lstore.helper import helper
#from lstore.base_tail_page import BasePage
from lstore.config import config
from lstore.ColumnIndex import DataIndex, RawIndex

import os 
#from lstore.page import Page
from lstore.page_range import PageRange
from lstore.record_physical_page import Record
# from lstore.ColumnIndex import RawIndex, DataIndex
import threading


INDIRECTION_COLUMN = 0
RID_COLUMN = 1
SCHEMA_ENCODING_COLUMN = 2
#mutex_lock= threading.Lock()


class PageDirectoryEntry:
    def __init__(self, page_id: PageID, metadata_page_id: MetadataPageID, offset: int, page_type: Literal["base", "tail"]):
        self.page_id = page_id
        self.metadata_page_id = metadata_page_id
        self.offset = offset
        self.page_type = page_type

    # @property
    # def high_level_str(self) -> str:
    #     return f"({self.page.high_level_str}, {self.offset})"

    def __str__(self) -> str:
        return f"({self.page_id}, {self.offset})"

PageDirectory = dict[int, PageDirectoryEntry]
class Table:
    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def __init__(self, name: str, num_columns: int, key_index: DataIndex, pages_per_range: int):
        self.name: str = name
        self.key_index = DataIndex(key_index)
        self.num_columns: int = num_columns # data columns only
        self.total_columns = self.num_columns + config.NUM_METADATA_COL # inclding metadata
        self.file_handler = FileHandler(self)
        self.page_directory_buff = PseudoBuffDictValue[int, PageDirectoryEntry](self.file_handler, "page_directory")
        # self.last_rid = 1
        self.pages_per_range = pages_per_range

        # ## second milestone
        # self.last_physical_page_id=None
        # self.last_tail_id=None  
        # ####
        
        
        # Page Directory:
        # {Rid: (Page, offset)}
        from lstore.index import Index
        self.index = Index(self)

        self.page_ranges: list[PageRange] = []
        self.page_ranges.append(PageRange(self.num_columns, self.key_index, self.pages_per_range))
        # create a B-tree index object for the key index (hard-coded for M1)
        self.index.create_index(self.key_index)

        self.db_bufferpool = None


    def ith_total_col_shift(self, col_idx: RawIndex) -> int: # returns the bit vector shifted to the indicated col idx
        return 0b1 << (self.total_columns - col_idx - 1)




    # @property
    # def page_range_str(self) -> str:
    #     return helper.str_each_el(self.page_ranges)

    # @property
    # def page_directory_str(self) -> str:
    #     return str({key: (value.page.high_level_str, value.offset) for (key, value) in
    #                 self.page_directory.items()})  # type: ignore[index]

#     def __str__(self) -> str:
#         return f"""{config.INDENT}TABLE: {self.name}
# {config.INDENT}key index: {self.key_index}
# {config.INDENT}num_columns: {self.num_columns}
# {config.INDENT}page_directory: {self.page_directory_str}
# {config.INDENT}page_ranges: {self.page_range_str}"""

    # {config.INDENT}page_directory_raw: {self.page_directory}
    # def get_record_by_rid(self, rid: int) -> Record:
    #     page_dir_entry = self.page_directory_buff[rid]
    #     return page_dir_entry.page_id.get_nth_record(
    #         page_dir_entry.offset)

    # def _update_record_by_id()
    # def __merge(self):
    #     print("merge is happening")
    #     pass
    def bring_base_pages_to_memory(self)-> None:
        list_base_pages=[]
        for table_name in os.listdir(self.path):
            if self.table_name==table_name:
                table_path=os.path.join(self.path,table_name)
                catalog_path = os.path.join(self.path,"catalog")
                with open(catalog_path, 'rb') as catalog:
                    #get the catalog to create the table 
                    table_num_columns= int.from_bytes(catalog.read(8))
                    table_key_index= int.from_bytes(catalog.read(8))
                    table_pages_per_range = int.from_bytes(catalog.read(8))
                    table_last_page_id = int.from_bytes(catalog.read(8))
                    table_last_tail_id= int.from_bytes(catalog.read(8))
                    table_last_rid= int.from_bytes(catalog.read(8))
                    
                for file in os.listdir(table_path):
                    if file =="*page*":
                        page_id=int(file.split("_")[1]) # take the page id, may not work :( 
        #                 #page= BasePage(table_num_columns, DataIndex(table_key_index))
        #                 #page.id=page_id
        #                 page_path = os.path.join(table_path,page_id)
        #                 with open(page_path, "rb") as page_file:
        #                     metadata_id= int(page_file.read(8))
        #                     offset=  int(page_file.read(8))
        #                     page_range_id=int(page_file.read(8))
        #                     if page_range_id== self.page_range_id:
        #                         metadata_path=os.path.join(table_path,metadata_id)
        #                         with open(metadata_path,"rb") as metadata_file:
        #                             rid=metadata_file.read(offset)
        #                             timestamp=metadata_file.read(offset)
        #                             indirection_column=metadata_file.read(offset)
        #                             schema_encoding=metadata_file.read(offset)
        #                             null_column=metadata_file.read(offset)
        #                         list_physical_pages=[]
        #                         list_physical_pages.append(PhysicalPage(bytearray(rid), offset))
        #                         list_physical_pages.append(PhysicalPage(bytearray(timestamp), offset))
        #                         list_physical_pages.append(PhysicalPage(bytearray(indirection_column), offset))
        #                         list_physical_pages.append(PhysicalPage(bytearray(schema_encoding), offset))
        #                         list_physical_pages.append(PhysicalPage(bytearray(null_column), offset))
        #                         while True:
        #                             physical_page_information=page_file.read(offset)
                                    
        #                             if not physical_page_information:
        #                                 break
                                    
        #                             physical_page_data = bytearray(physical_page_information)
        #                             physical_page = PhysicalPage(physical_page_data,offset)
                        file_page_read_result=FileHandler.read_page(page_id,[1]*table_num_columns,[1]*config.NUM_METADATA_COL)     


                        
                        self.get_updated_base_page(file_page_read_result,page_id)
                
                #page directory update 
            
                
                    
                



    
    def merge(self):
        list_base_page=self.bring_base_pages_to_memory()
        pass

    def get_updated_base_page(self,file_page_read_result,page_id):
        object_to_get_tps=PseudoBuffDictValue(FileHandler,page_id,config.TPS)
        tps=self.get_updated_base_page(file_page_read_result,object_to_get_tps.value())
        
        physical_pages=file_page_read_result.data_physical_pages
        metadata=file_page_read_result.metadata_physical_pages
        total_columns=len(physical_pages)+ len(metadata)
        offset=physical_pages[0].offset 
        num_records=offset/8

        for i in range(num_records):
            for j in range(len(physical_pages)): #iterate through all the columns of a record
                indirection_column=metadata[config.INDIRECTION_COLUMN].data[8*i : 8(i+1)]
                schema_encoding=metadata[config.SCHEMA_ENCODING_COLUMN].data[8*i : 8(i+1)]
                null_column=metadata[config.NULL_COLUMN].data[8*i : 8(i+1)]


                tail_indirection_column=indirection_column
                tail_schema_encoding=schema_encoding
                tail_physical_page=physical_pages
                tail_metadata_page=metadata
                tail_offset=i
                tail_null_column=null_column

                ## check tps 
                if tail_indirection_column<tps:
                    break
                ## check if deleted 
                if tail_null_column & 1 << (total_columns-j)!=0: ## check this 
                    break
            
                while tail_schema_encoding & 1 << (total_columns-j)!=0: #column has been updated
                #loop for retreiving information not updated 
                    tail_page_directory_entry = self.page_directory_buff[tail_indirection_column]
                    tail_offset=tail_page_directory_entry.offset
                    tail_page_id = tail_page_directory_entry.page_id  ## tail page id 
                    
                    tail=FileHandler.read_page(tail_page_id,[1]*len(physical_pages),[1]*len(metadata))
                    
                    tail_physical_page=tail.data_physical_pages
                    tail_metadata_page=tail.metadata_physical_pages
                    tail_indirection_column=tail_metadata_page[config.INDIRECTION_COLUMN].data[8*tail_offset: 8*(tail_offset+1)]
                    tail_schema_encoding=tail_metadata_page[config.SCHEMA_ENCODING_COLUMN].data[8*tail_offset : 8*(tail_offset+1)]
                    tail_null_column=metadata[config.NULL_COLUMN].data[8*tail_offset : 8*(tail_offset+1)]
                    

                physical_pages[j].data[8*i : 8*(i+1)]=tail_physical_page[i].data[tail_offset*8:(tail_offset+1)*8]
        #change schema encoding of the updated entry of the base page 
            metadata[config.SCHEMA_ENCODING_COLUMN].data[i*8:8*(i+1)]=0
        
        final_physical_pages=metadata+physical_pages
        #create new base page file with the updated information
        FileHandler.write_new_page(final_physical_pages, "base")
        object_to_get_tps._value=indirection_column
        object_to_get_tps.flush()
          
