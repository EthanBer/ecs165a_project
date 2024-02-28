import glob
import pickle
from lstore import pseudo_buff_dict_value
from lstore.ColumnIndex import DataIndex
#from lstore.base_tail_page import Page
from lstore.config import config
from lstore.helper import helper
from lstore.index import Index
from lstore.page_directory_entry import BasePageID, MetadataPageID, TailPageID
from lstore.record_physical_page import PhysicalPage
import os
from  lstore.bufferpool import Bufferpool, Table
class Database():

    def __init__(self) -> None:
        self.tables: list[Table] = []
        self.path=""
        pass

    def table_by_name(self, table_name: str) -> Table | None:
        return next(( table for table in self.tables if table.name == table_name ), None)
    # Not required for milestone1
    
    def open(self, path: str) -> None:
        self.path=path
        #if database is new and there are previous files 
        try:
            os.mkdir(path)
        except:  #the db is empty so there are no tables to load to bufferpool
            pass
        self.bpool=Bufferpool(path)
        for table_name in os.listdir(path):
            if self.table_by_name(table_name) is None:
                with open(os.path.join(path, table_name, "catalog"), "rb") as catalog_file:
                    num_columns= int.from_bytes(catalog_file.read(8), "big")
                    key_index= DataIndex(int.from_bytes(catalog_file.read(8), "big"))
                    # RID generation is handled by FileHandler
                    # with open(os.path.join())
                    # self.tables.append(Table(table_name, ))
                    # with open(file_handler.table_file_path("catalog"), 'rb') as catalog:

                table = Table(table_name, num_columns, key_index, path, self.bpool)
                self.tables.append(table)
            # table_names.append(table_name)


    def close(self) -> None:
        for table in self.tables:
            table.page_directory_buff.flush()
            table.file_handler.write_new_base_page()
        self.bpool.close_bufferpool()
        self.tables.clear()


        
    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key_index: int             #Index of table key in columns

    """
    def create_table(self, name : str, num_columns : int, key_index: int) -> Table:
        key_index = DataIndex(key_index)
        table_path = os.path.join(self.path, name)
        os.mkdir(table_path)
        table = Table(name, num_columns, key_index, self.path, self.bpool)
        self.tables.append(table)

        # initialize catalog file
        return table

    
    # Deletes the specified table
    def drop_table(self, name: str) -> None:
        # remove the Table that has the name passed in
        for table in self.tables:
            if table.name == name:
                self.tables.remove(table)
                break
        pass

    
    
    # Returns table with the passed name

    def get_table(self, name: str) -> Table | None:
        for table in self.tables:
            if table.name==name:
                return table 
        return None 

    
