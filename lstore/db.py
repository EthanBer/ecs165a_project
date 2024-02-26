import glob
import pickle
from lstore import table
from lstore.ColumnIndex import DataIndex
from lstore.base_tail_page import Page
from lstore.config import config
from lstore.record_physical_page import PhysicalPage
from lstore.table import Table
import os
from  lstore.bufferpool import Bufferpool 
from lstore.page_range import PageRange
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
        table_names = []
        for table_name in os.listdir(path):
            if self.table_by_name(table_name) is None:
                with open(os.path.join(path, table_name, "catalog")) as catalog_file:
                    table_num_columns= int.from_bytes(catalog.read(8), "big")
                    table_key_index= DataIndex(int.from_bytes(catalog.read(8), "big"))
                    table_pages_per_range = int.from_bytes(catalog.read(8), "big")
                    table_last_page_id = int.from_bytes(catalog.read(8), "big")
                    table_last_tail_id= int.from_bytes(catalog.read(8), "big")
                    table_last_rid = int.from_bytes(catalog.read(8), "big")

            page_dir_path = os.path.join(table_path, "page_directory.pickle")
            with open(page_dir_path, "rb") as page_directory:
                table_page_directory = pickle.load(page_directory)

            # final_path=os.path.join(newpath,"page_directory")
            index_path = os.path.join(table_path, "indices.pickle")
            with open(index_path, "rb") as index:
                table_index = pickle.load(index)
            
            self.tables
                # with open(os.path.join())
                # self.tables.append(Table(table_name, ))
                # with open(file_handler.table_file_path("catalog"), 'rb') as catalog:
            # table_names.append(table_name)

        if len (self.tables) == 0 and len(table_names)!= 0:
            for table_name in self.tables: #names of the tables
                table_path = os.path.join(path, table.name)
                
                # final_path = os.path.join(newpath,"catalog")
                with open(os.path.join(table_path, "catalog"), 'rb') as catalog:
                    #get the catalog to create the table 
                    table_num_columns = int.from_bytes(catalog.read(8), "big")
                    table_key_index = DataIndex(int.from_bytes(catalog.read(8), "big"))
                    table_last_base_id = int.from_bytes(catalog.read(8), "big")
                    table_last_tail_id= int.from_bytes(catalog.read(8), "big")
                    table_last_metadata_id = int.from_bytes(catalog.read(8), "big")
                    table_last_rid = int.from_bytes(catalog.read(8), "big")

                # final_path=os.path.join(newpath,"page_directory")
                page_dir_path = os.path.join(table_path, "page_directory.pickle")
                with open(page_dir_path, "rb") as page_directory:
                    table_page_directory = pickle.load(page_directory)

                # final_path=os.path.join(newpath,"page_directory")
                index_path = os.path.join(table_path, "indices.pickle")
                with open(index_path, "rb") as index:
                    table_index = pickle.load(index)
                

        self.bpool=Bufferpool(path, self.tables)

    def close(self) -> None:
        self.bpool.close_bufferpool()
        self.tables.clear()

        
    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key_index: int             #Index of table key in columns

    """
    """
    def create_table(self, name : str, num_columns : int, key_index : DataIndex) -> Table:
        # key_index = DataIndex(key_index)
        table = Table(name, num_columns, key_index, config.PAGES_PER_PAGERANGE)
        self.tables.append(table)
        return table

    
    """
    """
    # Deletes the specified table
    def drop_table(self, name):
        # remove the Table that has the name passed in
        for table in self.tables:
            if table.name == name:
                self.tables.remove(table)
                break
        pass
    """

    
    
    # Returns table with the passed name

    def get_table(self, name: str) -> Table | None:
        for table in self.tables:
            if table.name==name:
                return table 
        return None 

    
