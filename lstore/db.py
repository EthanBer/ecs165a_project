import glob
import pickle
from lstore import pseudo_buff_dict_value
from lstore.ColumnIndex import DataIndex
from lstore.base_tail_page import Page
from lstore.config import config
from lstore.file_handler import Table
from lstore.record_physical_page import PhysicalPage
import os
from  lstore.bufferpool import Bufferpool 
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
        for table_name in os.listdir(path):
            table_path = os.path.join(path, table_name)
            if self.table_by_name(table_name) is None:
                with open(os.path.join(path, table_name, "catalog"), "rb") as catalog_file:
                    num_columns= int.from_bytes(catalog_file.read(8), "big")
                    key_index= DataIndex(int.from_bytes(catalog_file.read(8), "big"))
                    # RID generation is handled by FileHandler

            page_dir_path = os.path.join(table_path, "page_directory.pickle")
            with open(page_dir_path, "rb") as page_directory:
                table_page_directory = pickle.load(page_directory)

            # final_path=os.path.join(newpath,"page_directory")
            index_path = os.path.join(table_path, "indices.pickle")
            with open(index_path, "rb") as index:
                table_index = pickle.load(index)
                # with open(os.path.join())
                # self.tables.append(Table(table_name, ))
                # with open(file_handler.table_file_path("catalog"), 'rb') as catalog:

            table = Table(table_name, num_columns, key_index)
            self.tables.append(table)
            # table_names.append(table_name)

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
    def create_table(self, name : str, num_columns : int, key_index: int) -> Table:
        key_index = DataIndex(key_index)
        table = Table(name, num_columns, key_index)
        self.tables.append(table)
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

    
