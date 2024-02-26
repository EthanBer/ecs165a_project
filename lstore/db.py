from lstore.ColumnIndex import DataIndex
from lstore.config import config
from lstore.table import Table
import os
from  lstore.bufferpool import Bufferpool 
from lstore.page import Page, PhysicalPage
from lstore.page_range import PageRange
class Database():

    def __init__(self) -> None:
        self.tables: list[Table] = []
        self.path=None 
        pass

    # Not required for milestone1
    
    def open(self, path):
        self.path=path
        config.PATH = path
        #if database is new and there are previous files 
        try:
            os.mkdir(path)
        except:  #the db is empty so there are no tables to load to bufferpool
            pass
        table_names = []
        for table_name in os.listdir(path):
            table_names.append(table_name)

        if len (self.tables) == 0 and len(table_names)!= 0:
            for table_name in table_names: #names of the tables
                newpath = os.path.join(path, table_name)
                
                final_path = os.path.join(newpath,"catalog")
                with open(final_path, 'rb') as catalog:
                    #get the catalog to create the table 
                    table_num_columns= int.from_bytes(catalog.readline())
                    table_key_index= int.from_bytes(catalog.readline())
                    table_pages_per_range = int.from_bytes(catalog.readline())
                    table_last_base_page_id = int.from_bytes(catalog.readline())
                    table_last_tail_id= int.from_bytes(catalog.readline())
                    table_last_rid= int.from_bytes(catalog.readline())
                    tps=int.from_bytes(catalog.readline())
                    

                final_path=os.path.join(newpath,"page_directory")
                with open(final_path, "rb") as page_directory:
                    table_page_directory=pickle.load(page_directory)

                final_path=os.path.join(newpath,"page_directory")
                with open(final_path, "rb") as index:
                    table_index=pickle.load(index) #index
                table=Table(table_num_columns,table_key_index,table_pages_per_range) 
                table.page_directory_buff=table_page_directory
                table.index=table_index
                self.tables.append(table)





                
                """"
                list_base_pages=[]
                list_tail_page =[]
                list_page_ranges = []
                new_page_range=PageRange(table_num_columns, table_key_index, table_pages_per_range)
                new_page_range.path=path 
                new_page_range.table_name=table_name
                new_page_range.base_pages=[]
                new_page_range.tail_pages=[]
                list_page_ranges.append(new_page_range)
                num_page = 0
                
                for file in os.listdir(newpath):
                    if file == "*page*":
                        page_id=int(file.split("_")[1]) # take the page id, may not work :( 
                        page=Page(table_num_columns,table_key_index)
                        page.id=page_id 

                        page_path=os.path.join(newpath,file)
                        with open(page_path, "rb") as page_file:
                            metadata_id = int(page_file.read(8))
                            offset= int(page_file.read(8))
                            page_range_id= int(page_file.read(8))
                            new_page_range.page_range_id=page_range_id
                    
                            metadata_path=os.path.join(newpath,metadata_id)
                            with open(metadata_path,"rb") as metadata_file:
                                rid=metadata_file.read(offset)
                                timestamp=metadata_file.read(offset)
                                indirection_column=metadata_file.read(offset)
                                schema_encoding=metadata_file.read(offset)
                                null_column=metadata_file.read(offset)
                            list_physical_pages=[]
                            list_physical_pages.append(PhysicalPage(bytearray(rid), offset))
                            list_physical_pages.append(PhysicalPage(bytearray(timestamp), offset))
                            list_physical_pages.append(PhysicalPage(bytearray(indirection_column), offset))
                            list_physical_pages.append(PhysicalPage(bytearray(schema_encoding), offset))
                            list_physical_pages.append(PhysicalPage(bytearray(null_column), offset))

                            while True:
                                physical_page_information=page_file.read(offset)
                                
                                if not physical_page_information:
                                    break
                                
                                physical_page_data = bytearray(physical_page_information)
                                physical_page = PhysicalPage(physical_page_data,offset)
                                list_physical_pages.append(physical_page)

                            page.physical_pages=list_physical_pages
                            list_base_pages.append(page)
                        
                        if not new_page_range.has_capacity():
                            num_page = 0
                            new_page_range = PageRange(table_num_columns, table_key_index, table_pages_per_range)
                            new_page_range.base_pages=[]
                            new_page_range.tail_pages=[]
                            list_page_ranges.append(new_page_range)
                        
                        if file == "base_page*":
                            list_page_ranges[-1].base_pages.append(page)
                        elif file == "tail_page*":
                            list_page_ranges[-1].tail_pages.append(page)
                        num_page += 1
                        """
             
        self.bpool=Bufferpool(path, self.tables)


    def close(self):
        self.bpool.close_bufferpool()
        self.tables.clear()


        
        
        
    """

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key_index: int             #Index of table key in columns
    """
    def create_table(self, name : str, num_columns : int, key_index : DataIndex) -> Table:
        # key_index = DataIndex(key_index)
        table = Table(name, num_columns, key_index, config.PAGES_PER_PAGERANGE)
        self.tables.append(table)
        return table

    
    """
    # Deletes the specified table
    def drop_table(self, name):
        # remove the Table that has the name passed in
        # self.tables = self.tables.filter((lambda x): x.name != name) 
        pass

    
    
    # Returns table with the passed name

    def get_table(self, name):
        for table in self.tables:
            if table.name==name:
                return table 
        return None 

    
