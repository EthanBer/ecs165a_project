from lstore.index import Index
from time import time

from lstore.page import PageRange

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, key, indirection_column, schema_encoding, columns):
        self.rid = RID_COLUMN
        self.key = key
        self.schema_encoding = schema_encoding
        self.indirection_column = 
        self.columns = columns
        RID_COLUMN += 1




class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.index = Index(self)
        
        self.page_ranges = []
        self.page_ranges.append(PageRange())



    def __merge(self):
        print("merge is happening")
        pass
 
