from lstore.table import Table

class Database():

    def __init__(self):
        self.tables = []
        pass

    # Not required for milestone1
    def open(self, path):
        pass

    def close(self):
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name : str, num_columns : int, key_index : int) -> Table:
        table = Table(name, num_columns, key_index)
        return table

    
    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        # remove the Table that has the name passed in
        # self.tables = self.tables.filter((lambda x): x.name != name) 
        pass

    
    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        pass
