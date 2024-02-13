"""
A  -Trees, but other data structures can be used as well.
"""

from BTrees.OOBTree import OOBTree
from lstore.table import Table


class Index:

    def __init__(self, table: Table):
        # One index for each table. All our empty initially.
        self.indices = [None] * table.num_columns

        pass

    # Tree
    class BTree:
        def __init__(self, t):
            self.tree = OOBTree()
            self.t = t
            self.key_count = 0

        def insert(self, k):
            self.tree[k[0]] = k[1]

        def search_key(self, k):
            if k in self.tree:
                return self.tree[k]
            else:
                return None

        def search_key_range(self, begin, end):
            result = []
            for key in self.tree.keys(min=begin, max=end):
                result.append(self.tree[key])
            return result

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value) -> int:

        search_result = self.indices[column].search_key(value)
        if search_result is not None:
            return search_result
        else:
            return None

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        """
        range locator for keys
        :param begin: the lower bound of range
        :param end: the higher bound of range
        :param column: the column number
        :return: a list of corresponding RID
        """
        search_result = self.indices[column].search_key_range(begin, end)
        if search_result is not None:
            return search_result
        else:
            return False

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):

        if self.indices[column_number] is None:
            self.indices[column_number] = self.BTree(t=3)

    def update_index(self, column_number, key, value):
        """
        insert a new key-value pair into an existing B-tree index
        :param key: The entry of the corresponding column
        :param value: Base RID of this record
        :return: None
        """
        if self.indices[column_number] is not None:
            self.indices[column_number].insert((key, value))

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        pass
