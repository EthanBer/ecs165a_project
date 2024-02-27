"""
A  -Trees, but other data structures can be used as well.
"""
from BTrees.OOBTree import OOBTree # type: ignore


# Tree
# B-Tree documentation:
# https://btrees.readthedocs.io/en/latest/overview.html#related-data-structures
class BTree(OOBTree):
    pass
class Index:

    def __init__(self, num_columns: int):
        # One index for each table. All our empty initially.
        self.indices: list[BTree | None] = [None] * num_columns

        pass


    def locate(self, column: int, value: int) -> int | None:
        try:
            index = self.indices[column] 
            if index is None:
                return None
            search_result = index[value]
            return search_result

        except:
            return None

    def locate_range(self, begin: int, end: int, column: int) -> list:
        result = []
        for key in range(begin, end + 1):
            locate_result = self.locate(column, key)
            if locate_result is not None:
                result.append(locate_result)
        return result

    def create_index(self, column_number: int) -> None:
        if self.indices[column_number] is None:
            self.indices[column_number] = BTree(t=3)

    def update_index(self, column_number: int, key: int | None, value: int) -> None:
        index = self.indices[column_number]  
        if index is not None:
            index.update({key: value})

    """
    # optional: Drop index of specific column
    """

    """
    
    def drop_index(self, column_number):
        pass
    """
