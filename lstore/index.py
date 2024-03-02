"""
A  -Trees, but other data structures can be used as well.
"""
from BTrees.OOBTree import OOBTree  # type: ignore


class Index:

    def __init__(self, num_columns: int):
        # One index for each table. All our empty initially.
        self.indices: list[BTree | None] = [None] * num_columns

        pass

    # Tree
    # B-Tree documentation:
    # https://btrees.readthedocs.io/en/latest/overview.html#related-data-structures
    class BTree(OOBTree):
        pass

    def locate(self, column: int, value: int) -> int | list | None:
        try:
            search_result = self.indices[column][value] #@IgnoreException
            if len(search_result) == 1:
                return search_result[0]
            else:
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
            self.indices[column_number] = self.BTree(t=3)

    def update_index(self, column_number: int, key: int, value: int) -> None:

        index_object = self.indices[column_number]
        existing_vals = self.locate(column_number, key)

        if existing_vals is not None:
            existing_vals = index_object[key]
            existing_vals.append(value)
            index_object.update({key: existing_vals})
        else:
            index_object.update({key: [value]})

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        pass


