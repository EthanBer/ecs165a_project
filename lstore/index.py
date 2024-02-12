"""
A  -Trees, but other data structures can be used as well.
"""


from lstore.table import Table

class Index:

    def __init__(self, table: Table):
        # One index for each table. All our empty initially.
        self.indices = [None] * table.num_columns
        pass

    class BTreeNode:
        def __init__(self, leaf=False):
            self.leaf = leaf
            self.keys = []
            self.child = []

    # Tree
    class BTree:
        # Source of B-tree code:
        # https://www.geeksforgeeks.org/introduction-of-b-tree-2/
        def __init__(self, t):
            self.root = Index.BTreeNode(True)
            self.t = t

        def insert(self, k):
            # insert a node
            root = self.root
            if len(root.keys) == (2 * self.t) - 1:
                temp = Index.BTreeNode()
                self.root = temp  # empty keys still
                temp.child.insert(0, root)
                self.split_child(temp, 0)
                self.insert_non_full(temp, k)
            else:
                self.insert_non_full(root, k)

        def insert_non_full(self, x, k):
            # insert a key to a non-full node
            i = len(x.keys) - 1
            if x.leaf:
                x.keys.append((None, None))
                while i >= 0 and k[0] < x.keys[i][0]:
                    x.keys[i + 1] = x.keys[i]
                    i -= 1
                x.keys[i + 1] = k
            else:
                while i >= 0 and k[0] < x.keys[i][0]:
                    i -= 1
                i += 1
                if len(x.child[i].keys) == (2 * self.t) - 1:
                    self.split_child(x, i)
                    if k[0] > x.keys[i][0]:
                        i += 1
                self.insert_non_full(x.child[i], k)

        def split_child(self, x, i):
            # Split the child node to give space for new keys
            t = self.t
            y = x.child[i]
            z = Index.BTreeNode(y.leaf)
            x.child.insert(i + 1, z)
            x.keys.insert(i, y.keys[t - 1])
            z.keys = y.keys[t: (2 * t) - 1]
            y.keys = y.keys[0: t - 1]
            if not y.leaf:
                z.child = y.child[t: 2 * t]
                y.child = y.child[0: t - 1]

        def print_tree(self, x, l=0):
            print("Level ", l, " ", len(x.keys), end=":")
            for i in x.keys:
                print(i, end=" ")
            print()
            l += 1
            if len(x.child) > 0:
                for i in x.child:
                    self.print_tree(i, l)

        # Search key in the tree
        def search_key(self, k, x=None):
            """
            A top-down searching algorithm
            :param k: the value users want to find
            :param x: the current node
            :return: a tuple that stores the key-value pair in a node
            """
            if x is not None:
                i = 0

                while i < len(x.keys) and k > x.keys[i][0]:  # compare k with the i-th element in the current node
                    i += 1

                # key found, return the node and its index
                if i < len(x.keys) and k == x.keys[i][0]:
                    return (x, i)

                # key not found
                elif x.leaf:
                    return None

                # search in a child
                else:
                    return self.search_key(k, x.child[i])

            # case 2: x is a root node
            else:
                return self.search_key(k, self.root)

        def search_key_range(self, begin, end, x=None):
            """
            A top-down searching algorithm. We begin with the root node
            and search for qualified keys as we traverse down
            :param begin: the lower bound of range
            :param end:  the higher bound of range
            :param x: the current node
            :return: a list of tuples
            """
            result = []

            if x is not None:
                i = 0
                while i < len(x.keys) and x.keys[i][0] < begin:
                    # move the pointer i to the key that is not smaller than `begin`
                    i += 1

                while i < len(x.keys) and x.keys[i][0] <= end:
                    # append all the keys until it is bigger than `end`
                    result.append(x.keys[i])
                    i += 1

                if not x.leaf:
                    for child in x.child:
                        # search each child node if x is not a leaf node
                        result.extend(self.search_key_range(begin, end, child))

                return result

            else:
                return self.search_key_range(begin, end, self.root)


    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):

        search_result = self.indices[column].search_key(value)
        if search_result is not None:
            value_ind = search_result[1]

            return search_result[0].keys[value_ind][1]

        else:
            print('no value found!')

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
            return [key[1] for key in search_result]

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
            #print("ABOUT TO UPDATE")
            key_val_pair = (key, value)
            self.indices[column_number].insert(key_val_pair)  # insert the pair into existing tree index
            #print("key_val_pair: ", key_val_pair)


    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        pass

