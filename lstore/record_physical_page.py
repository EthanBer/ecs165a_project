import struct

# from lstore.ColumnIndex import DataIndex, RawIndex
RawIndex = NewType('RawIndex', int) # Index taking into account the metadata
DataIndex = NewType('DataIndex', int) # Index not taking into account the metadata

class Record:
    def __init__(self, indirection_column: int, rid :int, schema_encoding: int, key : DataIndex, *columns : int):
        self.rid = rid
        self.key = key
        self.schema_encoding = schema_encoding
        self.indirection_column = indirection_column
        self.columns = columns

    def __getitem__(self, key: int) -> int:
        # this syntax is used in the increment() function of query.py, so this operator should be implemented
        return self.columns[key]


class PhysicalPage:

    def __init__(self) -> None:
        # self.size = 8192
        self.size = 4096
        self.data = bytearray(self.size)
        self.offset = 0

    def insert(self, value: int) -> None:
        # Pack the 64-bit integer into bytes (using 'Q' format for unsigned long long)
        packed_data = struct.pack('Q', value)
        # Append the packed bytes to the bytearray
        self.data[:len(packed_data)] = packed_data
        self.offset += 64


    def __get_nth_record__(self, record_idx: int) -> int:
        if record_idx == -1:
            return int(self.data[-4:])
            #return Record()
        return int.from_bytes(self.data[record_idx:record_idx+4], 'big') # Big endianness