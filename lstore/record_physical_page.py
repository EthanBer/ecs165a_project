import struct
from lstore.ColumnIndex import DataIndex, RawIndex


class Record:
    def __init__(self, indirection_column: int, rid :int, timestamp: int, schema_encoding: int, key: int | None, *columns : int | None):
        self.rid = rid
        self.key = key
        self.schema_encoding = schema_encoding
        self.indirection_column = indirection_column
        self.columns = columns

    def __getitem__(self, key: int) -> int | None:
        # this syntax is used in the increment() function of query.py, so this operator should be implemented
        return self.columns[key]


class PhysicalPage:

    def __init__(self) -> None:
        # self.size = 8192
        self.size = 4096
        self.data = bytearray(self.size)
        self.offset = 0

    def insert(self, value: int | None) -> None:
        # Pack the 64-bit integer into bytes (using '>Q' format for unsigned long long with big endian)
        packed_data = struct.pack('>Q', value)
        if self.offset + 8 > len(self.data):
            raise ValueError("Not enough space in bytearray")
        
        # Append the packed bytes to the bytearray
        self.data[self.offset : self.offset+8] = packed_data
        self.offset += 8  

    
    def __get_nth_record__(self, record_idx: int) -> int:
        if record_idx == -1:
            return int(self.data[-8:])
            #return Record()
        
        num_records = self.offset / 8
        if (record_idx > num_records-1):
            return 0
        
        value = struct.unpack('>Q', self.data[(record_idx * 8) : (record_idx * 8)+8])[0]

        if (value != 0):
            #print("Value: ", value, "Index: ", record_idx)
            # print("num records: ", num_records) print("offset: ", self.offset)
            pass

        return value
    

    def __str__(self) -> str:
        physical_page_contents = []
        for i in range(64):
            physical_page_contents.append(self.__get_nth_record__(i))
        return str(physical_page_contents)
