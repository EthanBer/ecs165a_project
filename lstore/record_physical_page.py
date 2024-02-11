import struct
from typing import Type
from lstore.ColumnIndex import DataIndex, RawIndex
from lstore.config import Metadata, config


class Record:
    def __init__(self, metadata: Metadata, key: int | None, *columns : int | None):
        self.indirection_column = metadata.indirection_column
        self.timestamp = metadata.timestamp
        self.rid = metadata.rid
        self.schema_encoding = metadata.schema_encoding
        self.key = key
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
        packed_data = struct.pack(config.PACKING_FORMAT_STR, 0 if value is None else value)
        if self.offset + 8 > len(self.data):
            raise ValueError("Not enough space in bytearray")
        
        # Append the packed bytes to the bytearray
        self.data[self.offset : self.offset+8] = packed_data
        self.offset += 8  

    
    def __get_nth_record__(self, record_idx: int) -> int | None:
        if record_idx == -1:
            return int(self.data[-8:])
            #return Record()
        
        num_records = self.offset / 8
        if (record_idx > num_records-1):
            # raise(Exception("get nth record read fail: out of bounds index"))
            return None # TODO: fix?
        
        value = struct.unpack(config.PACKING_FORMAT_STR, self.data[(record_idx * 8) : (record_idx * 8)+8])[0]

        if (value != 0):
            #print("Value: ", value, "Index: ", record_idx)
            # print("num records: ", num_records) print("offset: ", self.offset)
            pass

        return value
    

    def __str__(self) -> str:
        physical_page_contents: list[int] = []
        for i in range(64):
            rec = self.__get_nth_record__(i)
            # if rec is None:
            #     warn(Exception("a value was None when getting nth record for str function in PhysicalPage."))
            physical_page_contents.append(rec)
        return str(physical_page_contents)
