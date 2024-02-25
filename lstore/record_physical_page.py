import struct
from typing import Annotated
from lstore.config import Metadata, config
from lstore.helper import helper


class Record:
    def __init__(self, metadata: Metadata, base_record: bool, *columns : int | None):
        # self.indirection_column = metadata.indirection_column
        # self.timestamp = metadata.timestamp
        # self.rid = metadata.rid
        # self.schema_encoding = metadata.schema_encoding
        self.metadata = metadata
        self.columns = columns
        # self.key = key
        self.base_record = base_record

    def __getitem__(self, key: int) -> int | None:
        # this syntax is used in the increment() function of query.py, so this operator should be implemented
        return self.columns[key]
         #return col_list[key]


class BaseRecord(Record):
    def __init__(self, metadata: Metadata, key: int, *columns: int | None):
        super().__init__(metadata, *columns)
        self.key = key
    # def __str__(self) -> str:
    #     # NOTE: the self.columns is just the physical values in the columns. 
    #     # if the physical value is 0, the actual value could be 0 or None depending on the corresponding NULL_COLUMN value
    #     return f"BaseRecord RID{self.metadata.rid}; idr:{self.metadata.indirection_column}; senc:{bin(self.metadata.schema_encoding)}; key:{self.key}; columns:{self.columns}"

    # def __str__(self) -> str:
    #     # NOTE: the self.columns is just the physical values in the columns. 
    #     # if the physical value is 0, the actual value could be 0 or None depending on the corresponding NULL_COLUMN value
    #     return f"TailRecord RID{self.metadata.rid}; idr:{self.metadata.indirection_column}; senc:{bin(self.metadata.schema_encoding)}; columns:{self.columns}"

# class BaseRecord(Record):
#     pass

# class TailRecord(Record):
#     pass

class PhysicalPage:
    def __init__(self, data: bytearray=bytearray(config.PHYSICAL_PAGE_SIZE), offset: int=0) -> None:
        # self.size = 8192
        self.size = config.PHYSICAL_PAGE_SIZE
        self.data = data
        self.offset = offset

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
        
        # value = struct.unpack(config.PACKING_FORMAT_STR, self.data[(record_idx * 8) : (record_idx * 8)+8])[0]
        value = helper.unpack_data(self.data, record_idx)



        if (value != 0):
            ## print("Value: ", value, "Index: ", record_idx)
            # # print("num records: ", num_records) # print("offset: ", self.offset)
            pass

        return value
    

    def __str__(self) -> str:
        physical_page_contents: list[int | None] = []
        for i in range(64):
            rec = self.__get_nth_record__(i)
            # if rec is None:
            #     warn(Exception("a value was None when getting nth record for str function in PhysicalPage."))
            physical_page_contents.append(rec)
        return str(physical_page_contents)
