
import struct


class Record:

	RID_COLUMN = 0
	def __init__(self, key: int, indirection_column: int, schema_encoding: int, *columns: int):
		self.rid = self.RID_COLUMN
		self.key = key
		self.schema_encoding = schema_encoding
		self.indirection_column = indirection_column
		self.columns = columns
		self.RID_COLUMN += 1

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
        return int(self.data[record_idx:record_idx+4])