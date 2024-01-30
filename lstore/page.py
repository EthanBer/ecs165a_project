import struct

class PageRange:
    def __init__(self):
        self.base_pages = []
        self.tail_pages= []
        
        self.base_pages.append(Page())
  

class Page:

    def __init__(self):
        self.num_records = 0
        self.physical_pages = []
        # len(self.pages) == num_columns


    def has_capacity(self) -> bool:
        #if self.num_records
        pass
    
    def insert(self, *columns : list) -> int:
        print(columns,"\n")

        if (self.num_records == 0):
            for i in range(len(columns)):
                page = PhysicalPage()
                self.physical_pages.append(page)
        else:
            if (self.has_capacity == False):
                return -1
        
        for i in range(len(columns)):
            self.physical_pages[i].insert(columns[i])
        
        self.num_records += 1
        return 0


    def update():
        pass

    def get_nth_record(self, n):
        # get nth record of this page
        pass


class PhysicalPage:
    
    def __init__(self):
        # self.size = 8192
        self.size = 4096
        self.data = bytearray(self.size) 
        self.offset = 0

    def insert(self, value):
        
        #value = int(value)
        #self.data.append(value)

        # Pack the 64-bit integer into bytes (using 'Q' format for unsigned long long)
        packed_data = struct.pack('Q', value)
        # Append the packed bytes to the bytearray
        self.data[:len(packed_data)] = packed_data
        self.offset += 64


    def update():
        pass
