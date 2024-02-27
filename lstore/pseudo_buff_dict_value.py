import pickle
from time import time
from typing import Generic, Literal, TypeVar
from lstore.ColumnIndex import DataIndex, RawIndex
from lstore.config import config
from lstore.page_directory_entry import BasePageID, PageDirectoryEntry, PageID


INDIRECTION_COLUMN = 0
RID_COLUMN = 1
SCHEMA_ENCODING_COLUMN = 2
#mutex_lock= threading.Lock()

class Table:
    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def __init__(self, name: str, num_columns: int, key_index: DataIndex):
        self.name: str = name
        self.key_index = DataIndex(key_index)
        self.num_columns: int = num_columns # data columns only
        self.total_columns = self.num_columns + config.NUM_METADATA_COL # inclding metadata
        self.file_handler = FileHandler(self)
        self.page_directory_buff = PseudoBuffDictValue[int, PageDirectoryEntry](self.file_handler, "page_directory")
        # self.last_rid = 1

        # ## second milestone
        # self.last_physical_page_id=None
        # self.last_tail_id=None  
        # ####
        
        
        # Page Directory:
        # {Rid: (Page, offset)}
        from lstore.index import Index
        self.index = Index(self)

        # create a B-tree index object for the key index (hard-coded for M1)
        self.index.create_index(self.key_index)
    def ith_total_col_shift(self, col_idx: RawIndex) -> int: # returns the bit vector shifted to the indicated col idx
        return 0b1 << (self.total_columns - col_idx - 1)




    # @property
    # def page_range_str(self) -> str:
    #     return helper.str_each_el(self.page_ranges)

    # @property
    # def page_directory_str(self) -> str:
    #     return str({key: (value.page.high_level_str, value.offset) for (key, value) in
    #                 self.page_directory.items()})  # type: ignore[index]

#     def __str__(self) -> str:
#         return f"""{config.INDENT}TABLE: {self.name}
# {config.INDENT}key index: {self.key_index}
# {config.INDENT}num_columns: {self.num_columns}
# {config.INDENT}page_directory: {self.page_directory_str}
# {config.INDENT}page_ranges: {self.page_range_str}"""

    # {config.INDENT}page_directory_raw: {self.page_directory}
    # def get_record_by_rid(self, rid: int) -> Record:
    #     page_dir_entry = self.page_directory_buff[rid]
    #     return page_dir_entry.page_id.get_nth_record(
    #         page_dir_entry.offset)

    # def _update_record_by_id()
    # def __merge(self):
    #     print("merge is happening")
    #     pass

    # TODO: uncomment
    """
    
    def bring_base_pages_to_memory(self)-> None:
        list_base_pages=[]
        for table_name in os.listdir(self.path):
            if self.table_name==table_name:
                table_path=os.path.join(self.path,table_name)
                catalog_path = os.path.join(self.path,"catalog")
                with open(catalog_path, 'rb') as catalog:
                    #get the catalog to create the table 
                    table_num_columns= int.from_bytes(catalog.read(8))
                    table_key_index= int.from_bytes(catalog.read(8))
                    table_pages_per_range = int.from_bytes(catalog.read(8))
                    table_last_page_id = int.from_bytes(catalog.read(8))
                    table_last_tail_id= int.from_bytes(catalog.read(8))
                    table_last_rid= int.from_bytes(catalog.read(8))
                    
                for file in os.listdir(table_path):
                    if file =="*page*":
                        page_id=int(file.split("_")[1]) # take the page id, may not work :( 
        #                 #page= BasePage(table_num_columns, DataIndex(table_key_index))
        #                 #page.id=page_id
        #                 page_path = os.path.join(table_path,page_id)
        #                 with open(page_path, "rb") as page_file:
        #                     metadata_id= int(page_file.read(8))
        #                     offset=  int(page_file.read(8))
        #                     page_range_id=int(page_file.read(8))
        #                     if page_range_id== self.page_range_id:
        #                         metadata_path=os.path.join(table_path,metadata_id)
        #                         with open(metadata_path,"rb") as metadata_file:
        #                             rid=metadata_file.read(offset)
        #                             timestamp=metadata_file.read(offset)
        #                             indirection_column=metadata_file.read(offset)
        #                             schema_encoding=metadata_file.read(offset)
        #                             null_column=metadata_file.read(offset)
        #                         list_physical_pages=[]
        #                         list_physical_pages.append(PhysicalPage(bytearray(rid), offset))
        #                         list_physical_pages.append(PhysicalPage(bytearray(timestamp), offset))
        #                         list_physical_pages.append(PhysicalPage(bytearray(indirection_column), offset))
        #                         list_physical_pages.append(PhysicalPage(bytearray(schema_encoding), offset))
        #                         list_physical_pages.append(PhysicalPage(bytearray(null_column), offset))
        #                         while True:
        #                             physical_page_information=page_file.read(offset)
                                    
        #                             if not physical_page_information:
        #                                 break
                                    
        #                             physical_page_data = bytearray(physical_page_information)
        #                             physical_page = PhysicalPage(physical_page_data,offset)
                        file_page_read_result=FileHandler.read_page(page_id,[1]*table_num_columns,[1]*config.NUM_METADATA_COL)     


                        
                        self.get_updated_base_page(file_page_read_result,page_id)
                        
                #page directory update 
            
                
                    
                



        

    def merge(self):
        list_base_page=self.bring_base_pages_to_memory()
        pass

    def get_updated_base_page(self,file_page_read_result,page_id):
        object_to_get_tps=PseudoBuffDictValue(FileHandler,page_id,config.TPS)
        tps=self.get_updated_base_page(file_page_read_result,object_to_get_tps.value())
        
        physical_pages=file_page_read_result.data_physical_pages
        metadata=file_page_read_result.metadata_physical_pages
        total_columns=len(physical_pages)+ len(metadata)
        offset=physical_pages[0].offset 
        num_records=offset/8

        for i in range(num_records):
            for j in range(len(physical_pages)): #iterate through all the columns of a record
                indirection_column=metadata[config.INDIRECTION_COLUMN].data[8*i : 8(i+1)]
                schema_encoding=metadata[config.SCHEMA_ENCODING_COLUMN].data[8*i : 8(i+1)]
                null_column=metadata[config.NULL_COLUMN].data[8*i : 8(i+1)]


                tail_indirection_column=indirection_column
                tail_schema_encoding=schema_encoding
                tail_physical_page=physical_pages
                tail_metadata_page=metadata
                tail_offset=i
                tail_null_column=null_column

                ## check tps 
                if tail_indirection_column<tps:
                    break
                ## check if deleted 
                if tail_null_column & 1 << (total_columns-j)!=0: ## check this 
                    break
            
                while tail_schema_encoding & 1 << (total_columns-j)!=0: #column has been updated
                #loop for retreiving information not updated 
                    tail_page_directory_entry = self.page_directory_buff[tail_indirection_column]
                    tail_offset=tail_page_directory_entry.offset
                    tail_page_id = tail_page_directory_entry.page_id  ## tail page id 
                    
                    tail=FileHandler.read_page(tail_page_id,[1]*len(physical_pages),[1]*len(metadata))
                    
                    tail_physical_page=tail.data_physical_pages
                    tail_metadata_page=tail.metadata_physical_pages
                    tail_indirection_column=tail_metadata_page[config.INDIRECTION_COLUMN].data[8*tail_offset: 8*(tail_offset+1)]
                    tail_schema_encoding=tail_metadata_page[config.SCHEMA_ENCODING_COLUMN].data[8*tail_offset : 8*(tail_offset+1)]
                    tail_null_column=metadata[config.NULL_COLUMN].data[8*tail_offset : 8*(tail_offset+1)]
                    

                physical_pages[j].data[8*i : 8*(i+1)]=tail_physical_page[i].data[tail_offset*8:(tail_offset+1)*8]
        #change schema encoding of the updated entry of the base page 
            metadata[config.SCHEMA_ENCODING_COLUMN].data[i*8:8*(i+1)]=0
        
        final_physical_pages=metadata+physical_pages
        #create new base page file with the updated information
        FileHandler.write_new_page(final_physical_pages, "base")
        object_to_get_tps._value=indirection_column
        object_to_get_tps.flush()
          
        
    """
class PsuedoBuffIntValue():
	def __init__(self, file_handler: 'FileHandler', page_sub_path: PageID | Literal["catalog"], byte_position: int) -> None:
		self.page_sub_path = page_sub_path
		self.page_path = file_handler.page_path(page_sub_path)
		self.file_handler = file_handler
		self.byte_position = byte_position
		self._value = file_handler.read_int_value(page_sub_path, byte_position)
		# self._value = file_handler.read_value(page_sub_path, byte_position, "int")
	def flush(self) -> None:
		self.file_handler.write_position(self.page_path, self.byte_position, self._value)
	def value(self, increment: int=0) -> int:
		if increment != 0:
			self._value += increment 
		return self._value
	def __del__(self) -> None: # flush when this value is deleted
		self.flush()

class PseudoBuffBaseIDValue(PsuedoBuffIntValue):
	def value(self, increment: int = 0) -> BasePageID:
		super().value()
		return BasePageID(self._value)


class FileHandler:
	def __init__(self, table: 'Table') -> None:
		# self.last_base_page_id = self.get_last_base_page_id()

		# NOTE: these next_*_id variables represent the *next* id to be written, not necessarily the last one. the last 
		# written id is the next_*_id variable minus 1
		self.next_base_page_id = PseudoBuffBaseIDValue(self, "catalog", config.byte_position.CATALOG_LAST_BASE_ID)
		self.next_tail_page_id = PsuedoBuffIntValue(self, "catalog", config.byte_position.CATALOG_LAST_TAIL_ID)
		self.next_metadata_page_id = PsuedoBuffIntValue(self, "catalog", config.byte_position.CATALOG_LAST_METADATA_ID)
		self.next_rid = PsuedoBuffIntValue(self, "catalog", config.byte_position.CATALOG_LAST_RID)
		# TODO: populate the offset byte with 0 when creating a new page
		self.offset = PsuedoBuffIntValue(self, BasePageID(self.next_base_page_id.value() - 1), config.byte_position.OFFSET) # the current offset is based on the last written page
		self.table = table
		t_base = self.read_projected_cols_of_page(BasePageID(self.next_base_page_id.value() - 1)) # could be empty PhysicalPages, to start. but the page files should still exist, even when they are empty
		if t_base is None:
			raise(Exception("the base_page_id just before the next_page_id must have a folder."))
		t_tail = self.read_projected_cols_of_page(TailPageID(self.next_base_page_id.value() - 1))
		if t_tail is None:
			raise(Exception("the tail_page_id just before the next_page_id must have a folder."))
		
		self.base_page_to_commit: Annotated[list[PhysicalPage | None], self.table.total_columns] = t_base.metadata_physical_pages + t_base.data_physical_pages # could be empty PhysicalPages, to start. concatenating the metadata and physical 
		# check that physical page sizes and offsets are the same
		assert len(
			set(map(lambda physicalPage: physicalPage.size, self.page_to_commit))) <= 1
		assert len(
			set(map(lambda physicalPage: physicalPage.offset, self.page_to_commit))) <= 1
		self.table_path = os.path.join(config.PATH, self.table.name)
	def base_path(self, base_page_id: int) -> str:
		return os.path.join(self.table_path, f"base_{base_page_id}")
		# return os.path.join(config.PATH, self.table.name, f"base_{base_page_id}")
	def tail_path(self, tail_page_id: int) -> str:
		return os.path.join(self.table_path, f"tail_{tail_page_id}")
		# return os.path.join(config.PATH, self.table.name, f"tail_{tail_page_id}")
	def metadata_path(self, metadata_page_id: int) -> str:
		return os.path.join(self.table_path, f"metadata_{metadata_page_id}")

	# this calculated property gives the path for a "table file". 
	# Table files are files which apply to the entire table. These files are, as of now, 
	# "catalog", "page_directory.pickle", and "indices.pickle". The page directory and indices
	# files are only specified by their names (even though they will be persisted separately with pickle)
	def table_file_path(self, file_name: Literal["catalog", "page_directory", "indices"]) -> str:
		path = os.path.join(self.table_path, file_name)
		if file_name == "page_directory" or file_name == "indices":
			path += ".pickle" # these files have the .pickle extension
		return path

	# @staticmethod
	# def table_file_path_static(self, file_name: Literal["catalog", "page_directory", "indices"], table_name: str) -> str:
	# 	path = os.path.join(os.path.join(), file_name)
	# 	if file_name == "page_directory" or file_name == "indices":
	# 		path += ".pickle" # these files have the .pickle extension
	# 	return path


	# def get_last_ids(self) -> tuple[int, int, int]: # writing boolean specifies whether this id will be written to by the user.
	# 	with open(self.catalog_path, "r") as file:
	# 		return (int(file.read(8)), int(file.read(8)), int(file.read(8)))

	def page_id_to_path(self, page_id: PageID) -> str:
		path: str = ""
		if isinstance(page_id, BasePageID):
			path = self.base_path(page_id)
		elif isinstance(page_id, TailPageID):
			path = self.tail_path(page_id)
		elif isinstance(page_id, MetadataPageID):
			path = self.metadata_path(page_id)
		else:
			raise(Exception(f"page_id had unexpected type of {type(page_id)}"))
		return path

	@staticmethod
	def write_position(page_path: str, byte_position: int, value: int) -> bool:
		with open(page_path, "wb") as file:
			file.seek(byte_position)
			file.write(value.to_bytes(config.BYTES_PER_INT, byteorder="big"))
		return True

	# should only be "base" or "tail" path_type
	def write_new_page(self, physical_pages: list[PhysicalPage], path_type: str) -> bool: # the page MUST be full in order to write. returns true if success
		written_id = self.next_base_page_id.value(1)
		path = self.base_path(written_id) if path_type == "base" else self.base_path(self.next_tail_page_id.value(1))

		# check that physical page sizes and offsets are the same
		assert len(
			set(map(lambda physicalPage: physicalPage.size, physical_pages))) <= 1
		assert len(
			set(map(lambda physicalPage: physicalPage.offset, physical_pages))) <= 1

		metadata_pointer = self.next_metadata_page_id.value(1)
		with open(self.metadata_path(metadata_pointer), "wb") as file: # open metadata file
			for i in range(config.NUM_METADATA_COL):
				file.write(physical_pages[i].data) # write the metadata columns

		with open(path, "wb") as file: # open page file
			file.write(metadata_pointer.to_bytes(config.BYTES_PER_INT, byteorder="big"))
			file.write((16).to_bytes(8, byteorder="big")) # offset 16 is the first byte offset where data can go
			for i in range(config.NUM_METADATA_COL, len(physical_pages)): # write the data columns
				file.write(physical_pages[i].data)

		# t = self.read_page(BasePageID(self.next_base_page_id.value()))
		# assert t
		self.page_to_commit = [PhysicalPage()] * self.table.total_columns
		return True

	def read_value_page_directory(self) -> dict[int, 'PageDirectoryEntry']:
		page_path = self.page_path("page_directory")
		with open(page_path, "rb") as handle:
			ret: dict[int, 'PageDirectoryEntry'] = pickle.load(handle) # this is not typesafe at all.... ohwell
			return ret

	def read_int_value(self, page_sub_path: PageID | Literal["catalog"], byte_position: int) -> int:
		page_path = self.page_path(page_sub_path)
		with open(page_path) as file:
			assert byte_position is not None
			file.seek(byte_position)
			return int(file.read(8)) # assume buffered value is 8 bytes
	def read_dict_value(self, page_sub_path: Literal["page_directory", "indices"]) -> dict:
		with open(page_sub_path, "rb") as handle:
			return pickle.load(handle)


	# returns the full page path, given a particular pageID OR 
	# the special catalog/page_directory files
	def page_path(self, page_sub_path: PageID | Literal["catalog", "page_directory", "indices"]) -> str:
		if isinstance(page_sub_path, PageID):
			return self.page_id_to_path(page_sub_path)
		elif page_sub_path == "catalog" or page_sub_path == "page_directory":
			return self.table_file_path(page_sub_path)
		else:
			raise(Exception(f"unexpected page_sub_path {page_sub_path}"))


	# reads the full base page written to disk
	# the [1] default value is just so that I can overwrite it later with the proper default value; 
	# in other words it is just a placeholder
	# returns None for every column not in projected_columns_index
	def read_projected_cols_of_page(self, page_id: PageID, projected_columns_index: list[Literal[0, 1]] | None = None, projected_metadata_columns_index: list[Literal[0, 1]] | None = None) -> FilePageReadResult | None: 
		projected_columns_index = [1] * self.table.num_columns if projected_columns_index is None else projected_columns_index  # type: ignore
		projected_metadata_columns_index = [1] * config.NUM_METADATA_COL if projected_metadata_columns_index is None else projected_metadata_columns_index # type: ignore
		assert projected_columns_index is not None
		assert projected_metadata_columns_index is not None
		physical_pages: list[PhysicalPage | None] = [None] * self.table.num_columns
		metadata_pages: list[PhysicalPage | None] = [None] * config.NUM_METADATA_COL
		metadata_path = self.metadata_path(PsuedoBuffIntValue(self, page_id, config.byte_position.METADATA_PTR).value())
		path = self.page_id_to_path(page_id)
		if not os.path.isfile(metadata_path) or not os.path.isfile(path):
			return None

		offset = self.read_int_value(page_id, config.byte_position.OFFSET)
		# read selected metadata
		with open(metadata_path, "rb") as metadata_file:
			for i in range(config.NUM_METADATA_COL):
				metadata_pages[i] = PhysicalPage(data=bytearray(metadata_file.read(config.PHYSICAL_PAGE_SIZE)), offset=offset)

		# read selected data
		with open(path, "rb") as file: 
			for i in range(self.table.num_columns):
				if projected_columns_index[i] == 1:
					physical_pages[i] = PhysicalPage(data=bytearray(file.read(config.PHYSICAL_PAGE_SIZE)), offset=offset)
					# physical_pages.append(PhysicalPage(data=bytearray(file.read(config.PHYSICAL_PAGE_SIZE)), offset=offset))
				else:
					# physical_pages.append(None)
					file.seek(config.PHYSICAL_PAGE_SIZE, 1) # seek 4096 (or size) bytes forward from current position (the 1 means "from current position")
		page_type: Literal["base", "tail"] = "base"
		if isinstance(page_id, TailPageID):
			page_type = "tail"
		elif not isinstance(page_id, BasePageID):
			raise(Exception("unexpected page_id type that wasn't base or tail page?"))
		return FilePageReadResult(metadata_pages, physical_pages, page_type)

	def read_full_page(self, page_id: PageID) -> FullFilePageReadResult | None:
		res = self.read_projected_cols_of_page(page_id)
		if res is None:
			return None
		assert len(res.data_physical_pages) == self.table.num_columns
		assert len(res.metadata_physical_pages) == config.NUM_METADATA_COL
		filtered_data = [physical_page for physical_page in res.data_physical_pages if physical_page is not None]
		filtered_metadata = [physical_page for physical_page in res.metadata_physical_pages if physical_page is not None]
		assert len(filtered_data) == self.table.num_columns
		assert len(filtered_metadata) == config.NUM_METADATA_COL
		page_type: Literal["base", "tail"] = "base"
		if isinstance(page_id, TailPageID):
			page_type = "tail"
		elif not isinstance(page_id, BasePageID):
			raise(Exception("unexpected page_id type that wasn't base or tail page?"))
		return FullFilePageReadResult(filtered_metadata, filtered_data, page_type)



	def insert_record(self, path_type: Literal["base", "tail"], metadata: WriteSpecifiedMetadata, *columns: int | None) -> int: # returns RID of inserted record
		null_bitmask = 0
		total_cols = self.table.total_columns
		if metadata.indirection_column == None: # set 1 for null indirection column
			# print("setting indirection null bit")
			null_bitmask = helper.ith_total_col_shift(total_cols, config.INDIRECTION_COLUMN)
			# null_bitmask = 1 << (total_cols - 1)
		for idx, column in enumerate(columns):
			# print(f"checking cols for null... {column}")
			if column is None:
				# print("found a null col")
				null_bitmask = null_bitmask | helper.ith_total_col_shift(len(columns), idx, False) #
				# null_bitmask = null_bitmask | (1 << (len(columns)-idx-1))
			
		# print(f"inserting null bitmask {bin(null_bitmask)}")
		
		# Transform columns to a list to append the schema encoding and the indirection column
		# print(columns)
		list_columns: list[int | None] = list(columns)
		rid = self.next_rid.value(1)
		list_columns.insert(config.INDIRECTION_COLUMN, metadata.indirection_column)
		list_columns.insert(config.RID_COLUMN, rid)
		list_columns.insert(config.TIMESTAMP_COLUMN, int(time.time()))
		list_columns.insert(config.SCHEMA_ENCODING_COLUMN, metadata.schema_encoding)
		list_columns.insert(config.NULL_COLUMN, null_bitmask)
		cols = tuple(list_columns)
		for i in range(len(self.page_to_commit)):
			self.page_to_commit[i].insert(cols[i])
			self.offset.value(config.BYTES_PER_INT)
		if self.offset.value() == config.PHYSICAL_PAGE_SIZE:
			self.write_new_page(self.page_to_commit, path_type)	
		pg_dir_entry: 'PageDirectoryEntry'
		if path_type == "base":
			pg_dir_entry = PageDirectoryEntry(BasePageID(self.next_base_page_id.value()), MetadataPageID(self.next_metadata_page_id.value()), self.offset.value(), "base")
		elif path_type == "tail":
			pg_dir_entry = PageDirectoryEntry(TailPageID(self.next_base_page_id.value()), MetadataPageID(self.next_metadata_page_id.value()), self.offset.value(), "tail")
		self.table.page_directory_buff.value_assign(rid, pg_dir_entry)
		return rid
U = TypeVar('U')
V = TypeVar('V')
class PseudoBuffDictValue(Generic[U, V]):
	def __init__(self, file_handler: FileHandler, page_sub_path: Literal["page_directory", "indices"]):
		self.page_sub_path = page_sub_path
		self.page_path = file_handler.page_path(page_sub_path)
		self.file_handler = file_handler
		self._value = file_handler.read_dict_value(page_sub_path)
		# self._value = file_handler.read_value(page_sub_path, byte_position, "int")
	def flush(self) -> None:
		with open(self.page_path, "wb") as handle:
			pickle.dump(self._value, handle)
	def value_get(self) -> dict[U, V]:
		return self._value
	def __getitem__(self, key: U) -> V:
		return self._value[key]
	def value_assign(self, new_key: U, new_value: V) -> dict[U, V]:
		self._value[new_key] = new_value
		return self._value
	def __del__(self) -> None: # flush when this value is deleted
		self.flush()




            
            
            