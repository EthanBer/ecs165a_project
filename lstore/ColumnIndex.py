# RawIndex = NewType('RawIndex', int) # Index taking into account the metadata
# DataIndex = NewType('DataIndex', int) # Index not taking into account the metadata





class RawIndex(int):
	pass

class DataIndex(int):
	def toRawIndex(self) -> RawIndex:
		from lstore.config import config
		return RawIndex(self + config.NUM_METADATA_COL)





