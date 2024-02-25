from __future__ import annotations
# RawIndex = NewType('RawIndex', int) # Index taking into account the metadata
# DataIndex = NewType('DataIndex', int) # Index not taking into account the metadata
class RawIndex(int):
	def toDataIndex(self) -> DataIndex:
		from lstore.config import config
		return DataIndex(self - config.NUM_METADATA_COL)

class DataIndex(int):
	def toRawIndex(self) -> RawIndex:
		from lstore.config import config
		return RawIndex(self + config.NUM_METADATA_COL)





