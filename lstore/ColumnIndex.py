from typing import NewType


RawIndex = NewType('RawIndex', int) # Index taking into account the metadata
DataIndex = NewType('DataIndex', int) # Index not taking into account the metadata

def toRawIndex(idx: DataIndex) -> RawIndex:
	return RawIndex(idx + 4)



