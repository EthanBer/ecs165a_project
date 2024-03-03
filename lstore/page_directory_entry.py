from __future__ import annotations
from typing import Literal, TypeGuard, runtime_checkable
from abc import ABC

class BasePageID(int):
	pass

class TailPageID(int):
	pass
BaseTailPageID = BasePageID | TailPageID

class BaseMetadataPageID(int):
	pass

class TailMetadataPageID(int):
	pass
MetadataPageID = BaseMetadataPageID | TailMetadataPageID

# def isMetadataPageID(val: int) -> TypeGuard[MetadataPageID]:
# 	return isinstance(val, BaseMetadataPageID) or isinstance(val, TailMetadataPageID)

class BaseRID(int):
	pass

class TailRID(int):
	pass

PageID = BaseTailPageID | MetadataPageID

RID = BaseRID | TailRID | Literal[None]

class PageDirectoryEntry:
    def __init__(self, page_id: BaseTailPageID, metadata_page_id: MetadataPageID, offset: int, page_type: Literal["base", "tail"]):
        self.page_id = page_id
        self.metadata_page_id = metadata_page_id
        self.offset = offset
        self.page_type = page_type

    def __str__(self) -> str:
        return f"({self.page_id}, {self.offset})"

PageDirectory = dict[int, PageDirectoryEntry]