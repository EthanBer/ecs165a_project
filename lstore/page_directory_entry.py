from __future__ import annotations
from typing import Literal, runtime_checkable
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

class BaseRID(int):
	pass

class TailRID(int):
	pass

PageID = BaseTailPageID | MetadataPageID

RID = BaseRID | TailRID

class PageDirectoryEntry:
    def __init__(self, page_id: PageID, metadata_page_id: MetadataPageID, offset: int, page_type: Literal["base", "tail"]):
        self.page_id = page_id
        self.metadata_page_id = metadata_page_id
        self.offset = offset
        self.page_type = page_type

    def __str__(self) -> str:
        return f"({self.page_id}, {self.offset})"

PageDirectory = dict[int, PageDirectoryEntry]