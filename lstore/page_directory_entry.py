from __future__ import annotations
from typing import Literal

class PageID(int):
	pass

class BasePageID(PageID):
	pass

class TailPageID(PageID):
	pass

class MetadataPageID(PageID):
	pass

class BaseRID(int):
	pass

class TailRID(int):
	pass

class PageDirectoryEntry:
    def __init__(self, page_id: 'PageID', metadata_page_id: 'MetadataPageID', offset: int, page_type: Literal["base", "tail"]):
        self.page_id = page_id
        self.metadata_page_id = metadata_page_id
        self.offset = offset
        self.page_type = page_type

    def __str__(self) -> str:
        return f"({self.page_id}, {self.offset})"

PageDirectory = dict[int, PageDirectoryEntry]