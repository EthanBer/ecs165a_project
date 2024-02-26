# this class is called "PseudoBuff" because it kind of works like the BufferedValue
# but without an actual Bufferpool (hence "pseudo")
from __future__ import annotations
import pickle
from typing import Generic, Literal, TypeVar

from lstore.file_handler import FileHandler
from lstore.page_directory_entry import BasePageID, PageID

